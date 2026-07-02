from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel

from .config import settings
from .gpustack import GPUStackClient, ModelFolder, Worker, free_for_path
from .reconcile import choose_source, collect_status, folder_id, reconcile
from .syncthing import SyncthingClient
from .web import PAGE, SCRIPT, USERSCRIPT

log = logging.getLogger("modelsync")

STATE = Path(settings.state_dir)
PLAN_FILE = STATE / "plan.json"
REGISTRY_FILE = STATE / "registry.json"  # (wid@path) -> gpustack model-file id WE made
MEMBERS_FILE = STATE / "members.json"


def _key(path: str, wid: int) -> str:
    return f"{wid}@{path}"


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)  # rename is atomic; no torn file on crash


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError, ValueError):
        log.warning("corrupt state file %s; backing up and starting empty", path)
        with contextlib.suppress(OSError):
            path.replace(path.with_suffix(path.suffix + ".corrupt"))
        return default


def _int_ids(v) -> set[int]:
    """Coerce a JSON value to a set of ints, dropping anything else. Guards the
    state-file trust boundary: a hand-edited/corrupt file must not crash startup
    (e.g. set(5) TypeError) or inject str ids that silently never match."""
    return {x for x in v if isinstance(x, int) and not isinstance(x, bool)} if isinstance(v, list) else set()


def load_plan() -> dict[str, set[int]]:
    raw = _load_json(PLAN_FILE, {})
    if not isinstance(raw, dict):
        return {}
    return {k: ids for k, v in raw.items() if isinstance(k, str) and (ids := _int_ids(v))}


def save_plan(plan: dict[str, set[int]]) -> None:
    _atomic_write(PLAN_FILE, json.dumps({k: sorted(v) for k, v in plan.items()}))


def load_registry() -> dict[str, int]:
    raw = _load_json(REGISTRY_FILE, {})
    if not isinstance(raw, dict):
        return {}
    out = {}
    for k, v in raw.items():
        wid_s = k.split("@", 1)[0] if isinstance(k, str) else ""
        if wid_s.isdigit() and isinstance(v, int) and not isinstance(v, bool):  # positive ids only
            out[k] = v  # drop malformed keys instead of crashing later on int()
        else:
            log.warning("dropping malformed registry key %r", k)
    return out


def save_registry(reg: dict[str, int]) -> None:
    _atomic_write(REGISTRY_FILE, json.dumps(reg))


def load_members() -> set[int]:
    return _int_ids(_load_json(MEMBERS_FILE, []))


def save_members(m: set[int]) -> None:
    _atomic_write(MEMBERS_FILE, json.dumps(sorted(m)))


class State:
    http: httpx.AsyncClient
    gpustack: GPUStackClient
    plan: dict[str, set[int]]
    registry: dict[str, int]
    members: set[int]
    resetting: set[str]  # paths under an in-flight /reset; background loop skips them
    lock: asyncio.Lock
    loop_task: asyncio.Task[None]


state = State()


def client_for(w: Worker) -> SyncthingClient:
    return SyncthingClient(
        f"http://{w.ip}:{settings.syncthing_gui_port}",
        settings.syncthing_api_key,
        state.http,
        tuple(r.strip() for r in settings.cache_roots.split(",") if r.strip()),
    )


_EXEMPT_NETS = [
    ipaddress.ip_network(c.strip())
    for c in settings.auth_exempt_cidrs.split(",")
    if c.strip()
]


def _peer_exempt(request: Request) -> bool:
    """True if the TCP peer's IP is in an auth-exempt CIDR. Uses the socket peer
    ONLY (request.client) — never X-Forwarded-For or any header a client could
    forge. A non-IP peer (test harness, unix socket) is never exempt."""
    host = request.client.host if request.client else ""
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(addr in n for n in _EXEMPT_NETS)


def _check_token(tok: str) -> None:
    if not settings.auth_token:
        return  # open API (only allowed on loopback; enforced in main())
    # bytes compare: constant-time AND won't TypeError on a non-ASCII token
    if not secrets.compare_digest(tok.encode("utf-8"), settings.auth_token.encode("utf-8")):
        raise HTTPException(status_code=401, detail="unauthorized")


async def require_auth(
    request: Request,
    authorization: str | None = Header(None),
    x_auth_token: str | None = Header(None),
) -> None:
    """Header-only auth for normal routes (never a query param, which would leak
    the token into access/proxy logs). Trusted-local peers skip the token."""
    if _peer_exempt(request):
        return
    _check_token((authorization or "").removeprefix("Bearer ").strip() or (x_auth_token or ""))


async def require_auth_query(
    request: Request,
    authorization: str | None = Header(None),
    x_auth_token: str | None = Header(None),
    token: str | None = Query(None),
) -> None:
    """Only for /events: EventSource can't set headers, so a query token is
    accepted here (and ONLY here)."""
    if _peer_exempt(request):
        return
    _check_token(
        (authorization or "").removeprefix("Bearer ").strip()
        or (x_auth_token or "")
        or (token or "")
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    state.http = httpx.AsyncClient()
    state.gpustack = GPUStackClient(
        settings.gpustack_url,
        settings.gpustack_token,
        state.http,
        settings.gpustack_api_prefix,
        [c for c in settings.allowed_worker_cidrs.split(",") if c.strip()],
        [r for r in settings.cache_roots.split(",") if r.strip()],
    )
    # Create the state dir up front so the first apply can't crash on a missing
    # STATE_DIR (misconfigured/unmounted volume) — fail clearly at startup instead.
    STATE.mkdir(parents=True, exist_ok=True)
    state.plan = load_plan()
    state.registry = load_registry()
    state.members = load_members()
    state.resetting = set()
    state.lock = asyncio.Lock()
    if not settings.auth_token:
        log.warning("MODELSYNC_AUTH_TOKEN unset — orchestrator API is UNAUTHENTICATED")
    state.loop_task = asyncio.create_task(_background_loop())
    yield
    state.loop_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await state.loop_task  # let it finish so we don't close http under it
    await state.http.aclose()


app = FastAPI(title="gpustack-modelsync", lifespan=lifespan)


# ---- core automation (assumes state.lock held) ----------------------------


async def _reconcile_core(
    plan: dict[str, set[int]],
    workers: list[Worker] | None = None,
    folders: list[ModelFolder] | None = None,
) -> dict[str, Any]:
    if workers is None:
        workers = await state.gpustack.workers()
    if folders is None:
        folders = await state.gpustack.model_folders()
    have = {f.path: set(f.current_nodes) for f in folders}
    spec = {f.path: f.spec for f in folders}

    involved = {wid for t in plan.values() for wid in t}
    involved |= {int(k.split("@", 1)[0]) for k in state.registry}
    involved |= state.members
    managed = [w for w in workers if w.id in involved and w.syncable]
    unreachable, warnings = await reconcile(
        plan, managed, client_for, settings.syncthing_data_port, have
    )

    rows = await collect_status(plan, workers, client_for)
    done = {(r.path, r.worker_id) for r in rows if r.complete}  # both APIs agree

    # Resolve at most ONE stuck path per pass. A folder settled (idle/error) yet
    # still "needing" bytes is stuck (poisoned index after remove -> re-add), not
    # mid-sync (need>0 also skips a fresh, not-yet-connected folder). Resolve it
    # from the AUTHORITATIVE copy: the node whose folder is complete (full, clean,
    # and GPUStack-known). Override that copy so its version wins cluster-wide,
    # then drop the poisoned index on the stuck node(s) so they re-pull it clean.
    # Deterministic and convergent: once fixed the node is complete, not stuck, so
    # it won't re-trigger (no loop). No complete copy anywhere -> surface, no act.
    by_id = {w.id: w for w in workers}
    working = ("scanning", "syncing", "sync-preparing", "cleaning")
    stuck = next(
        (r for r in rows
         if not r.complete and r.need_bytes > 0
         and r.state not in working and r.worker_id in by_id
         and r.path not in state.resetting),  # don't fight an in-flight /reset
        None,
    )
    if stuck:
        fid = folder_id(stuck.path)
        # Authoritative = a Syncthing-CLEAN copy (self-consistent, hash-verified,
        # not a doubled/poisoned index), preferring a GPUStack-confirmed holder,
        # and among those the smallest global (the un-poisoned true size). This
        # doesn't trust GPUStack's weights-only size, nor a poisoned "complete".
        cands = [r for r in rows if r.path == stuck.path and r.clean and r.worker_id in by_id]
        holders = [r for r in cands if r.worker_id in have.get(stuck.path, set())]
        pool = holders or cands
        auth = min(pool, key=lambda r: r.global_bytes).worker_id if pool else None
        if auth is None:
            log.warning("stuck %s: no clean verified copy to resolve from; manual check", stuck.path)
        else:
            log.info("resolving stuck %s from clean copy %s", stuck.path, by_id[auth].name)
            with contextlib.suppress(httpx.HTTPError):
                # override: the clean copy's version becomes the cluster truth.
                await client_for(by_id[auth]).override(fid)
            # revert each stuck replica so it drops its divergent/poisoned local
            # and takes the clean version (pulls what's missing, deletes extras).
            # Safe: source is integrity-verified. Self-limiting: a reverted folder
            # goes to syncing, so it isn't re-detected as stuck (no reset loop).
            for r in rows:
                if r.path == stuck.path and r.worker_id != auth and not r.complete and r.worker_id in by_id:
                    with contextlib.suppress(httpx.HTTPError):
                        await client_for(by_id[r.worker_id]).revert(fid)

    registered, deregistered = [], []
    if settings.register_in_gpustack:
        for path, targets in plan.items():
            for wid in sorted(targets):  # deterministic order
                k = _key(path, wid)
                if (path, wid) not in done or wid in have.get(path, set()):
                    continue
                if k in state.registry or not spec.get(path):
                    continue
                try:
                    mid = await state.gpustack.register_synced(wid, spec[path])
                    if mid is not None:
                        state.registry[k] = mid
                        save_registry(state.registry)  # persist per-op (crash-safe)
                        registered.append(k)
                    else:
                        log.warning("register %s returned no id (possible orphan)", k)
                except httpx.HTTPError as e:
                    log.warning("register %s failed: %s", k, e)

    # Deregister ALWAYS (even if register_in_gpustack was turned off), so copies
    # we created get cleaned up. Verify the id still maps to OUR worker before
    # deleting — GPUStack may reuse a model-file id after an out-of-band delete.
    for k, mid in list(state.registry.items()):
        wid_s, path = k.split("@", 1)
        if int(wid_s) in plan.get(path, set()):
            continue
        try:
            mf = await state.gpustack.get_model_file(mid)
            if mf is None:
                del state.registry[k]  # already gone; forget it
                save_registry(state.registry)
                continue
            if mf.get("worker_id") != int(wid_s):
                log.warning("registry id %s no longer maps to worker %s; forgetting", mid, wid_s)
                del state.registry[k]
                save_registry(state.registry)
                continue
            await state.gpustack.delete_model_file(mid)
            del state.registry[k]
            save_registry(state.registry)
            deregistered.append(k)
        except httpx.HTTPError as e:
            log.warning("deregister %s (id %s) failed: %s", k, mid, e)

    # prune members no longer referenced by the plan or registry (avoid forever
    # poking decommissioned nodes)
    keep_members = {wid for t in plan.values() for wid in t}
    keep_members |= {int(k.split("@", 1)[0]) for k in state.registry}
    if keep_members != state.members:
        state.members = keep_members
        save_members(state.members)

    return {
        "unreachable": unreachable,
        "warnings": warnings,
        "registered": registered,
        "deregistered": deregistered,
    }


async def reconcile_all(plan: dict[str, set[int]] | None = None) -> dict[str, Any]:
    async with state.lock:
        if plan is not None:
            return await _reconcile_core(plan)
        # background path: prune plan entries whose model no longer exists in
        # GPUStack (deleted there) so its sync stops within one interval. Errors
        # propagate (loop retries), so a transient API blip never prunes.
        folders = await state.gpustack.model_folders()
        kept = _prune_plan(state.plan, {f.path for f in folders})
        if kept != state.plan:
            log.info("pruning %d plan entries deleted from GPUStack", len(state.plan) - len(kept))
            state.plan = kept
            save_plan(state.plan)
        return await _reconcile_core(state.plan, folders=folders)


def _prune_plan(plan: dict[str, set[int]], known_paths: set[str]) -> dict[str, set[int]]:
    """Drop plan entries for models GPUStack no longer reports. Safety: an EMPTY
    model list against a non-empty plan is treated as a suspicious false-empty
    response (auth glitch / filter bug) and left untouched — never deregister
    everything on a bad 200. Genuine full-delete is rare; clear the plan by hand."""
    if not known_paths and plan:
        return plan
    return {p: t for p, t in plan.items() if p in known_paths}


async def _background_loop() -> None:
    while True:
        try:
            await asyncio.sleep(max(1, settings.reconcile_interval))  # floor: never spin
            await reconcile_all()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # never let the loop die
            log.warning("reconcile loop error: %s", e)


# ---- API ------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    # CSP blocks inline-handler execution -> defangs any HTML injection via model
    # names; our JS uses addEventListener, not inline onclick.
    return HTMLResponse(
        PAGE,
        headers={
            "Content-Security-Policy": (
                "default-src 'self'; style-src 'unsafe-inline'; img-src 'self' data:; "
                "frame-ancestors 'none'"
            ),
            "X-Frame-Options": "DENY",
        },
    )


@app.get("/app.js")
async def app_js() -> Response:
    return Response(SCRIPT, media_type="application/javascript")


@app.get("/userscript.js")
async def userscript(request: Request) -> Response:
    """Tampermonkey userscript that embeds the matrix into the GPUStack dashboard.
    Open (it carries no secret); placeholders filled from the request + config so
    @match/@connect/API base are correct for wherever the orchestrator is reached."""
    api = str(request.base_url).rstrip("/")
    gp = urlparse(settings.gpustack_url)
    js = (
        USERSCRIPT.replace("__API__", api)
        .replace("__GP_ORIGIN__", f"{gp.scheme}://{gp.netloc}")
        .replace("__ORCH_HOST__", urlparse(api).hostname or "*")
    )
    return Response(js, media_type="application/javascript")


@app.get("/nodes", dependencies=[Depends(require_auth)])
async def nodes() -> list[Worker]:
    return await state.gpustack.workers()


@app.get("/clusters", dependencies=[Depends(require_auth)])
async def clusters() -> list[dict[str, Any]]:
    return await state.gpustack.clusters()


@app.get("/models", dependencies=[Depends(require_auth)])
async def models() -> list[dict[str, Any]]:
    folders = await state.gpustack.model_folders()
    instances = await state.gpustack.model_instances()
    plan = dict(state.plan)  # snapshot for a consistent read vs a concurrent apply
    serving: dict[str, set[int]] = {}
    for mi in instances:
        if mi.local_dir and mi.worker_id is not None:
            serving.setdefault(mi.local_dir, set()).add(mi.worker_id)
    return [
        {
            "path": f.path,
            "label": f.label,
            "size": f.size,
            "nodes": sorted(plan.get(f.path, set())),  # checkbox = plan
            "have": f.current_nodes,
            "serving": sorted(serving.get(f.path, set())),
        }
        for f in folders
    ]


class PlanIn(BaseModel):
    plan: dict[str, list[int]]


def _eligible(
    requested: dict[str, set[int]],
    workers: list[Worker],
    folders: list[ModelFolder],
) -> tuple[dict[str, set[int]], list[str]]:
    by_id = {w.id: w for w in workers}
    known = {f.path for f in folders}
    size = {f.path: f.size for f in folders}
    have = {f.path: set(f.current_nodes) for f in folders}
    out: dict[str, set[int]] = {}
    warn: list[str] = []
    for path, targets in requested.items():
        if path not in known:  # only sync paths GPUStack actually reports
            warn.append(f"{path}: unknown model path, ignored")
            continue
        keep: set[int] = set()
        for wid in targets:
            w = by_id.get(wid)
            if w is None:  # selected a node that no longer exists — never silent
                warn.append(f"node {wid}: unknown or removed, skipped {path}")
                continue
            if wid in have.get(path, set()):
                keep.add(wid)
                continue
            if not w.syncable:
                warn.append(f"{w.name}: not ready/in maintenance, skipped {path}")
                continue
            need = size.get(path, 0)
            free = free_for_path(w, path)  # free on the mount that holds it
            if free is not None and need and free < need:
                warn.append(
                    f"{w.name}: {free // 2**30}GiB free < {need // 2**30}GiB model, skipped {path}"
                )
                continue
            keep.add(wid)
        if keep:
            out[path] = keep
            if not (keep & have.get(path, set())):
                warn.append(f"{path}: no selected node holds it yet — nothing to sync from")
    return out, warn


@app.post("/plan", dependencies=[Depends(require_auth)])
async def set_plan(body: PlanIn) -> dict[str, Any]:
    requested = {k: set(v) for k, v in body.plan.items() if v}
    async with state.lock:
        workers = await state.gpustack.workers()
        folders = await state.gpustack.model_folders()
        plan, warnings = _eligible(requested, workers, folders)

        prev = state.plan
        name = {w.id: w.name for w in workers}
        added, removed = [], []
        for p in set(prev) | set(plan):
            old, new = prev.get(p, set()), plan.get(p, set())
            lbl = p.rstrip("/").split("/")[-1]
            added += [f"{lbl} → {name.get(w, w)}" for w in new - old]
            removed += [f"{lbl} ✗ {name.get(w, w)}" for w in old - new]

        state.plan = plan
        save_plan(plan)
        # members are recomputed authoritatively inside _reconcile_core (from
        # plan | registry), so no need to pre-add here.
        result = await _reconcile_core(plan, workers, folders)

    warnings += result["warnings"]
    warnings += [f"{name.get(i, i)}: Syncthing unreachable" for i in result["unreachable"]]
    return {"ok": True, "models": len(plan), "added": added, "removed": removed, "warnings": warnings}


class ResetIn(BaseModel):
    path: str


@app.post("/reset", dependencies=[Depends(require_auth)])
async def reset(body: ResetIn) -> dict[str, Any]:
    """Operator recovery for a stuck/conflicted folder. Setup runs under the lock;
    the slow recovery (pause/reset can take many seconds) runs OUTSIDE it, with a
    per-path guard so the background loop won't touch the same folder meanwhile —
    avoids both a race and freezing all orchestration behind one reset."""
    path = body.path
    if path not in state.plan:
        return {"ok": False, "error": "path not in current plan"}
    async with state.lock:
        if path in state.resetting:  # per-path mutex: no two concurrent resets
            return {"ok": False, "error": "reset already in progress for this path"}
        state.resetting.add(path)  # claim; try/finally below always releases it
    try:
        async with state.lock:  # setup under the lock; recovery below runs outside
            await _reconcile_core(state.plan)  # ensure folders wired
            workers = await state.gpustack.workers()
            by_id = {w.id: w for w in workers}
            folders = await state.gpustack.model_folders()
            have = {f.path: set(f.current_nodes) for f in folders}
            targets = sorted(t for t in state.plan.get(path, set()) if (w := by_id.get(t)) and w.syncable)
            fid = folder_id(path)
            # Integrity-first source (same as reconcile): a CLEAN verified copy,
            # not just the lowest GPUStack holder — never override a poisoned node.
            status: dict[int, dict[str, Any] | None] = {}
            for t in targets:
                try:
                    status[t] = await client_for(by_id[t]).folder_status(fid)
                except httpx.HTTPError:
                    status[t] = None
            src = choose_source(targets, have.get(path, set()), status)
        if src is None:
            return {"ok": False, "error": "no reachable source node holds this model"}

        replicas = [t for t in targets if t != src]
        actions: list[str] = []
        # Source completion gates override; only override a complete source, else
        # it could propagate an empty/stale copy and wipe good replicas.
        src_compl = (status[src] or {}).get("completion", 0.0)
        if src_compl >= 100.0:
            try:
                await client_for(by_id[src]).override(fid)
                actions.append(f"{by_id[src].name}: override (source)")
            except httpx.HTTPError:
                actions.append(f"{by_id[src].name}: override failed")
        else:
            actions.append(f"{by_id[src].name}: source incomplete ({src_compl:.0f}%), override skipped")

        for wid in replicas:
            c = client_for(by_id[wid])
            try:
                st = await c.folder_status(fid)
                # deliberate recovery: heavy-reset ANY incomplete replica (drop its
                # index db, rescan clean from the overridden source) — fixes deep
                # poison a revert can't. A complete replica is left alone.
                if not st["complete"]:
                    actions.append(f"{by_id[wid].name}: {await _heavy_reset(c, fid)}")
                else:
                    actions.append(f"{by_id[wid].name}: ok ({st['state']})")
            except httpx.HTTPError:
                actions.append(f"{by_id[wid].name}: unreachable")
    finally:
        state.resetting.discard(path)
    return {"ok": True, "source": src, "actions": actions}


async def _heavy_reset(c: SyncthingClient, fid: str) -> str:
    """Pause -> confirm paused -> drop index DB -> always unpause. Self-contained:
    never propagates (so one replica's failure doesn't abort the others) and
    never leaves the folder paused."""
    try:
        await c.set_paused(fid, True)
    except httpx.HTTPError:
        return "pause failed"
    paused = False
    try:
        for _ in range(15):
            try:
                if await c.is_paused(fid):  # config flag; runtime state never says "paused"
                    paused = True
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
        if not paused:
            return "could not pause; reset skipped"
        try:
            await c.reset_folder_db(fid)
            return "index reset (replica)"
        except httpx.HTTPError:
            return "index reset failed"
    finally:
        for _ in range(5):  # never leave a folder stuck paused
            try:
                await c.set_paused(fid, False)
                break
            except httpx.HTTPError:
                await asyncio.sleep(1)


@app.get("/status", dependencies=[Depends(require_auth)])
async def status() -> list[dict[str, Any]]:
    workers = await state.gpustack.workers()
    folders = await state.gpustack.model_folders()
    exp = {f.path: f.size for f in folders}
    rows = await collect_status(dict(state.plan), workers, client_for)  # snapshot
    # clean = Syncthing-verified self-consistent copy; expected_bytes = GPUStack's
    # weights-only size (shown for reference, not used to judge integrity).
    return [{**r.__dict__, "expected_bytes": exp.get(r.path, 0), "clean": r.clean} for r in rows]


@app.get("/suggest", dependencies=[Depends(require_auth)])
async def suggest() -> list[dict[str, Any]]:
    instances = await state.gpustack.model_instances()
    plan = dict(state.plan)  # snapshot
    out = []
    for mi in instances:
        if mi.local_dir and mi.worker_id is not None and mi.worker_id not in plan.get(mi.local_dir, set()):
            out.append({"path": mi.local_dir, "worker_id": mi.worker_id})
    return out


@app.get("/events", dependencies=[Depends(require_auth_query)])
async def events() -> StreamingResponse:
    async def gen():
        try:
            async for _ in state.gpustack.watch_workers():
                yield b"data: reload\n\n"
        except asyncio.CancelledError:
            raise
        except Exception:
            yield b"data: reload\n\n"  # nudge the browser to reconnect

    return StreamingResponse(gen(), media_type="text/event-stream")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    # Fail closed: an unauthenticated API may only bind loopback. Refuse to start
    # open on a routable interface (the mutating-cluster-control footgun).
    loopback = {"127.0.0.1", "::1", "localhost"}
    if not settings.auth_token and settings.listen_host not in loopback:
        raise SystemExit(
            "Refusing to start: MODELSYNC_AUTH_TOKEN is unset and LISTEN_HOST "
            f"({settings.listen_host}) is not loopback. Set a token or bind 127.0.0.1."
        )
    uvicorn.run(app, host=settings.listen_host, port=settings.listen_port)


if __name__ == "__main__":
    main()
