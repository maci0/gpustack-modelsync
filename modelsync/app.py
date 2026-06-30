from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel

from .config import settings
from .gpustack import GPUStackClient, Worker, free_for_path
from .reconcile import collect_status, folder_id, pick_source, reconcile
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
        try:
            path.replace(path.with_suffix(path.suffix + ".corrupt"))
        except OSError:
            pass
        return default


def load_plan() -> dict[str, set[int]]:
    return {k: set(v) for k, v in _load_json(PLAN_FILE, {}).items()}


def save_plan(plan: dict[str, set[int]]) -> None:
    _atomic_write(PLAN_FILE, json.dumps({k: sorted(v) for k, v in plan.items()}))


def load_registry() -> dict[str, int]:
    raw = _load_json(REGISTRY_FILE, {})
    out = {}
    for k, v in raw.items():
        wid_s = k.split("@", 1)[0]
        if wid_s.lstrip("-").isdigit() and isinstance(v, int):
            out[k] = v  # drop malformed keys instead of crashing later on int()
        else:
            log.warning("dropping malformed registry key %r", k)
    return out


def save_registry(reg: dict[str, int]) -> None:
    _atomic_write(REGISTRY_FILE, json.dumps(reg))


def load_members() -> set[int]:
    return set(_load_json(MEMBERS_FILE, []))


def save_members(m: set[int]) -> None:
    _atomic_write(MEMBERS_FILE, json.dumps(sorted(m)))


class State:
    http: httpx.AsyncClient
    gpustack: GPUStackClient
    plan: dict[str, set[int]]
    registry: dict[str, int]
    members: set[int]
    lock: asyncio.Lock
    loop_task: asyncio.Task


state = State()


def client_for(w: Worker) -> SyncthingClient:
    return SyncthingClient(
        f"http://{w.ip}:{settings.syncthing_gui_port}",
        settings.syncthing_api_key,
        state.http,
    )


def _check_token(tok: str) -> None:
    if not settings.auth_token:
        return  # open API (only allowed on loopback; enforced in main())
    # bytes compare: constant-time AND won't TypeError on a non-ASCII token
    if not secrets.compare_digest(tok.encode("utf-8"), settings.auth_token.encode("utf-8")):
        raise HTTPException(status_code=401, detail="unauthorized")


async def require_auth(
    authorization: str | None = Header(None),
    x_auth_token: str | None = Header(None),
) -> None:
    """Header-only auth for normal routes (never a query param, which would leak
    the token into access/proxy logs)."""
    _check_token((authorization or "").removeprefix("Bearer ").strip() or (x_auth_token or ""))


async def require_auth_query(
    authorization: str | None = Header(None),
    x_auth_token: str | None = Header(None),
    token: str | None = Query(None),
) -> None:
    """Only for /events: EventSource can't set headers, so a query token is
    accepted here (and ONLY here)."""
    _check_token(
        (authorization or "").removeprefix("Bearer ").strip()
        or (x_auth_token or "")
        or (token or "")
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.http = httpx.AsyncClient()
    state.gpustack = GPUStackClient(
        settings.gpustack_url,
        settings.gpustack_token,
        state.http,
        settings.gpustack_api_prefix,
        [c for c in settings.allowed_worker_cidrs.split(",") if c.strip()],
        [r for r in settings.cache_roots.split(",") if r.strip()],
    )
    state.plan = load_plan()
    state.registry = load_registry()
    state.members = load_members()
    state.lock = asyncio.Lock()
    if not settings.auth_token:
        log.warning("MODELSYNC_AUTH_TOKEN unset — orchestrator API is UNAUTHENTICATED")
    state.loop_task = asyncio.create_task(_background_loop())
    yield
    state.loop_task.cancel()
    try:
        await state.loop_task  # let it finish so we don't close http under it
    except asyncio.CancelledError:
        pass
    await state.http.aclose()


app = FastAPI(title="gpustack-modelsync", lifespan=lifespan)


# ---- core automation (assumes state.lock held) ----------------------------


async def _reconcile_core(plan: dict[str, set[int]], workers=None, folders=None) -> dict:
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

    registered, deregistered = [], []
    if settings.register_in_gpustack:
        for path, targets in plan.items():
            for wid in targets:
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


async def reconcile_all(plan: dict[str, set[int]] | None = None) -> dict:
    async with state.lock:
        return await _reconcile_core(state.plan if plan is None else plan)


async def _background_loop() -> None:
    while True:
        try:
            await asyncio.sleep(settings.reconcile_interval)
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
                "default-src 'self'; style-src 'unsafe-inline'; frame-ancestors 'none'"
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


@app.get("/models", dependencies=[Depends(require_auth)])
async def models() -> list[dict]:
    folders = await state.gpustack.model_folders()
    instances = await state.gpustack.model_instances()
    serving: dict[str, set[int]] = {}
    for mi in instances:
        if mi.local_dir and mi.worker_id is not None:
            serving.setdefault(mi.local_dir, set()).add(mi.worker_id)
    return [
        {
            "path": f.path,
            "label": f.label,
            "size": f.size,
            "nodes": sorted(state.plan.get(f.path, set())),  # checkbox = plan
            "have": f.current_nodes,
            "serving": sorted(serving.get(f.path, set())),
        }
        for f in folders
    ]


class PlanIn(BaseModel):
    plan: dict[str, list[int]]


def _eligible(requested, workers, folders) -> tuple[dict[str, set[int]], list[str]]:
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
            if w is None:
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
async def set_plan(body: PlanIn) -> dict:
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
        state.members |= {wid for t in plan.values() for wid in t}
        save_members(state.members)
        result = await _reconcile_core(plan, workers, folders)

    warnings += result["warnings"]
    warnings += [f"{name.get(i, i)}: Syncthing unreachable" for i in result["unreachable"]]
    return {"ok": True, "models": len(plan), "added": added, "removed": removed, "warnings": warnings}


class ResetIn(BaseModel):
    path: str


@app.post("/reset", dependencies=[Depends(require_auth)])
async def reset(body: ResetIn) -> dict:
    """Operator recovery for a stuck/conflicted folder. Held under the lock so it
    can't interleave with the background reconcile on the same folder."""
    path = body.path
    async with state.lock:
        await _reconcile_core(state.plan)  # ensure folders wired
        workers = await state.gpustack.workers()
        by_id = {w.id: w for w in workers}
        folders = await state.gpustack.model_folders()
        have = {f.path: set(f.current_nodes) for f in folders}
        targets = {t for t in state.plan.get(path, set()) if (w := by_id.get(t)) and w.syncable}
        src = pick_source(targets, have.get(path, set()))  # confirmed holder only
        if src is None:
            return {"ok": False, "error": "no reachable source node holds this model"}
        fid = folder_id(path)
        replicas = [t for t in sorted(targets) if t != src]
        actions: list[str] = []

        # Source completion gates BOTH override and revert. Unknown -> 0.0.
        try:
            src_compl = (await client_for(by_id[src]).folder_status(fid))["completion"]
        except httpx.HTTPError:
            src_compl = 0.0
        # override forces the source's content cluster-wide, bypassing the revert
        # guards — only do it when the source is actually complete, else it could
        # propagate an empty/stale source and wipe good replicas.
        override_ok = False
        if src_compl >= 100.0:
            try:
                await client_for(by_id[src]).override(fid)
                override_ok = True
                actions.append(f"{by_id[src].name}: override (source)")
            except httpx.HTTPError:
                actions.append(f"{by_id[src].name}: override failed")
        else:
            actions.append(f"{by_id[src].name}: source incomplete ({src_compl:.0f}%), override skipped")

        for wid in replicas:
            c = client_for(by_id[wid])
            try:
                st = await c.folder_status(fid)
                if st["errors"] > 0 or st["state"] == "error":
                    actions.append(f"{by_id[wid].name}: {await _heavy_reset(c, fid)}")
                elif (override_ok and st["global_bytes"] > 0 and not st["complete"]
                      and st["receive_only_changed"] > 0 and st["completion"] < src_compl):
                    await c.revert(fid)  # safe: known, incomplete, strictly worse than source
                    actions.append(f"{by_id[wid].name}: revert (replica)")
                else:
                    actions.append(f"{by_id[wid].name}: ok ({st['state']})")
            except httpx.HTTPError:
                actions.append(f"{by_id[wid].name}: unreachable")
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
                # runtime state (db/status), not the config flag we just set —
                # the runner stops asynchronously after the PATCH.
                if (await c.folder_status(fid))["state"] == "paused":
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
async def status() -> list[dict]:
    workers = await state.gpustack.workers()
    rows = await collect_status(state.plan, workers, client_for)
    return [r.__dict__ for r in rows]


@app.get("/suggest", dependencies=[Depends(require_auth)])
async def suggest() -> list[dict]:
    instances = await state.gpustack.model_instances()
    out = []
    for mi in instances:
        if mi.local_dir and mi.worker_id is not None and mi.worker_id not in state.plan.get(mi.local_dir, set()):
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
