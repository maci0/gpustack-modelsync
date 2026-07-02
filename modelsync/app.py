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
from .gpustack import GPUStackClient, ModelFolder, Worker, under_roots, free_for_path
from .reconcile import choose_source, collect_status, folder_id, reconcile
from .syncthing import SyncthingClient
from .web import PAGE, SCRIPT, USERSCRIPT

log = logging.getLogger("modelsync")

STATE = Path(settings.state_dir)
PLAN_FILE = STATE / "plan.json"
REGISTRY_FILE = STATE / "registry.json"  # (wid@path) -> gpustack model-file id WE made
MEMBERS_FILE = STATE / "members.json"
PINS_FILE = STATE / "pins.json"  # model_id -> {path, label, prev_selector}


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


def load_pins() -> dict[str, dict[str, Any]]:
    raw = _load_json(PINS_FILE, {})
    if not isinstance(raw, dict):
        return {}
    return {
        k: v for k, v in raw.items()
        if isinstance(k, str) and k.isdigit() and isinstance(v, dict)
        and isinstance(v.get("path"), str) and isinstance(v.get("label"), str)
    }


def save_pins(p: dict[str, dict[str, Any]]) -> None:
    _atomic_write(PINS_FILE, json.dumps(p))


class State:
    http: httpx.AsyncClient
    gpustack: GPUStackClient
    plan: dict[str, set[int]]
    registry: dict[str, int]
    members: set[int]
    pins: dict[str, dict[str, Any]]  # model_id -> {path, label, prev_selector}
    resetting: set[str]  # paths under an in-flight /reset; background loop skips them
    lock: asyncio.Lock
    loop_task: asyncio.Task[None]
    wake: asyncio.Event  # set by event watchers -> reconcile now, not next tick
    dev_ids: dict[int, str]  # worker id -> Syncthing device id (peer-view fallback)
    ev_tasks: dict[int, tuple[str, asyncio.Task[None]]]  # wid -> (ip, watcher task)
    watch_tasks: list[asyncio.Task[None]]
    net_prev: dict[int, tuple[float, int, int]]  # wid -> (t, in_total, out_total)
    stuck_seen: dict[tuple[str, int], int]  # (path, wid) -> consecutive stuck passes
    counters: dict[str, int]
    metrics: dict[str, float]


state = State()


# Optional per-node Syncthing API keys ("ip=key,..."); fall back to the shared key.
_ST_KEYS: dict[str, str] = dict(
    kv.split("=", 1) for kv in
    (p.strip() for p in settings.syncthing_api_keys.split(","))
    if kv and "=" in kv
)


def client_for(w: Worker) -> SyncthingClient:
    return SyncthingClient(
        f"http://{w.ip}:{settings.syncthing_gui_port}",
        _ST_KEYS.get(w.ip, settings.syncthing_api_key),
        state.http,
        tuple(r.strip() for r in settings.cache_roots.split(",") if r.strip()),
    )


_EXEMPT_NETS = [
    ipaddress.ip_network(c.strip())
    for c in settings.auth_exempt_cidrs.split(",")
    if c.strip()
]

# Cache roots gate: any path we stand up as a Syncthing share OR delete on disk
# must be strictly under one of these, so a compromised GPUStack can't point us
# at /etc or an arbitrary directory. Empty = allow (roots not configured).
_CACHE_ROOTS = [r.strip().rstrip("/") for r in settings.cache_roots.split(",") if r.strip()]


def _path_allowed(path: str) -> bool:
    return not _CACHE_ROOTS or under_roots(path, _CACHE_ROOTS)


def _peer_exempt(request: Request) -> bool:
    """True if the TCP peer's IP is in an auth-exempt CIDR. Uses the socket peer
    ONLY (request.client) — never X-Forwarded-For or any header a client could
    forge. A non-IP peer (test harness, unix socket) is never exempt."""
    host = request.client.host if request.client else ""
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    # A dual-stack socket reports loopback as ::ffff:127.0.0.1 — unwrap so it
    # matches the IPv4 exempt CIDRs instead of silently demanding a token.
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped
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
    state.pins = load_pins()
    state.resetting = set()
    state.lock = asyncio.Lock()
    state.wake = asyncio.Event()
    state.dev_ids = {}
    state.ev_tasks = {}
    state.net_prev = {}
    state.stuck_seen = {}
    state.counters = {"registered": 0, "deregistered": 0, "stuck_resolved": 0, "reconciles": 0}
    state.metrics = {}
    if not settings.auth_token:
        log.warning("MODELSYNC_AUTH_TOKEN unset — orchestrator API is UNAUTHENTICATED")
    # Cleartext-credential warnings: the GPUStack bearer token and the Syncthing
    # API key travel in plaintext over http on a routable network.
    gp = urlparse(settings.gpustack_url)
    if gp.scheme == "http" and (gp.hostname or "") not in ("localhost", "127.0.0.1", "::1"):
        log.warning("GPUSTACK_URL is http:// to a remote host — bearer token sent in cleartext; use https")
    if not settings.syncthing_api_keys:
        log.warning("single shared SYNCTHING_API_KEY on all nodes — set SYNCTHING_API_KEYS "
                    "(ip=key,...) so one compromised node can't reach every peer")
    state.loop_task = asyncio.create_task(_background_loop())
    # GPUStack SSE watches: worker/model-file create+delete wake the reconcile
    # loop immediately (the interval remains as fallback heartbeat).
    state.watch_tasks = [
        asyncio.create_task(_gpustack_watch_loop("workers")),
        asyncio.create_task(_gpustack_watch_loop("model-files")),
    ]
    yield
    for _ip, t in state.ev_tasks.values():
        t.cancel()
    for t in state.watch_tasks:
        t.cancel()
    state.loop_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await state.loop_task  # let it finish so we don't close http under it
    for _ip, t in list(state.ev_tasks.values()):
        with contextlib.suppress(asyncio.CancelledError):
            await t
    for t in state.watch_tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
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
        plan, managed, client_for, settings.syncthing_data_port, have,
        settings.sync_max_send_kbps, settings.sync_max_recv_kbps,
        dev_out=state.dev_ids,
    )
    _ensure_event_watchers(workers, {w.id for w in managed})

    rows = await collect_status(plan, workers, client_for, state.dev_ids)
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
    stuck_rows = [
        r for r in rows
        if not r.complete and r.need_bytes > 0
        and r.state not in working and r.worker_id in by_id
        and r.path not in state.resetting  # don't fight an in-flight /reset
    ]
    # Forget counters for rows that started moving / completed (only currently
    # stuck rows carry a count). Incrementing is deferred to below and only
    # happens when a clean source actually existed to attempt a gentle resolve —
    # otherwise a run of source-less passes would escalate straight to heavy-reset
    # the moment a source appears, skipping the gentle revert.
    stuck_keys = {(r.path, r.worker_id) for r in stuck_rows}
    for key in list(state.stuck_seen):
        if key not in stuck_keys:
            del state.stuck_seen[key]
    stuck = stuck_rows[0] if stuck_rows else None
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
            state.counters["stuck_resolved"] += 1
            # count this pass now that a real resolve is happening (drives escalation)
            for r in stuck_rows:
                if r.path == stuck.path:
                    key = (r.path, r.worker_id)
                    state.stuck_seen[key] = state.stuck_seen.get(key, 0) + 1
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
                    # Escalate: override+revert provably can't clear the
                    # remove->re-add stall (folder idle at 0% with a connected
                    # peer). After 3 stuck passes, drop the replica's index db so
                    # it rebuilds from the clean source. Index-only: files are
                    # never touched, and there's a verified clean copy to pull from.
                    key = (r.path, r.worker_id)
                    if state.stuck_seen.get(key, 0) >= 3:
                        log.warning("stuck %s on %s after %d passes; heavy-resetting its index",
                                    r.path, by_id[r.worker_id].name, state.stuck_seen[key])
                        await _heavy_reset(client_for(by_id[r.worker_id]), fid)
                        state.stuck_seen[key] = 0

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

    await _sync_pins(workers, have)

    state.counters["registered"] += len(registered)
    state.counters["deregistered"] += len(deregistered)
    state.counters["reconciles"] += 1
    state.metrics = {
        "rows": len(rows),
        "rows_complete": sum(1 for r in rows if r.complete),
        "rows_clean": sum(1 for r in rows if r.clean),
        "rows_errors": sum(1 for r in rows if r.errors > 0),
        "rows_unreachable": sum(1 for r in rows if r.state == "unreachable"),
        "need_bytes": sum(r.need_bytes for r in rows),
        "plan_models": len(plan),
        "unreachable_nodes": len(unreachable),
    }

    return {
        "unreachable": unreachable,
        "warnings": warnings,
        "registered": registered,
        "deregistered": deregistered,
    }


async def _sync_pins(workers: list[Worker], have: dict[str, set[int]]) -> None:
    """Keep pin labels in step with reality: every READY holder of a pinned
    model's folder carries the pin label; nodes that lost the copy lose it.
    GPUStack's scheduler then places instances via Model.worker_selector."""
    if not state.pins:
        return
    # Drop pins whose folder no longer exists anywhere (model fully removed): the
    # pin can never label a holder again, so stop tracking + writing for it.
    dead = [mid for mid, pin in state.pins.items() if pin["path"] not in have]
    if dead:
        for mid in dead:
            del state.pins[mid]
        save_pins(state.pins)
    by_id = {w.id: w for w in workers}
    for pin in state.pins.values():
        path, label = pin["path"], pin["label"]
        holders = have.get(path, set())
        for w in by_id.values():
            has = label in w.labels
            want = w.id in holders
            if has == want:
                continue
            labels = {k: v for k, v in w.labels.items() if k != label}
            if want:
                labels[label] = "true"
            try:
                await state.gpustack.set_worker_labels(w.id, labels)
                w.labels = labels  # keep the in-memory view consistent this pass
            except httpx.HTTPError as e:
                log.warning("pin label update for worker %s failed: %s", w.id, e)


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
            # Wait for either an event wake (Syncthing completion, GPUStack
            # create/delete) or the fallback interval — whichever comes first.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(state.wake.wait(), timeout=max(1, settings.reconcile_interval))
            state.wake.clear()
            await asyncio.sleep(1)  # debounce an event burst into one pass
            await reconcile_all()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # never let the loop die
            log.warning("reconcile loop error: %s", e)


async def _gpustack_watch_loop(resource: str) -> None:
    """Wake the reconcile loop on GPUStack create/delete events. Update events
    (status heartbeats) are ignored — they'd wake every few seconds for nothing.
    Backoff doubles up to 5 min so an unsupported/broken stream doesn't spam."""
    backoff = 5
    while True:
        try:
            async for line in state.gpustack.watch(resource):
                backoff = 5  # stream is alive; reset
                up = line.upper()
                if b"CREATE" in up or b"DELETE" in up:
                    state.wake.set()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(backoff)  # stream died / unsupported; reconnect
            backoff = min(300, backoff * 2)


async def _syncthing_event_loop(w: Worker) -> None:
    """Long-poll one node's Syncthing events. A folder reaching 100% completion
    (or reporting errors) wakes the reconcile loop, so registration happens
    seconds after a sync finishes instead of at the next interval tick."""
    c = client_for(w)
    since = 0
    while True:
        try:
            evs = await c.events(since, "FolderCompletion,FolderErrors")
            for ev in evs:
                eid = ev.get("id")
                if isinstance(eid, int):
                    since = max(since, eid)
                data = ev.get("data") or {}
                etype = ev.get("type")
                if etype == "FolderErrors" or (etype == "FolderCompletion" and data.get("completion") == 100):
                    state.wake.set()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(10)


def _ensure_event_watchers(workers: list[Worker], managed_ids: set[int]) -> None:
    """One event-watcher task per managed reachable worker; recreated when the
    node's IP changes, cancelled when the node leaves the managed set."""
    want = {w.id: w for w in workers if w.id in managed_ids and w.syncable}
    for wid, (ip, task) in list(state.ev_tasks.items()):
        if wid not in want or want[wid].ip != ip:
            task.cancel()
            del state.ev_tasks[wid]
    for wid, w in want.items():
        if wid not in state.ev_tasks:
            state.ev_tasks[wid] = (w.ip, asyncio.create_task(_syncthing_event_loop(w)))


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


@app.get("/net", dependencies=[Depends(require_auth)])
async def net() -> dict[int, dict[str, Any]]:
    """Per managed node: connected peer count + real in/out rates, computed from
    Syncthing's connection byte totals between polls (not a client-side guess)."""
    workers = await state.gpustack.workers()
    managed = [w for w in workers if w.id in state.members and w.syncable]
    out: dict[int, dict[str, Any]] = {}
    now = asyncio.get_running_loop().time()
    for w in managed:
        try:
            conns = (await client_for(w).connections()).get("connections") or {}
        except httpx.HTTPError:
            continue
        connected = in_tot = out_tot = 0
        for c in conns.values():
            if not isinstance(c, dict):
                continue
            if c.get("connected"):
                connected += 1
            in_tot += int(c.get("inBytesTotal") or 0)
            out_tot += int(c.get("outBytesTotal") or 0)
        prev = state.net_prev.get(w.id)
        in_bps = out_bps = 0.0
        if prev and now > prev[0]:
            dt = now - prev[0]
            in_bps = max(0.0, (in_tot - prev[1]) / dt)
            out_bps = max(0.0, (out_tot - prev[2]) / dt)
        state.net_prev[w.id] = (now, in_tot, out_tot)
        out[w.id] = {"connected": connected, "in_bps": in_bps, "out_bps": out_bps}
    # prune counters for workers that no longer exist (bound growth under churn)
    live = {w.id for w in workers}
    for wid in [k for k in state.net_prev if k not in live]:
        del state.net_prev[wid]
    return out


@app.get("/metrics", dependencies=[Depends(require_auth)])
async def metrics() -> Response:
    """Prometheus text exposition of the last reconcile snapshot + counters."""
    lines = []
    for k, v in state.metrics.items():
        lines.append(f"# TYPE modelsync_{k} gauge")
        lines.append(f"modelsync_{k} {v}")
    for k, v in state.counters.items():
        lines.append(f"# TYPE modelsync_{k}_total counter")
        lines.append(f"modelsync_{k}_total {v}")
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


class PinIn(BaseModel):
    path: str
    model_id: int


@app.post("/pin", dependencies=[Depends(require_auth)])
async def pin(body: PinIn) -> dict[str, Any]:
    """Schedule-aware placement: label every node holding this folder and point
    the Model's worker_selector at that label, so GPUStack schedules instances
    onto nodes that already have the weights. Reconcile keeps labels current."""
    if not _path_allowed(body.path):
        return {"ok": False, "error": "path outside configured cache roots"}
    label = f"modelsync-{folder_id(body.path)[-8:]}"
    async with state.lock:
        folders = await state.gpustack.model_folders()
        have = {f.path: set(f.current_nodes) for f in folders}
        if body.path not in have:
            return {"ok": False, "error": "unknown model path"}
        m = await state.gpustack.get_model(body.model_id)
        if m is None:
            return {"ok": False, "error": "model not found"}
        try:
            await state.gpustack.set_model_selector(body.model_id, {label: "true"})
        except httpx.HTTPError as e:
            return {"ok": False, "error": f"selector update failed: {e}"}
        state.pins[str(body.model_id)] = {
            "path": body.path,
            "label": label,
            "prev_selector": m.get("worker_selector") or {},
        }
        save_pins(state.pins)
        workers = await state.gpustack.workers()
        await _sync_pins(workers, have)
    return {"ok": True, "label": label, "holders": sorted(have[body.path])}


class UnpinIn(BaseModel):
    model_id: int


@app.post("/unpin", dependencies=[Depends(require_auth)])
async def unpin(body: UnpinIn) -> dict[str, Any]:
    """Restore the Model's original worker_selector; pin labels stop updating."""
    async with state.lock:
        pin_rec = state.pins.pop(str(body.model_id), None)
        if pin_rec is None:
            return {"ok": False, "error": "model not pinned"}
        save_pins(state.pins)
        try:
            await state.gpustack.set_model_selector(
                body.model_id, pin_rec.get("prev_selector") or {}
            )
        except httpx.HTTPError as e:
            return {"ok": False, "error": f"selector restore failed: {e} (pin removed)"}
        # Strip the now-orphaned pin label off every worker that carries it.
        label = pin_rec.get("label")
        for w in await state.gpustack.workers():
            if label in w.labels:
                with contextlib.suppress(httpx.HTTPError):
                    await state.gpustack.set_worker_labels(
                        w.id, {k: v for k, v in w.labels.items() if k != label}
                    )
    return {"ok": True}


class PurgeIn(BaseModel):
    path: str
    worker_id: int
    delete_files: bool = False


@app.post("/purge", dependencies=[Depends(require_auth)])
async def purge(body: PurgeIn) -> dict[str, Any]:
    """Reclaim a node's copy: unshare (drop from plan), deregister, and with
    delete_files=True have GPUStack delete the bytes on disk. Refuses while a
    model instance is running from that copy."""
    if not _path_allowed(body.path):
        return {"ok": False, "error": "path outside configured cache roots"}
    actions: list[str] = []
    async with state.lock:
        # Do the DESTRUCTIVE steps first and mutate the plan only after they
        # succeed. If we abort (instance running) the plan is untouched, so we
        # never leave "removed from plan but files/record still there" (which the
        # background loop would then unshare, orphaning the GPUStack record).
        for mi in await state.gpustack.model_instances():
            if mi.local_dir == body.path and mi.worker_id == body.worker_id:
                return {"ok": False, "error": "a model instance is running from this copy"}
        # Delete the model-file BEFORE unshare-reconcile: reconcile would
        # deregister a copy WE created (record gone, bytes kept), leaving a
        # cleanup delete nothing to act on — files stranded forever.
        mid = await state.gpustack.find_model_file(body.path, body.worker_id)
        if mid is None:
            actions.append("no GPUStack model-file record on that node")
        else:
            try:
                await state.gpustack.delete_model_file(mid, cleanup=body.delete_files)
                actions.append("deleted record + files" if body.delete_files else "deleted record (files kept)")
            except httpx.HTTPError as e:
                return {"ok": False, "error": f"delete failed: {e}", "actions": actions}
            k = _key(body.path, body.worker_id)
            if state.registry.pop(k, None) is not None:  # it was ours; already deleted above
                save_registry(state.registry)
        # Destructive part done -> now drop from plan + unshare the folder.
        if body.worker_id in state.plan.get(body.path, set()):
            state.plan[body.path] = state.plan[body.path] - {body.worker_id}
            if not state.plan[body.path]:
                del state.plan[body.path]
            save_plan(state.plan)
            actions.append("removed from plan")
        await _reconcile_core(state.plan)  # unshare the Syncthing folder (GC)
    return {"ok": True, "actions": actions}


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
    if bool(settings.ssl_certfile) != bool(settings.ssl_keyfile):
        raise SystemExit("Set BOTH MODELSYNC_SSL_CERTFILE and MODELSYNC_SSL_KEYFILE (or neither).")
    if settings.ssl_certfile:
        uvicorn.run(
            app, host=settings.listen_host, port=settings.listen_port,
            ssl_certfile=settings.ssl_certfile, ssl_keyfile=settings.ssl_keyfile,
        )
    else:
        uvicorn.run(app, host=settings.listen_host, port=settings.listen_port)


if __name__ == "__main__":
    main()
