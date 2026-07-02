"""Reconcile a desired plan (model path -> set of node ids) into Syncthing.

Distribution is master->replica: the authoritative node is the `sendonly`
source; the rest are `receiveonly` replicas, reverted to converge. The source is
chosen integrity-first (see choose_source): a Syncthing-verified CLEAN copy
(complete, idle, no errors, no divergence, local==global) beats GPUStack's
possibly-stale holder list; never an empty node. A replica is never reverted
toward a less-complete source, so a real copy is never wiped.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Callable

import httpx

from .gpustack import Worker
from .syncthing import SyncthingClient

ClientFor = Callable[[Worker], SyncthingClient]
_NET_ERRORS = (httpx.HTTPError, OSError)  # narrow: don't mask programming bugs


def folder_id(path: str) -> str:
    """Deterministic, collision-resistant Syncthing folder id from an absolute
    model path. The slug stays readable; an 8-char path hash prevents distinct
    paths (e.g. a/b vs a-b) from colliding to the same id."""
    p = path.strip("/")
    slug = re.sub(r"[^a-zA-Z0-9._-]", "-", p)[:80]
    # sha1 is fine here: non-cryptographic id disambiguation, not integrity.
    # Changing the algorithm would re-key every existing Syncthing folder.
    h = hashlib.sha1(p.encode()).hexdigest()[:8]  # noqa: S324
    return f"{slug}-{h}"


@dataclass
class SyncStatus:
    path: str
    worker_id: int
    worker_name: str
    completion: float
    complete: bool = False  # both: globalBytes>0 and needBytes==0
    state: str = "unknown"
    need_bytes: int = 0
    local_bytes: int = 0  # bytes actually on disk (Syncthing-hashed)
    global_bytes: int = 0
    receive_only_changed: int = 0
    errors: int = 0

    @property
    def clean(self) -> bool:
        """Syncthing verified a self-consistent full copy: complete + idle, no
        errors, no receive-only divergence, and local matches global (not a
        doubled/poisoned index). This is the reliable integrity signal — GPUStack's
        ModelFile.size counts only weights, so it can't be used as the reference."""
        return (
            self.complete
            and self.state == "idle"
            and self.errors == 0
            and self.receive_only_changed == 0
            and self.global_bytes > 0
            and self.local_bytes == self.global_bytes
        )


def _addr(w: Worker, port: int) -> str:
    # IPv6 literals must be bracketed in a host:port URL, else the port is
    # ambiguous ("fd00::1:22000") and Syncthing can't dial the peer.
    host = f"[{w.ip}]" if ":" in w.ip else w.ip
    return f"tcp://{host}:{port}"


async def reconcile(
    plan: dict[str, set[int]],
    workers: list[Worker],
    client_for: ClientFor,
    data_port: int = 22000,
    have: dict[str, set[int]] | None = None,
    max_send_kbps: int = 0,
    max_recv_kbps: int = 0,
    dev_out: dict[int, str] | None = None,
) -> tuple[list[int], list[str]]:
    """Wire Syncthing shares to match the plan. Returns (unreachable_ids,
    warnings). A node we can't reach is skipped, not fatal. dev_out, if given,
    collects worker_id -> Syncthing device id (for remote-completion fallback)."""
    have = have or {}
    by_id = {w.id: w for w in workers}
    warnings: list[str] = []

    # 1. device id + LAN-only + bandwidth caps per node; skip unreachable.
    dev_id: dict[int, str] = {}
    unreachable: set[int] = set()
    for w in workers:
        try:
            c = client_for(w)
            await c.enforce_local_only(max_send_kbps, max_recv_kbps)
            dev_id[w.id] = await c.my_id()
        except _NET_ERRORS:
            unreachable.add(w.id)
    if dev_out is not None:
        dev_out.update(dev_id)

    desired: dict[int, dict[str, str]] = {}  # wid -> {folder_id: path}

    for path, raw_targets in plan.items():
        # sorted -> deterministic source tie-break (no flapping across passes)
        targets = sorted(t for t in raw_targets if t in by_id and t not in unreachable)
        if not targets:
            continue
        fid = folder_id(path)

        # current per-target folder state (None = no folder yet / unreachable)
        status: dict[int, dict[str, Any] | None] = {}
        for wid in targets:
            try:
                status[wid] = await client_for(by_id[wid]).folder_status(fid)
            except _NET_ERRORS:
                status[wid] = None

        confirmed = set(have.get(path, set()))
        src = choose_source(targets, confirmed, status)
        if src is None:
            warnings.append(f"{path}: no node holds it (no source); skipped")
            continue
        # Only ever revert toward a CONFIRMED holder whose completion we actually
        # read. Unknown completion -> 0.0 so the guard can never wrongly fire.
        src_confirmed = src in confirmed
        src_compl = (status[src] or {}).get("completion", 0.0)

        for wid in targets:
            desired.setdefault(wid, {})[fid] = path
        for wid in targets:
            peers = [o for o in targets if o != wid]
            ftype = "sendonly" if wid == src else "receiveonly"
            try:
                c = client_for(by_id[wid])
                for o in peers:
                    await c.put_device(dev_id[o], by_id[o].name, _addr(by_id[o], data_port))
                await c.put_folder(fid, path, [dev_id[o] for o in peers], ftype)
                await c.set_ignores(fid)  # partial/temp files never replicate
                if wid == src and src_confirmed:
                    st = status.get(src)
                    # Source idle but "needing" bytes = replicas diverge from it
                    # (independent copies of the same model differ slightly). The
                    # sendonly source won't pull, so it stays non-complete forever.
                    # Override to make the source authoritative; replicas converge.
                    if st and st["state"] == "idle" and st["need_bytes"] > 0:
                        await c.override(fid)
                if ftype == "receiveonly" and src_confirmed:
                    st = status.get(wid) or await c.folder_status(fid)
                    # Revert a diverged replica ONLY if it is incomplete and not
                    # more complete than the confirmed source. A replica that
                    # already holds the full model (complete) is never reverted —
                    # that would destroy a good, independently-present copy.
                    if (
                        src_compl > 0  # never wipe toward an unknown/empty source
                        and st["global_bytes"] > 0  # replica state is known
                        and not st["complete"]
                        and st["receive_only_changed"] > 0
                        and st["completion"] < src_compl  # strictly less complete
                    ):
                        await c.revert(fid)
                    # Flag-clear revert: after an index reset, local files hashed
                    # before the remote index arrives are misattributed as local
                    # changes, leaving a COMPLETE, byte-identical replica marked
                    # diverged forever (complete -> never "stuck", so nothing else
                    # clears it). With local==global, need==0 and a 100% confirmed
                    # source, revert deletes nothing: identical content is just
                    # re-marked synced; any truly-diverged file is re-pulled from
                    # the clean source.
                    elif (
                        st["complete"]
                        and st["receive_only_changed"] > 0
                        and st["need_bytes"] == 0
                        and st["local_bytes"] == st["global_bytes"]
                        and src_compl >= 100
                    ):
                        await c.revert(fid)
            except _NET_ERRORS:
                unreachable.add(wid)

    # GC: drop folders WE own that aren't desired on this node (orphans / removed
    # targets). Never touches 'default' or the operator's own folders.
    for w in workers:
        if w.id in unreachable:
            continue
        keep = set(desired.get(w.id, {}))
        try:
            c = client_for(w)
            for fid in await c.owned_folders() - keep:
                await c.delete_folder(fid)
        except _NET_ERRORS:
            unreachable.add(w.id)

    return sorted(unreachable), warnings


def _is_clean(st: dict[str, Any] | None) -> bool:
    """A Syncthing-verified, self-consistent full copy (hash-checked, not a
    doubled/poisoned index). The reliable integrity signal."""
    return bool(
        st
        and st.get("complete")
        and st.get("state") == "idle"
        and not st.get("errors")
        and not st.get("receive_only_changed")
        and st.get("global_bytes", 0) > 0
        and st.get("local_bytes") == st.get("global_bytes")
    )


def choose_source(
    targets: list[int], have: set[int], status: dict[int, dict[str, Any] | None]
) -> int | None:
    """Authoritative node, integrity-first: a confirmed holder with a clean
    verified copy; else ANY node with a clean copy (a clean copy is trusted over
    GPUStack's possibly-stale `have`, e.g. a holder whose files were deleted);
    else a confirmed holder; else the most-complete node with data; else None."""
    confirmed = sorted(t for t in targets if t in have)
    clean_confirmed = [t for t in confirmed if _is_clean(status.get(t))]
    if clean_confirmed:
        return clean_confirmed[0]
    clean_any = sorted(t for t in targets if _is_clean(status.get(t)))
    if clean_any:
        return clean_any[0]
    if confirmed:
        return confirmed[0]
    best, best_c = None, 0.0
    for t in targets:
        c = (status.get(t) or {}).get("completion", 0.0)
        if c > best_c:
            best, best_c = t, c
    return best  # None if nobody has any data


async def collect_status(
    plan: dict[str, set[int]],
    workers: list[Worker],
    client_for: ClientFor,
    dev_ids: dict[int, str] | None = None,
) -> list[SyncStatus]:
    """Per (path, node) sync status. If a node's own GUI is unreachable, fall
    back to a reachable PEER's view of it (db/completion by device id) so the
    row shows real progress instead of a blind 'unreachable'."""
    dev_ids = dev_ids or {}
    by_id = {w.id: w for w in workers}
    out: list[SyncStatus] = []
    for path, targets in plan.items():
        fid = folder_id(path)
        tsorted = sorted(t for t in targets if t in by_id)
        for wid in tsorted:  # deterministic row order (stuck-pick depends on it)
            w = by_id[wid]
            try:
                s = await client_for(w).folder_status(fid)
                out.append(
                    SyncStatus(
                        path, wid, w.name, s["completion"], s["complete"],
                        s["state"], s["need_bytes"], s["local_bytes"],
                        s["global_bytes"], s["receive_only_changed"], s["errors"],
                    )
                )
            except _NET_ERRORS:
                pct = await _peer_view(fid, wid, tsorted, by_id, client_for, dev_ids)
                if pct is None:
                    out.append(SyncStatus(path, wid, w.name, 0.0, state="unreachable"))
                else:
                    out.append(SyncStatus(path, wid, w.name, pct, state="remote-view"))
    return out


async def _peer_view(
    fid: str,
    wid: int,
    targets: list[int],
    by_id: dict[int, Worker],
    client_for: ClientFor,
    dev_ids: dict[int, str],
) -> float | None:
    """A reachable peer's view of `wid`'s completion, else None."""
    dev = dev_ids.get(wid)
    if not dev:
        return None
    for other in targets:
        if other == wid:
            continue
        try:
            return await client_for(by_id[other]).remote_completion(fid, dev)
        except _NET_ERRORS:
            continue
    return None


