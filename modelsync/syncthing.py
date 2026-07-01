"""Thin async client for one Syncthing instance's REST API.

Only the calls the reconciler needs. Config is driven entirely by us (we push
device + folder entries to both sides), so peers never need manual accept.
"""

from __future__ import annotations

from typing import Any
import httpx

# Force every node LAN-only: no global discovery, no relays, no NAT traversal,
# no usage reporting, no crash phone-home. Combined with static tcp:// device
# addresses (the GPUStack node IPs) this means zero internet traffic.
# Folder label stamped on every folder we manage, so GC can tell ours from the
# operator's own folders (or Syncthing's built-in 'default').
OWNED_LABEL = "modelsync"

LOCAL_ONLY_OPTIONS: dict[str, Any] = {
    "globalAnnounceEnabled": False,
    "localAnnounceEnabled": False,
    "relaysEnabled": False,
    "natEnabled": False,
    "urAccepted": -1,
    "crashReportingEnabled": False,
    "startBrowser": False,
}


class SyncthingClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        http: httpx.AsyncClient,
        cache_roots: tuple[str, ...] = (),
    ):
        self._base = base_url.rstrip("/")
        self._http = http
        self._headers = {"X-API-Key": api_key}
        self._roots = tuple(r.rstrip("/") for r in cache_roots if r)

    async def _req(self, method: str, path: str, **kw) -> httpx.Response:
        r = await self._http.request(
            method, f"{self._base}{path}", headers=self._headers, timeout=15, **kw
        )
        r.raise_for_status()
        return r

    async def my_id(self) -> str:
        r = await self._req("GET", "/rest/system/status")
        return r.json()["myID"]

    async def enforce_local_only(self) -> None:
        # PATCH merges into existing options.
        await self._req("PATCH", "/rest/config/options", json=LOCAL_ONLY_OPTIONS)

    async def put_device(self, device_id: str, name: str, address: str) -> None:
        await self._req(
            "PUT",
            f"/rest/config/devices/{device_id}",
            json={
                "deviceID": device_id,
                "name": name,
                "addresses": [address],  # static GPUStack node IP, no discovery
                "autoAcceptFolders": True,  # never prompt; we control the mesh
                "paused": False,
            },
        )

    async def put_folder(
        self,
        folder_id: str,
        path: str,
        peer_ids: list[str],
        folder_type: str = "sendreceive",
    ) -> None:
        await self._req(
            "PUT",
            f"/rest/config/folders/{folder_id}",
            json={
                "id": folder_id,
                "label": OWNED_LABEL,  # marks the folder as ours, for safe GC
                "path": path,
                "type": folder_type,
                "paused": False,  # self-heal a folder left paused by an aborted reset
                "fsWatcherEnabled": True,
                "rescanIntervalS": 3600,
                "devices": [{"deviceID": d} for d in peer_ids],
            },
        )

    async def revert(self, folder_id: str) -> None:
        """Discard a receive-only folder's local divergence, taking the source's
        version. No-op when local content already matches (block hashes agree),
        so it never re-transfers identical data."""
        await self._req("POST", "/rest/db/revert", params={"folder": folder_id})

    async def override(self, folder_id: str) -> None:
        """On a send-only source: force this node's version on the cluster,
        superseding conflicting versions. The recovery for a folder whose index
        got poisoned by prior conflicts."""
        await self._req("POST", "/rest/db/override", params={"folder": folder_id})

    async def set_paused(self, folder_id: str, paused: bool) -> None:
        await self._req(
            "PATCH", f"/rest/config/folders/{folder_id}", json={"paused": paused}
        )

    async def is_paused(self, folder_id: str) -> bool:
        """Config `paused` flag. This (not the db/status runtime state, which does
        NOT report a literal 'paused') is the check that reliably confirms a
        folder is paused before a reset."""
        r = await self._req("GET", f"/rest/config/folders/{folder_id}")
        return bool(r.json().get("paused"))

    async def reset_folder_db(self, folder_id: str) -> None:
        """Drop the folder's local index database (folder MUST be paused first),
        forcing a fresh rescan + index from peers. Clears a corrupted index that
        override/revert can't, e.g. accumulated phantom versions."""
        await self._req("POST", "/rest/system/reset", params={"folder": folder_id})

    async def delete_folder(self, folder_id: str) -> None:
        # Stops syncing this folder. Local files are left on disk untouched.
        r = await self._http.request(
            "DELETE",
            f"{self._base}/rest/config/folders/{folder_id}",
            headers=self._headers,
            timeout=15,
        )
        if r.status_code not in (200, 404):
            r.raise_for_status()

    async def owned_folders(self) -> set[str]:
        """Folder ids GC may remove: ours by label, OR any folder whose path is
        under a configured cache root (on our dedicated sidecar, those are always
        ours — catches folders left by an earlier folder-id/label scheme). Never
        Syncthing's built-in 'default' or a folder outside the cache roots."""
        r = await self._req("GET", "/rest/config/folders")
        return owned_ids(r.json(), self._roots)

    async def folder_status(self, folder_id: str) -> dict[str, Any]:
        """Rich per-folder state: completion %, sync state, bytes needed, error
        count. Surfaces problems a bare % hides (a stuck folder shows error)."""
        r = await self._req("GET", "/rest/db/status", params={"folder": folder_id})
        return parse_folder_status(r.json())


def _int(x) -> int:
    """Safe int for untrusted JSON: non-numeric (or bool) -> 0, never raises."""
    return int(x) if isinstance(x, (int, float)) and not isinstance(x, bool) else 0


def owned_ids(folders, roots: tuple[str, ...]) -> set[str]:
    """Folder ids GC may remove: ours by label, OR any folder whose path is under
    a cache root. Pure + total over malformed config (non-dict entries, missing
    id, non-str path all skipped) so GC can't crash on a bad folder list. Never
    the built-in 'default' or a folder outside the roots."""
    out: set[str] = set()
    if not isinstance(folders, list):
        return out
    for f in folders:
        if not isinstance(f, dict):
            continue
        fid = f.get("id")
        if not isinstance(fid, str) or fid == "default":
            continue
        fpath = f.get("path")
        path = fpath.rstrip("/") if isinstance(fpath, str) else ""
        under_root = any(path == r0 or path.startswith(r0 + "/") for r0 in roots)
        if f.get("label") == OWNED_LABEL or under_root:
            out.add(fid)
    return out


def parse_folder_status(d: Any) -> dict[str, Any]:
    """Coerce raw Syncthing db/status JSON into our status dict. Pure + total:
    any malformed field degrades to a safe default rather than raising."""
    if not isinstance(d, dict):
        d = {}
    g = _int(d.get("globalBytes"))
    need = _int(d.get("needBytes"))
    state = d.get("state") if isinstance(d.get("state"), str) else "unknown"
    # globalBytes==0 means we have NOT yet learned the folder's contents (no
    # index/scan): "unknown", NOT done. And only call it complete once the folder
    # is idle, so a mid-scan (g>0,need==0 transient) doesn't register an
    # incomplete model as present.
    complete = g > 0 and need == 0 and state == "idle"
    pct = 0.0 if g == 0 else max(0.0, (g - need) / g * 100.0)
    return {
        "completion": pct,
        "complete": complete,
        "state": state,
        "need_bytes": need,
        "global_bytes": g,
        # bytes actually present locally on disk (for integrity vs GPUStack's
        # expected model size — a poisoned global can't fake this).
        "local_bytes": _int(d.get("localBytes")),
        "errors": _int(d.get("errors")) + _int(d.get("pullErrors")),
        # receive-only divergence: local files not matching the source.
        "receive_only_changed": _int(d.get("receiveOnlyChangedBytes")),
    }
