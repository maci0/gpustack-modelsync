"""Thin async client for one Syncthing instance's REST API.

Only the calls the reconciler needs. Config is driven entirely by us (we push
device + folder entries to both sides), so peers never need manual accept.
"""

from __future__ import annotations

import httpx

# Force every node LAN-only: no global discovery, no relays, no NAT traversal,
# no usage reporting, no crash phone-home. Combined with static tcp:// device
# addresses (the GPUStack node IPs) this means zero internet traffic.
# Folder label stamped on every folder we manage, so GC can tell ours from the
# operator's own folders (or Syncthing's built-in 'default').
OWNED_LABEL = "modelsync"

LOCAL_ONLY_OPTIONS: dict = {
    "globalAnnounceEnabled": False,
    "localAnnounceEnabled": False,
    "relaysEnabled": False,
    "natEnabled": False,
    "urAccepted": -1,
    "crashReportingEnabled": False,
    "startBrowser": False,
}


class SyncthingClient:
    def __init__(self, base_url: str, api_key: str, http: httpx.AsyncClient):
        self._base = base_url.rstrip("/")
        self._http = http
        self._headers = {"X-API-Key": api_key}

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
        """Folder ids WE created (label == OWNED_LABEL). GC only touches these,
        never 'default' or folders the operator added for other uses."""
        r = await self._req("GET", "/rest/config/folders")
        return {f["id"] for f in r.json() if f.get("label") == OWNED_LABEL}

    async def folder_status(self, folder_id: str) -> dict:
        """Rich per-folder state: completion %, sync state, bytes needed, error
        count. Surfaces problems a bare % hides (a stuck folder shows error)."""
        r = await self._req("GET", "/rest/db/status", params={"folder": folder_id})
        d = r.json()
        g = int(d.get("globalBytes") or 0)
        need = int(d.get("needBytes") or 0)
        state = d.get("state", "unknown")
        # globalBytes==0 means we have NOT yet learned the folder's contents (no
        # index/scan): "unknown", NOT done. And only call it complete once the
        # folder is idle, so a mid-scan (g>0,need==0 transient) doesn't register
        # an incomplete model as present.
        complete = g > 0 and need == 0 and state == "idle"
        pct = 0.0 if g == 0 else max(0.0, (g - need) / g * 100.0)
        return {
            "completion": pct,
            "complete": complete,
            "state": state,
            "need_bytes": need,
            "global_bytes": g,
            "errors": int(d.get("errors") or 0) + int(d.get("pullErrors") or 0),
            # receive-only divergence: local files not matching the source.
            "receive_only_changed": int(d.get("receiveOnlyChangedBytes") or 0),
        }
