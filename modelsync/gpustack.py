"""Async client for the GPUStack server API: workers, model files, model
instances, the SSE watch stream, and registration writes."""

from __future__ import annotations

import ipaddress
import logging
import os
from typing import Any, AsyncIterator

import httpx
from pydantic import BaseModel

log = logging.getLogger("modelsync.gpustack")


class Worker(BaseModel):
    id: int
    name: str
    ip: str
    cluster_id: int | None = None
    state: str | None = None
    unreachable: bool = False
    maintenance: bool = False
    free_bytes: int | None = None  # most free space on any mount; None = unknown
    mounts: list[dict[str, Any]] = []  # [{mount_point, free}] for per-path capacity checks
    labels: dict[str, str] = {}
    worker_version: str | None = None
    gpu_name: str | None = None
    vram_total: int = 0
    vram_used: int = 0
    gpu_util: float | None = None  # avg utilization across devices, %

    @property
    def syncable(self) -> bool:
        return (
            not self.unreachable
            and not self.maintenance
            and (self.state or "").lower() == "ready"
        )


class ModelFolder(BaseModel):
    """A syncable model directory. `path` = absolute ModelFile.local_dir/path,
    identical across nodes sharing the cache layout. `current_nodes` = workers
    GPUStack reports holding it. `spec` = source-defining fields to re-register
    the same model on a new node after syncing."""

    path: str
    label: str
    size: int
    current_nodes: list[int]
    spec: dict[str, str] = {}


class ModelInstance(BaseModel):
    model_id: int | None = None
    worker_id: int | None = None
    state: str | None = None
    local_dir: str | None = None


_SPEC_KEYS = (
    "source",
    "huggingface_repo_id",
    "huggingface_filename",
    "model_scope_model_id",
    "model_scope_file_path",
    "local_path",
)


class GPUStackClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        http: httpx.AsyncClient,
        api_prefix: str = "/v2",
        allowed_cidrs: list[str] | None = None,
        cache_roots: list[str] | None = None,
    ):
        self._base = base_url.rstrip("/")
        # Normalize to a leading-slash, no-trailing-slash prefix so a value like
        # "v2" (no slash) can't produce "http://hostv2/workers".
        p = api_prefix.strip("/")
        self._v = f"/{p}" if p else ""
        self._http = http
        self._headers = {"Authorization": f"Bearer {token}"}
        self._nets = [ipaddress.ip_network(c.strip()) for c in (allowed_cidrs or [])]
        self._roots = [r.strip().rstrip("/") for r in (cache_roots or []) if r.strip()]

    async def _get(self, path: str, **params: Any) -> Any:  # JSON: shape is untrusted
        r = await self._http.get(
            f"{self._base}{path}", headers=self._headers, params=params, timeout=15
        )
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _items(data: Any) -> list[Any]:
        # PaginatedList -> {"items":[...]}. Branch on type FIRST (a bare list
        # has no .get); never call .get on a list.
        if isinstance(data, dict):
            v = data.get("items")
            return v if isinstance(v, list) else []  # {"items": null} -> [], not None
        return data if isinstance(data, list) else []

    async def _list(self, path: str, per_page: int = 100, max_pages: int = 1000) -> list[Any]:
        """Fetch every page of a paginated GPUStack list. Uses the response's
        pagination metadata (so a server-side perPage cap doesn't truncate), with
        a hard page cap as a backstop against a server that ignores paging.
        Errors propagate — a fetch failure must NOT look like an empty result."""
        out: list[Any] = []
        for page in range(1, max_pages + 1):
            data = await self._get(path, page=page, perPage=per_page)
            items = self._items(data)
            out.extend(items)
            # Coerce pagination metadata safely: a non-numeric field must not
            # crash the fetch, it just falls back to the short-page heuristic.
            total_pages = None
            if isinstance(data, dict):
                pg_raw = data.get("pagination")
                pg = pg_raw if isinstance(pg_raw, dict) else {}
                tp = _int_or_none(pg.get("totalPage")) or _int_or_none(pg.get("total_pages"))
                total = _int_or_none(pg.get("total"))
                if tp is not None:
                    total_pages = tp
                elif total is not None:
                    pp = _int_or_none(pg.get("perPage")) or per_page or 1
                    total_pages = -(-total // pp)  # ceil div
            if total_pages is not None:
                if page >= total_pages:
                    break
            elif len(items) < per_page:  # no metadata: stop on a short page
                break
        return out

    def _ip_ok(self, ip: str) -> bool:
        if not self._nets:
            return True
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        return any(addr in n for n in self._nets)

    async def workers(self) -> list[Worker]:
        by_id: dict[int, Worker] = {}  # dedup: pagination churn can repeat a worker
        for w in await self._list(f"{self._v}/workers"):
            wid, ip = w.get("id"), w.get("ip")
            if wid is None or not ip:
                continue  # provisioning / not-yet-reported; skip until it has an IP
            if not self._ip_ok(ip):
                log.warning("worker %s ip %s outside allowed CIDRs, skipping", wid, ip)
                continue
            gpu = _gpu_summary(w.get("status"))
            by_id[wid] = (
                Worker(
                    id=wid,
                    name=w.get("name") or w.get("hostname") or str(wid),
                    ip=ip,
                    cluster_id=w.get("cluster_id"),
                    state=w.get("state"),
                    unreachable=bool(w.get("unreachable")),
                    maintenance=_maintenance_on(w.get("maintenance")),
                    free_bytes=_max_free(w.get("status")),
                    mounts=_mounts(w.get("status")),
                    labels=w.get("labels") or {},
                    worker_version=w.get("worker_version"),
                    **gpu,
                )
            )
        return list(by_id.values())

    async def model_folders(self) -> list[ModelFolder]:
        nodes: dict[str, set[int]] = {}
        # size per (path, worker): sum shards WITHIN a worker, then take the max
        # ACROSS workers — the model size is one full copy, not the total of every
        # node's copy (summing across nodes would report N× the real size).
        size_bw: dict[str, dict[int, int]] = {}
        spec: dict[str, dict[str, str]] = {}
        for mf in await self._list(f"{self._v}/model-files"):
            path = _model_dir(mf)
            if not path:
                continue
            if self._roots and not _under_roots(path, self._roots):
                log.warning("model path %s outside cache roots, ignoring", path)
                continue
            nodes.setdefault(path, set())
            wid = mf.get("worker_id")
            bucket = size_bw.setdefault(path, {})
            key = wid if wid is not None else -1
            bucket[key] = bucket.get(key, 0) + _int(mf.get("size"))  # safe: malformed size -> 0
            spec.setdefault(path, {k: mf[k] for k in _SPEC_KEYS if mf.get(k)})
            if wid is not None:
                nodes[path].add(wid)
        return [
            ModelFolder(
                path=p,
                label=os.path.basename(p) or p,
                size=max(size_bw.get(p, {}).values(), default=0),
                current_nodes=sorted(w),
                spec=spec.get(p, {}),
            )
            for p, w in sorted(nodes.items())
        ]

    async def clusters(self) -> list[dict[str, Any]]:
        """id -> display name for the cluster selector. Best-effort: purely
        cosmetic (UI falls back to 'cluster <id>'), so any error yields []."""
        try:
            items = await self._list(f"{self._v}/clusters")
        except httpx.HTTPError:
            return []
        out = []
        for c in items:
            cid = c.get("id") if isinstance(c, dict) else None
            if cid is None:
                continue
            name = c.get("name")
            out.append({"id": cid, "name": name if isinstance(name, str) and name else f"cluster {cid}"})
        return out

    async def model_instances(self) -> list[ModelInstance]:
        # Best-effort: instances only drive the decorative "serving" badge and
        # /suggest, never a destructive action, so a transient error just yields
        # empty rather than 500-ing /models and /status. (model_folders, which
        # DOES drive reconcile/GC, stays strict and propagates errors.)
        try:
            items = await self._list(f"{self._v}/model-instances")
        except httpx.HTTPError:
            log.warning("model-instances fetch failed; serving info omitted")
            return []
        out = []
        for mi in items:
            out.append(
                ModelInstance(
                    model_id=mi.get("model_id"),
                    worker_id=mi.get("worker_id"),
                    state=mi.get("state"),
                    local_dir=_instance_dir(mi),
                )
            )
        return out

    async def watch_workers(self) -> AsyncIterator[bytes]:
        async with self._http.stream(
            "GET",
            f"{self._base}{self._v}/workers",
            headers=self._headers,
            params={"watch": "true"},
            timeout=None,
        ) as r:
            r.raise_for_status()
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    yield line.encode()

    async def register_synced(self, worker_id: int, spec: dict[str, str]) -> int | None:
        """Register a synced model onto a worker by cloning the source model's
        spec (huggingface repo etc.), so GPUStack sees the SAME model on the new
        node. Returns the new model-file id."""
        r = await self._http.post(
            f"{self._base}{self._v}/model-files",
            headers=self._headers,
            json={**spec, "worker_id": worker_id},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("id")

    async def get_model_file(self, model_file_id: int) -> dict[str, Any] | None:
        try:
            return await self._get(f"{self._v}/model-files/{model_file_id}")
        except httpx.HTTPError:
            return None

    async def delete_model_file(self, model_file_id: int) -> None:
        r = await self._http.request(
            "DELETE",
            f"{self._base}{self._v}/model-files/{model_file_id}",
            headers=self._headers,
            timeout=15,
        )
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()


# Safe coercion for untrusted GPUStack JSON: a wrong-typed field (string where a
# number is expected, number where a path is expected) must degrade, not crash
# the whole worker fetch. bool excluded (it is an int subclass).
def _int(x) -> int:
    return int(x) if isinstance(x, (int, float)) and not isinstance(x, bool) else 0


def _int_or_none(x) -> int | None:
    return int(x) if isinstance(x, (int, float)) and not isinstance(x, bool) else None


def _float_or_none(x) -> float | None:
    return float(x) if isinstance(x, (int, float)) and not isinstance(x, bool) else None


def _first_str(*vals) -> str | None:
    for v in vals:
        if isinstance(v, str) and v:
            return v
    return None


def _model_dir(mf: dict[str, Any]) -> str | None:
    p = _first_str(mf.get("local_dir"), mf.get("local_path"))
    if not p:
        rps = mf.get("resolved_paths")
        p = next((x for x in rps if isinstance(x, str) and x), None) if isinstance(rps, list) else None
    return _as_dir(p) if p else None


def _instance_dir(mi: dict[str, Any]) -> str | None:
    rp = _first_str(mi.get("resolved_path"), mi.get("local_path"))
    return _as_dir(rp) if rp else None


_MODEL_FILE_EXTS = {".gguf", ".safetensors", ".bin", ".pt", ".pth", ".onnx", ".npz"}


def _as_dir(p: str) -> str:
    """The model directory, normalized. If the path is a weight file, use its
    parent; else it already is the directory. normpath collapses any `..` so a
    traversal can't slip past the cache-root allowlist. Model *names* contain
    dots, so match a known extension set rather than a bare splitext check."""
    p = os.path.normpath(p)
    ext = os.path.splitext(os.path.basename(p))[1].lower()
    return os.path.dirname(p) if ext in _MODEL_FILE_EXTS else p


def _maintenance_on(m) -> bool:
    return bool(m.get("enabled")) if isinstance(m, dict) else bool(m)


def _gpu_summary(status: Any) -> dict[str, Any]:
    name: str | None = None
    vram_total = vram_used = 0
    utils: list[float] = []
    devs = status.get("gpu_devices") if isinstance(status, dict) else None
    for gd in devs if isinstance(devs, list) else []:
        if not isinstance(gd, dict):
            continue
        if name is None and isinstance(gd.get("name"), str):
            name = gd["name"]
        mem = gd.get("memory")
        mem = mem if isinstance(mem, dict) else {}
        vram_total += _int(mem.get("total"))
        vram_used += _int(mem.get("used"))
        core = gd.get("core")
        core = core if isinstance(core, dict) else {}
        u = _float_or_none(core.get("utilization_rate"))
        if u is not None:
            utils.append(u)
    return {
        "gpu_name": name,
        "vram_total": vram_total,
        "vram_used": vram_used,
        "gpu_util": round(sum(utils) / len(utils), 1) if utils else None,
    }


def _under_roots(path: str, roots: list[str]) -> bool:
    # strictly UNDER a root (depth >= 1), never equal to a root — sharing a whole
    # cache root as one folder would let a revert/override wipe sibling models.
    p = os.path.normpath(path)
    return any(p.startswith(r + "/") for r in roots)


def _mounts(status: Any) -> list[dict[str, Any]]:
    if not isinstance(status, dict) or not isinstance(status.get("filesystem"), list):
        return []
    out = []
    for m in status["filesystem"]:
        if not isinstance(m, dict):
            continue
        free = _int_or_none(m.get("free") if m.get("free") is not None else m.get("available"))
        mp = _first_str(m.get("mount_point"), m.get("mountPoint"))
        if mp and free is not None:
            out.append({"mount_point": mp, "free": free})
    return out


def free_for_path(worker, path: str) -> int | None:
    """Free bytes on the worker's mount that holds `path` (longest-prefix match),
    falling back to the most-free mount, else None. Used for capacity checks so a
    model isn't placed where its own filesystem can't hold it."""
    best_mp, best_free = "", None
    for m in worker.mounts:
        mp = m["mount_point"].rstrip("/") or "/"
        if (path == mp or path.startswith(mp.rstrip("/") + "/")) and len(mp) > len(best_mp):
            best_mp, best_free = mp, m["free"]
    if best_free is not None:
        return best_free
    return worker.free_bytes


def _max_free(status) -> int | None:
    if not isinstance(status, dict):
        return None
    fs = status.get("filesystem")
    if not isinstance(fs, list):
        return None
    frees: list[int] = []
    for m in fs:
        if isinstance(m, dict):
            f = _int_or_none(m.get("free") if m.get("free") is not None else m.get("available"))
            if f is not None:
                frees.append(f)
    return max(frees) if frees else None
