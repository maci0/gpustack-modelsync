"""Run the orchestrator against a FAKE GPUStack + FAKE Syncthing serving sample
data, so you can click around the UI with no real cluster. Nothing leaves
loopback and no real hosts are touched.

    uv run python scripts/demo.py
    # then open http://127.0.0.1:18585

Ports: orchestrator 18585, fake GPUStack 19900, fake Syncthing 18384 (override
any with the matching env var if they clash). This is a dev aid, not shipped in
the container image.
"""

import asyncio
import hashlib
import json
import os
from pathlib import Path

STATE = Path(__file__).resolve().parent.parent / ".demo-state"
STATE.mkdir(exist_ok=True)

ORCH_PORT = int(os.environ.get("DEMO_ORCH_PORT", "18585"))
GP_PORT = int(os.environ.get("DEMO_GPUSTACK_PORT", "19900"))
ST_PORT = int(os.environ.get("DEMO_SYNCTHING_PORT", "18384"))

os.environ.update(
    MODELSYNC_GPUSTACK_URL=f"http://127.0.0.1:{GP_PORT}",
    MODELSYNC_GPUSTACK_TOKEN="demo",
    MODELSYNC_SYNCTHING_API_KEY="demo",
    MODELSYNC_SYNCTHING_GUI_PORT=str(ST_PORT),
    MODELSYNC_ALLOWED_WORKER_CIDRS="127.0.0.0/8",
    MODELSYNC_CACHE_ROOTS="/var/lib/gpustack",
    MODELSYNC_AUTH_TOKEN="",  # loopback -> tokenless
    MODELSYNC_LISTEN_HOST="127.0.0.1",
    MODELSYNC_LISTEN_PORT=str(ORCH_PORT),
    MODELSYNC_STATE_DIR=str(STATE),
    MODELSYNC_RECONCILE_INTERVAL="3",
)

import uvicorn  # noqa: E402
from fastapi import FastAPI, Request  # noqa: E402

GB = 1024**3
CACHE = "/var/lib/gpustack/cache/huggingface"


def _gpu(name, tot, used, util):
    return {"gpu_devices": [{"name": name, "memory": {"total": tot, "used": used},
                             "core": {"utilization_rate": util}}],
            "filesystem": [{"mount_point": "/var/lib/gpustack", "free": 3 * 1024 * GB}]}


WORKERS = [
    {"id": 1, "ip": "127.0.0.1", "name": "gpu-node-01", "state": "ready", "cluster_id": 1,
     "labels": {}, "worker_version": "v2.2.0", "status": _gpu("NVIDIA H100 80GB HBM3", 80 * GB, 44 * GB, 82)},
    {"id": 2, "ip": "127.0.0.1", "name": "gpu-node-02", "state": "ready", "cluster_id": 1,
     "labels": {}, "worker_version": "v2.2.0", "status": _gpu("NVIDIA A100-SXM4-40GB", 40 * GB, 21 * GB, 46)},
    {"id": 3, "ip": "127.0.0.1", "name": "gpu-node-03", "state": "ready", "cluster_id": 1,
     "labels": {}, "worker_version": "v2.2.0", "status": _gpu("NVIDIA RTX 4090", 24 * GB, 9 * GB, 12)},
    {"id": 4, "ip": "127.0.0.1", "name": "gpu-node-04", "state": "ready", "cluster_id": 1,
     "labels": {}, "worker_version": "v2.2.0", "status": _gpu("NVIDIA L40S", 48 * GB, 3 * GB, 4)},
]

# (repo_id, size_gb, [worker ids that HOLD it], [worker ids GPUStack is downloading to])
MODELS = [
    ("meta-llama/Llama-3.1-70B-Instruct", 140, [1, 2], []),
    ("Qwen/Qwen2.5-32B-Instruct", 64, [1], [3]),
    ("mistralai/Mistral-Small-24B-Instruct-2501", 47, [2], []),
    ("deepseek-ai/DeepSeek-R1-Distill-Qwen-14B", 28, [1, 3], []),
    ("google/gemma-2-27b-it", 54, [4], []),
    ("Qwen/Qwen2.5-Coder-7B-Instruct", 15, [2], []),
]

MODEL_FILES = []
_mid = 100
for repo, gb, holders, pending in MODELS:
    path = f"{CACHE}/{repo}"
    for wid in holders:
        MODEL_FILES.append({"id": _mid, "worker_id": wid, "local_dir": path, "size": gb * GB,
                            "state": "ready", "source": "huggingface", "huggingface_repo_id": repo})
        _mid += 1
    for wid in pending:
        MODEL_FILES.append({"id": _mid, "worker_id": wid, "local_dir": path, "size": gb * GB,
                            "state": "downloading", "source": "huggingface", "huggingface_repo_id": repo})
        _mid += 1

INSTANCES = [{"model_id": 7, "worker_id": 2, "state": "running",
              "resolved_path": f"{CACHE}/Qwen/Qwen2.5-Coder-7B-Instruct"}]

PLAN = {
    f"{CACHE}/meta-llama/Llama-3.1-70B-Instruct": [1, 2, 3],
    f"{CACHE}/Qwen/Qwen2.5-32B-Instruct": [1, 3],
    f"{CACHE}/mistralai/Mistral-Small-24B-Instruct-2501": [2, 4],
    f"{CACHE}/deepseek-ai/DeepSeek-R1-Distill-Qwen-14B": [1, 3],
    f"{CACHE}/google/gemma-2-27b-it": [4],
    f"{CACHE}/Qwen/Qwen2.5-Coder-7B-Instruct": [2],
}
(STATE / "plan.json").write_text(json.dumps({k: sorted(v) for k, v in PLAN.items()}))

gp = FastAPI()


def _page(items):
    return {"items": items, "pagination": {"total": len(items), "totalPage": 1, "perPage": 100}}


@gp.get("/v2/workers")
async def gp_workers():
    return _page(WORKERS)


@gp.get("/v2/model-files")
async def gp_mf():
    return _page(MODEL_FILES)


@gp.get("/v2/model-instances")
async def gp_mi():
    return _page(INSTANCES)


@gp.get("/v2/clusters")
async def gp_cl():
    return _page([{"id": 1, "name": "production"}])


@gp.get("/v2/models/{mid}")
async def gp_getm(mid: int):
    return {"id": mid, "name": f"model-{mid}", "worker_selector": {}}


@gp.api_route("/v2/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def gp_any(path: str):
    return _page([])


st = FastAPI()
# per-folder profiles: a spread of complete/syncing so the matrix shows bars + badges
_PROFILES = [
    {"state": "idle", "need": 0},
    {"state": "syncing", "need": 18 * GB},
    {"state": "idle", "need": 0},
    {"state": "syncing", "need": 40 * GB},
    {"state": "idle", "need": 0},
]


@st.get("/rest/system/status")
async def st_status():
    return {"myID": "DEMO-DEVICE-ID-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}


@st.get("/rest/db/status")
async def st_dbstatus(folder: str):
    p = _PROFILES[int(hashlib.sha1(folder.encode()).hexdigest(), 16) % len(_PROFILES)]
    g = 100 * GB
    return {"globalBytes": g, "needBytes": p["need"], "state": p["state"],
            "localBytes": g - p["need"], "errors": 0, "pullErrors": 0, "receiveOnlyChangedBytes": 0}


@st.get("/rest/system/connections")
async def st_conns():
    return {"connections": {
        "PEER-A": {"connected": True, "inBytesTotal": 900 * GB, "outBytesTotal": 400 * GB},
        "PEER-B": {"connected": True, "inBytesTotal": 120 * GB, "outBytesTotal": 800 * GB}}}


@st.get("/rest/db/ignores")
async def st_getign(folder: str):
    return {"ignore": ["(?d).cache", "*.tmp", "*.part"]}


@st.get("/rest/config/folders")
async def st_folders():
    return []


@st.api_route("/rest/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def st_any(path: str, request: Request):
    return {}


async def main() -> None:
    from modelsync.app import app as orch  # import AFTER env is set

    servers = [uvicorn.Server(uvicorn.Config(a, host="127.0.0.1", port=p, log_level="warning"))
               for a, p in ((gp, GP_PORT), (st, ST_PORT), (orch, ORCH_PORT))]
    print(f"demo up -> http://127.0.0.1:{ORCH_PORT}  (Ctrl-C to stop)")
    await asyncio.gather(*(s.serve() for s in servers))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
