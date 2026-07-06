"""Full-loop e2e in one process: the REAL orchestrator app, GPUStackClient,
client_for and SyncthingClient, talking over a host-routing httpx transport to
a GPUStack-shaped fake and one fake Syncthing per node. Exercises the whole
advertised automation: plan -> wire shares -> sync complete -> register in
GPUStack -> remove -> deregister -> GC. Everything the piecewise tests fake at
a Python boundary here crosses a (virtual) HTTP wire."""

import json

import httpx
import pytest
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.testclient import TestClient

import modelsync.app as A
from modelsync.gpustack import GPUStackClient

GP_TOKEN = "gp-tok"
ST_KEY = "st-key"
PATH = "/var/lib/gpustack/m"


def make_gpustack():
    db = {
        "workers": [{"id": i, "ip": f"10.0.0.{i}", "name": f"n{i}", "state": "ready",
                     "labels": {}} for i in (1, 2)],
        "models": {7: {"id": 7, "name": "deploy-m", "worker_selector": {"zone": "a"}}},
        # worker 1 already holds the model (the sync source), with a real spec
        "model_files": {50: {"id": 50, "worker_id": 1, "local_dir": PATH, "size": 10,
                             "state": "ready", "source": "hf",
                             "huggingface_repo_id": "o/r"}},
        "next_id": 100,
        "deleted": [],
    }
    gp = FastAPI()

    def auth(authorization: str | None):
        if authorization != f"Bearer {GP_TOKEN}":
            raise HTTPException(status_code=401)

    def page(items):
        return {"items": items,
                "pagination": {"total": len(items), "totalPage": 1, "perPage": 100}}

    @gp.get("/v2/workers")
    async def workers(authorization: str | None = Header(None)):
        auth(authorization)
        return page(db["workers"])

    @gp.get("/v2/model-files")
    async def model_files(authorization: str | None = Header(None)):
        auth(authorization)
        return page(list(db["model_files"].values()))

    @gp.get("/v2/model-files/{mid}")
    async def get_mf(mid: int, authorization: str | None = Header(None)):
        auth(authorization)
        if mid not in db["model_files"]:
            raise HTTPException(status_code=404)
        return db["model_files"][mid]

    @gp.post("/v2/model-files")
    async def create_mf(request: Request, authorization: str | None = Header(None)):
        auth(authorization)
        body = json.loads(await request.body())
        mid = db["next_id"]
        db["next_id"] += 1
        db["model_files"][mid] = {"id": mid, **body,
                                  "local_dir": PATH, "state": "ready", "size": 10}
        return {"id": mid}

    @gp.delete("/v2/model-files/{mid}")
    async def delete_mf(mid: int, request: Request, authorization: str | None = Header(None)):
        auth(authorization)
        db["model_files"].pop(mid, None)
        db["deleted"].append(mid)
        db["last_delete_cleanup"] = request.query_params.get("cleanup")
        return {"ok": True}

    @gp.get("/v2/model-instances")
    async def instances(authorization: str | None = Header(None)):
        auth(authorization)
        return page([])

    @gp.get("/v2/models/{mid}")
    async def get_model(mid: int, authorization: str | None = Header(None)):
        auth(authorization)
        if mid not in db["models"]:
            raise HTTPException(status_code=404)
        return db["models"][mid]

    @gp.put("/v2/models/{mid}")
    async def put_model(mid: int, request: Request, authorization: str | None = Header(None)):
        auth(authorization)
        db["models"][mid] = json.loads(await request.body())
        return db["models"][mid]

    @gp.get("/v2/workers/{wid}")
    async def get_worker(wid: int, authorization: str | None = Header(None)):
        auth(authorization)
        w = next((w for w in db["workers"] if w["id"] == wid), None)
        if w is None:
            raise HTTPException(status_code=404)
        return w

    @gp.put("/v2/workers/{wid}")
    async def put_worker(wid: int, request: Request, authorization: str | None = Header(None)):
        auth(authorization)
        body = json.loads(await request.body())
        for i, w in enumerate(db["workers"]):
            if w["id"] == wid:
                db["workers"][i] = body
        return body

    return gp, db


def make_syncthing(dev_id):
    # status_override: folder -> partial db/status dict (simulate stuck/dirty)
    st = {"options": {}, "devices": {}, "folders": {}, "ignores": {},
          "status_override": {}, "resets": [], "reverts": [], "overrides": []}
    app = FastAPI()

    def auth(x_api_key: str | None):
        if x_api_key != ST_KEY:
            raise HTTPException(status_code=401)

    @app.get("/rest/system/status")
    async def status(x_api_key: str | None = Header(None)):
        auth(x_api_key)
        return {"myID": dev_id}

    @app.patch("/rest/config/options")
    async def options(request: Request, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["options"].update(json.loads(await request.body()))
        return {}

    @app.put("/rest/config/devices/{did}")
    async def put_device(did: str, request: Request, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["devices"][did] = json.loads(await request.body())
        return {}

    @app.put("/rest/config/folders/{fid}")
    async def put_folder(fid: str, request: Request, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["folders"][fid] = json.loads(await request.body())
        return {}

    @app.get("/rest/config/folders")
    async def list_folders(x_api_key: str | None = Header(None)):
        auth(x_api_key)
        return list(st["folders"].values())

    @app.delete("/rest/config/folders/{fid}")
    async def delete_folder(fid: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        if fid not in st["folders"]:
            raise HTTPException(status_code=404)
        del st["folders"][fid]
        return {}

    @app.get("/rest/db/ignores")
    async def get_ignores(folder: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        return {"ignore": st["ignores"].get(folder)}

    @app.post("/rest/db/ignores")
    async def set_ignores(folder: str, request: Request, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["ignores"][folder] = json.loads(await request.body())["ignore"]
        return {}

    @app.get("/rest/config/folders/{fid}")
    async def get_folder(fid: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        if fid not in st["folders"]:
            raise HTTPException(status_code=404)
        return st["folders"][fid]

    @app.patch("/rest/config/folders/{fid}")
    async def patch_folder(fid: str, request: Request, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        if fid not in st["folders"]:
            raise HTTPException(status_code=404)
        st["folders"][fid].update(json.loads(await request.body()))
        return st["folders"][fid]

    @app.post("/rest/system/reset")
    async def system_reset(folder: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["resets"].append(folder)
        st["status_override"].pop(folder, None)  # index rebuilt clean, like the daemon
        return {}

    @app.get("/rest/db/status")
    async def db_status(folder: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        if folder not in st["folders"]:
            raise HTTPException(status_code=404)  # unknown folder, like the real daemon
        base = {"globalBytes": 10, "needBytes": 0, "state": "idle", "localBytes": 10,
                "errors": 0, "pullErrors": 0, "receiveOnlyChangedBytes": 0}
        base.update(st["status_override"].get(folder, {}))
        return base

    @app.post("/rest/db/override")
    async def override(folder: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["overrides"].append(folder)
        return {}

    @app.post("/rest/db/revert")
    async def revert(folder: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        st["reverts"].append(folder)
        return {}

    return app, st


class HostRouter(httpx.AsyncBaseTransport):
    """Dispatch by request host: 'gp' -> fake GPUStack, node IPs -> fake Syncthings.
    Hosts in `down` raise ConnectError, simulating a dead node."""

    def __init__(self, apps):
        self._transports = {host: httpx.ASGITransport(app=app) for host, app in apps.items()}
        self.down: set[str] = set()

    async def handle_async_request(self, request):
        if request.url.host in self.down:
            raise httpx.ConnectError("connection refused", request=request)
        return await self._transports[request.url.host].handle_async_request(request)


@pytest.fixture
def loop_env(monkeypatch):
    monkeypatch.setattr(A.settings, "auth_token", "")
    monkeypatch.setattr(A.settings, "reconcile_interval", 9999)
    monkeypatch.setattr(A.settings, "syncthing_api_key", ST_KEY)
    gp_app, gp_db = make_gpustack()
    st1_app, st1 = make_syncthing("DEV1" * 13)
    st2_app, st2 = make_syncthing("DEV2" * 13)
    transport = HostRouter({"gp": gp_app, "10.0.0.1": st1_app, "10.0.0.2": st2_app})
    router = httpx.AsyncClient(transport=transport)
    with TestClient(A.app) as c:
        A.state.http = router  # real client_for now reaches the fake Syncthings
        A.state.gpustack = GPUStackClient("http://gp", GP_TOKEN, router, "/v2",
                                          [], ["/var/lib/gpustack"])
        A.state.plan, A.state.registry, A.state.members = {}, {}, set()
        A.state.pins, A.state.stuck_seen = {}, {}
        monkeypatch.setattr(A, "_ensure_event_watchers", lambda *a: None)
        yield c, gp_db, st1, st2, transport


def test_full_loop_register_then_deregister(loop_env):
    c, gp_db, st1, st2, _router = loop_env

    # --- apply: sync PATH to nodes 1 (holder/source) + 2 (new replica) ---
    r = c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()
    assert r["ok"], r

    fid = next(iter(st1["folders"]))
    assert st1["folders"][fid]["type"] == "sendonly"       # confirmed holder = source
    assert st2["folders"][fid]["type"] == "receiveonly"    # replica converges to it
    assert st1["folders"][fid]["path"] == PATH
    assert st1["ignores"][fid]                             # temp/partial excludes installed
    assert st1["options"]["globalAnnounceEnabled"] is False  # LAN-only enforced
    assert st1["devices"] and st2["devices"]               # both sides wired, no accept

    # fake Syncthing reports the folder complete -> registered on node 2 in GPUStack
    key = f"2@{PATH}"
    assert A.state.registry.get(key) == 100
    assert gp_db["model_files"][100]["worker_id"] == 2
    assert gp_db["model_files"][100]["source"] == "hf"     # spec cloned from the source

    # --- remove node 2: deregister OUR record, keep the original, GC the share ---
    r2 = c.post("/plan", json={"plan": {PATH: [1]}}).json()
    assert r2["ok"], r2
    assert 100 in gp_db["deleted"]                         # our record removed
    assert 50 in gp_db["model_files"]                      # user's original untouched
    assert A.state.registry == {}
    assert fid not in st2["folders"]                       # replica share GC'd
    assert fid in st1["folders"]                           # source keeps its share


def test_stuck_replica_escalates_to_heavy_reset(loop_env):
    """Replica idle-but-needing (poisoned index) with a clean source: pass 1-2
    gentle (override source + revert replica), pass 3 heavy reset (pause ->
    drop index -> unpause) — the whole sequence over the REST wire."""
    c, _gp_db, st1, st2, _router = loop_env
    assert c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()["ok"]
    fid = next(iter(st2["folders"]))
    # poison the replica: settled at 50%, still needing bytes
    st2["status_override"][fid] = {"needBytes": 5, "localBytes": 5}

    for _ in range(3):  # each apply runs one reconcile pass
        assert c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()["ok"]

    assert st1["overrides"]                      # clean source forced authoritative
    assert st2["reverts"]                        # gentle revert attempted first
    assert st2["resets"] == [fid]                # 3rd stuck pass: index dropped
    assert st2["folders"][fid]["paused"] is False  # never left paused
    # index rebuilt clean -> one more pass detects nothing stuck
    assert c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()["ok"]
    assert st2["resets"] == [fid]                # no reset loop


def test_dead_node_is_reported_not_fatal(loop_env):
    c, _gp_db, st1, _st2, router = loop_env
    router.down.add("10.0.0.2")
    r = c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()
    assert r["ok"]
    assert any("unreachable" in w for w in r["warnings"])   # surfaced, never silent
    assert st1["folders"]                                   # live node still wired
    # node recovers -> next apply wires it with no manual intervention
    router.down.clear()
    assert c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()["ok"]


def test_pin_unpin_over_the_wire(loop_env):
    c, gp_db, _st1, _st2, _router = loop_env
    r = c.post("/pin", json={"path": PATH, "model_id": 7}).json()
    assert r["ok"] and r["holders"] == [1]
    label = r["label"]
    w1 = next(w for w in gp_db["workers"] if w["id"] == 1)
    assert w1["labels"] == {label: "true"}                  # holder labelled
    assert gp_db["models"][7]["worker_selector"] == {label: "true"}

    r2 = c.post("/unpin", json={"model_id": 7}).json()
    assert r2["ok"]
    assert gp_db["models"][7]["worker_selector"] == {"zone": "a"}  # original restored
    w1 = next(w for w in gp_db["workers"] if w["id"] == 1)
    assert w1["labels"] == {}                               # label stripped


def test_reset_over_the_wire(loop_env):
    """Operator /reset: override the complete source, heavy-reset the incomplete
    replica (pause -> drop index -> unpause), never leave it paused."""
    c, _gp_db, st1, st2, _router = loop_env
    assert c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()["ok"]
    fid = next(iter(st2["folders"]))
    st2["status_override"][fid] = {"needBytes": 5, "localBytes": 5}  # incomplete replica

    r = c.post("/reset", json={"path": PATH}).json()
    assert r["ok"] and r["source"] == 1
    assert st1["overrides"]                          # complete source overrode
    assert st2["resets"] == [fid]                    # replica index dropped
    assert st2["folders"][fid]["paused"] is False    # never left paused
    assert any("override (source)" in a for a in r["actions"])
    assert any("index reset" in a for a in r["actions"])


def test_purge_over_the_wire(loop_env):
    c, gp_db, _st1, st2, _router = loop_env
    assert c.post("/plan", json={"plan": {PATH: [1, 2]}}).json()["ok"]
    assert A.state.registry            # node 2's copy registered (id 100)
    fid = next(iter(st2["folders"]))

    r = c.post("/purge", json={"path": PATH, "worker_id": 2, "delete_files": True}).json()
    assert r["ok"], r
    assert 100 in gp_db["deleted"]                       # our record removed
    assert gp_db["last_delete_cleanup"] == "true"        # files-on-disk delete requested
    assert 50 in gp_db["model_files"]                    # user's original untouched
    assert A.state.registry == {} and A.state.plan == {PATH: {1}}
    assert fid not in st2["folders"]                     # share unshared (GC)
