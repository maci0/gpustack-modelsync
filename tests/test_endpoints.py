"""Endpoint integration tests via TestClient with a faked GPUStack + Syncthing.
Covers the HTTP request/response shaping (plan diff, status integrity, models)."""

import pytest
from fastapi.testclient import TestClient

import modelsync.app as A
from modelsync.gpustack import ModelFolder, ModelInstance, Worker


class FakeGP:
    def __init__(self):
        self.deleted: list[tuple[int, bool]] = []
        self.ws = [Worker(id=1, name="a", ip="10.0.0.1", state="ready", cluster_id=1),
                   Worker(id=2, name="b", ip="10.0.0.2", state="ready", cluster_id=1)]
        self.folders = [ModelFolder(path="/var/lib/gpustack/m", label="m", size=100,
                                    current_nodes=[1], spec={"source": "huggingface"})]
        self.instances = [ModelInstance(model_id=1, worker_id=1, state="running",
                                        local_dir="/var/lib/gpustack/m")]

    async def workers(self):
        return self.ws

    async def model_folders(self):
        return self.folders

    async def model_instances(self):
        return self.instances

    async def register_synced(self, wid, spec):
        return 1

    async def get_model_file(self, mid):
        return None

    async def delete_model_file(self, mid, cleanup=False):
        self.deleted.append((mid, cleanup))

    async def find_model_file(self, path, wid):
        return 42 if (path, wid) == ("/var/lib/gpustack/m", 2) else None

    async def clusters(self):
        return [{"id": 1, "name": "test"}]


class FakeSt:
    async def enforce_local_only(self, *a): pass
    async def set_ignores(self, fid, patterns=None): pass
    async def remote_completion(self, fid, dev): return 0.0
    async def my_id(self): return "DEV"
    async def put_device(self, *a): pass
    async def put_folder(self, *a, **k): pass
    async def owned_folders(self): return set()
    async def delete_folder(self, fid): pass
    async def revert(self, fid): pass
    async def override(self, fid): pass

    async def folder_status(self, fid):
        return {"completion": 100.0, "complete": True, "state": "idle", "need_bytes": 0,
                "global_bytes": 100, "local_bytes": 100, "receive_only_changed": 0, "errors": 0}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(A.settings, "auth_token", "")  # test logic, not auth
    monkeypatch.setattr(A.settings, "reconcile_interval", 9999)  # keep the bg loop out
    with TestClient(A.app) as c:
        A.state.gpustack = FakeGP()
        A.state.plan, A.state.registry, A.state.members, A.state.resetting = {}, {}, set(), set()
        monkeypatch.setattr(A, "client_for", lambda w: FakeSt())
        monkeypatch.setattr(A, "_ensure_event_watchers", lambda *a: None)
        yield c


def test_nodes_and_models(client):
    nodes = client.get("/nodes").json()
    assert {n["id"] for n in nodes} == {1, 2}
    models = client.get("/models").json()
    assert len(models) == 1
    m = models[0]
    assert m["have"] == [1] and m["serving"] == [1] and m["nodes"] == []  # plan empty


def test_plan_apply_reports_added(client):
    r = client.post("/plan", json={"plan": {"/var/lib/gpustack/m": [1, 2]}}).json()
    assert r["ok"] and r["models"] == 1
    assert any("b" in a for a in r["added"])  # node 2 (b) newly added
    # unknown path is rejected with a warning, not applied
    r2 = client.post("/plan", json={"plan": {"/unknown": [1]}}).json()
    assert r2["models"] == 0 and any("unknown" in w for w in r2["warnings"])


def test_status_exposes_integrity(client):
    client.post("/plan", json={"plan": {"/var/lib/gpustack/m": [1, 2]}})
    rows = client.get("/status").json()
    assert rows and all("clean" in r and "expected_bytes" in r and "local_bytes" in r for r in rows)


def test_suggest_lists_running_not_in_plan(client):
    s = client.get("/suggest").json()
    assert {"path": "/var/lib/gpustack/m", "worker_id": 1} in s  # instance runs, not planned


def test_reset_unknown_path(client):
    r = client.post("/reset", json={"path": "/not/planned"}).json()
    assert r["ok"] is False and "not in current plan" in r["error"]


def test_metrics_prometheus_format(client):
    A.state.metrics = {"rows": 4, "rows_clean": 4}
    A.state.counters = {"registered": 2, "deregistered": 1, "stuck_resolved": 0, "reconciles": 9}
    body = client.get("/metrics").text
    assert "modelsync_rows 4" in body and "modelsync_registered_total 2" in body
    assert "# TYPE modelsync_reconciles_total counter" in body


def test_purge_refuses_running_instance(client):
    r = client.post("/purge", json={"path": "/var/lib/gpustack/m", "worker_id": 1,
                                    "delete_files": True}).json()
    assert r["ok"] is False and "instance is running" in r["error"]  # fixture serves there


def test_purge_deletes_record_with_cleanup(client):
    # node 2: no instance running; fake maps (path, 2) -> model-file 42
    r = client.post("/purge", json={"path": "/var/lib/gpustack/m", "worker_id": 2,
                                    "delete_files": True}).json()
    assert r["ok"] is True
    assert (42, True) in A.state.gpustack.deleted        # cleanup delete issued
    assert any("deleted record + files" in a for a in r["actions"])


def test_clusters_endpoint(client):
    assert client.get("/clusters").json() == [{"id": 1, "name": "test"}]
