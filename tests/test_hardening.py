"""Tests for the round-3 hardening: path-root guard, auth, heavy-reset
always-unpause, pagination stop conditions, unknown-path rejection."""

import fastapi
import httpx
import pytest

from modelsync.app import _check_token, _eligible, _heavy_reset
from modelsync.config import settings
from modelsync.gpustack import GPUStackClient, ModelFolder, Worker, _under_roots

ROOTS = ["/var/lib/gpustack"]


def test_under_roots_rejects_traversal_and_root_itself():
    assert _under_roots("/var/lib/gpustack/cache/m", ROOTS)
    assert not _under_roots("/var/lib/gpustack", ROOTS)            # equal to root
    assert not _under_roots("/var/lib/gpustack/../../etc/x", ROOTS)  # traversal
    assert not _under_roots("/var/lib/gpustackEVIL/x", ROOTS)       # prefix confusion
    assert not _under_roots("/etc/passwd", ROOTS)


def test_check_token(monkeypatch):
    monkeypatch.setattr(settings, "auth_token", "")
    _check_token("anything")  # empty config -> open, no raise
    monkeypatch.setattr(settings, "auth_token", "s3kret")
    _check_token("s3kret")  # correct
    with pytest.raises(fastapi.HTTPException):
        _check_token("wrong")
    with pytest.raises(fastapi.HTTPException):
        _check_token("café")  # non-ascii must 401, not TypeError/500


class FakeReset:
    def __init__(self, reset_raises=False, can_pause=True):
        self.paused = []
        self.reset_called = False
        self.reset_raises = reset_raises
        self.can_pause = can_pause

    async def set_paused(self, fid, p):
        self.paused.append(p)

    async def folder_status(self, fid):
        return {"state": "paused" if self.can_pause else "syncing"}

    async def reset_folder_db(self, fid):
        self.reset_called = True
        if self.reset_raises:
            raise httpx.HTTPError("boom")


async def test_heavy_reset_always_unpauses_even_on_failure():
    c = FakeReset(reset_raises=True)
    await _heavy_reset(c, "fid")
    assert c.paused[0] is True and c.paused[-1] is False  # paused, then unpaused
    assert c.reset_called  # it tried (folder was paused)


async def test_heavy_reset_skips_reset_if_never_paused(monkeypatch):
    monkeypatch.setattr("asyncio.sleep", _no_sleep)
    c = FakeReset(can_pause=False)
    msg = await _heavy_reset(c, "fid")
    assert not c.reset_called and "could not pause" in msg
    assert c.paused[-1] is False  # still unpaused


async def _no_sleep(_):  # avoid the 15s poll in the never-paused test
    return None


def test_eligible_rejects_unknown_path():
    w = Worker(id=1, name="n", ip="10.0.0.1", state="ready", free_bytes=99 * 2**30)
    folders = [ModelFolder(path="/known", label="known", size=1, current_nodes=[1])]
    plan, warn = _eligible({"/unknown": {1}}, [w], folders)
    assert plan == {} and any("unknown model path" in x for x in warn)


def _client():
    return GPUStackClient("http://x", "t", None)  # http unused; _get is patched


async def test_model_folders_size_max_across_nodes_sum_within():
    gp = _client()

    async def fake_list(path, **k):
        return [
            {"local_dir": "/var/lib/gpustack/A", "worker_id": 7, "size": 100},
            {"local_dir": "/var/lib/gpustack/A", "worker_id": 8, "size": 100},  # same model, 2nd node
            {"local_dir": "/var/lib/gpustack/B", "worker_id": 7, "size": 30},   # sharded on one node
            {"local_dir": "/var/lib/gpustack/B", "worker_id": 7, "size": 40},
        ]

    gp._list = fake_list
    byp = {f.path: f for f in await gp.model_folders()}
    assert byp["/var/lib/gpustack/A"].size == 100  # max across nodes, NOT 200
    assert byp["/var/lib/gpustack/A"].current_nodes == [7, 8]
    assert byp["/var/lib/gpustack/B"].size == 70   # summed within a node


async def test_list_stops_on_short_page():
    gp, calls = _client(), []

    async def fake_get(path, **p):
        calls.append(p["page"])
        return {"items": [1] * (100 if p["page"] < 3 else 40)}

    gp._get = fake_get
    out = await gp._list("/x")
    assert len(out) == 240 and calls == [1, 2, 3]


async def test_list_honors_total_pages_metadata():
    gp, calls = _client(), []

    async def fake_get(path, **p):
        calls.append(p["page"])
        return {"items": [1] * 100, "pagination": {"totalPage": 2}}  # server caps pages

    gp._get = fake_get
    out = await gp._list("/x")
    assert len(out) == 200 and calls == [1, 2]


async def test_list_caps_runaway_server():
    gp, calls = _client(), []

    async def fake_get(path, **p):  # never short, no metadata -> would loop forever
        calls.append(p["page"])
        return {"items": [1] * 100}

    gp._get = fake_get
    out = await gp._list("/x", max_pages=5)
    assert len(calls) == 5 and len(out) == 500
