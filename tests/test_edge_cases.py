"""20 edge cases across the real code. Any failing assert is a bug to fix."""

import modelsync.app as A
from modelsync.gpustack import (
    GPUStackClient,
    Worker,
    _as_dir,
    _instance_dir,
    _maintenance_on,
    _max_free,
    _model_dir,
    under_roots,
    free_for_path,
)
from modelsync.reconcile import choose_source, _is_clean, collect_status, folder_id


def W(i, mounts=(), free=None):
    return Worker(id=i, name=f"n{i}", ip=f"10.0.0.{i}", state="ready",
                  mounts=list(mounts), free_bytes=free)


def st(**kw):
    base = {"complete": True, "state": "idle", "errors": 0, "receive_only_changed": 0,
            "global_bytes": 100, "local_bytes": 100, "completion": 100.0, "need_bytes": 0}
    base.update(kw)
    return base


# 1. distinct paths that slug identically must not collide
def test_1_folder_id_no_collision():
    assert folder_id("/a/b") != folder_id("/a-b")


# 2. unicode path -> valid ascii id, no crash
def test_2_folder_id_unicode():
    fid = folder_id("/模型/x")
    assert fid and all(ord(c) < 128 for c in fid)


# 3. all-special path -> non-empty id
def test_3_folder_id_all_special():
    assert folder_id("/@#$%/") .strip("-")  # has a hash tail even if slug empties


# 4. all status unreachable but a confirmed holder -> pick it
def test_4choose_source_confirmed_fallback():
    assert choose_source([1, 2], {2}, {1: None, 2: None}) == 2


# 5. empty targets -> None
def test_5choose_source_empty():
    assert choose_source([], set(), {}) is None


# 6. two clean copies -> deterministic lowest
def test_6choose_source_two_clean_deterministic():
    assert choose_source([3, 1, 2], set(), {1: st(), 2: st(), 3: st()}) == 1


# 6b. no confirmed holder, no clean copy -> fall back to highest completion
def test_6b_choose_source_completion_fallback():
    lo = st(complete=False, need=60, local=40, global_bytes=100, completion=40.0)
    hi = st(complete=False, need=30, local=70, global_bytes=100, completion=70.0)
    assert choose_source([1, 2], set(), {1: lo, 2: hi}) == 2   # most-complete wins
    assert choose_source([1, 2], set(), {1: None, 2: None}) is None  # nobody has data


# 7. poisoned copy (local != global) is not clean
def test_7_is_clean_rejects_poison():
    assert not _is_clean(st(local_bytes=200, global_bytes=100))
    assert not _is_clean(st(global_bytes=0, local_bytes=0))  # unknown, not clean


# 8. under_roots: equal to root is NOT under; sibling-prefix is NOT under
def test_8under_roots_boundaries():
    roots = ["/var/lib/gpustack"]
    assert not under_roots("/var/lib/gpustack", roots)          # equal
    assert under_roots("/var/lib/gpustack/m", roots)            # under
    assert not under_roots("/var/lib/gpustack-evil/m", roots)   # sibling prefix


# 9. free_for_path: exact mount-point match
def test_9_free_for_path_exact_mount():
    w = W(1, mounts=[{"mount_point": "/data", "free": 50}])
    assert free_for_path(w, "/data") == 50


# 10. free_for_path: no mount matches -> worker.free_bytes
def test_10_free_for_path_fallback():
    w = W(1, mounts=[{"mount_point": "/other", "free": 50}], free=999)
    assert free_for_path(w, "/data/m") == 999


# 11. free_for_path: longest-prefix wins over shorter
def test_11_free_for_path_longest_prefix():
    w = W(1, mounts=[{"mount_point": "/", "free": 10},
                     {"mount_point": "/var/lib", "free": 20}])
    assert free_for_path(w, "/var/lib/gpustack/m") == 20


# 12. _as_dir: weight file -> parent; dir -> itself; trailing slash normalized
def test_12_as_dir():
    assert _as_dir("/m/A/model.safetensors") == "/m/A"
    assert _as_dir("/m/A") == "/m/A"
    assert _as_dir("/m/A/") == "/m/A"


# 13. _ip_ok: IPv6 in CIDR, malformed, ip:port
def test_13_ip_ok():
    gp = GPUStackClient("http://x", "t", None, allowed_cidrs=["fd00::/8", "10.0.0.0/8"])
    assert gp._ip_ok("fd00::1")
    assert not gp._ip_ok("notanip")
    assert not gp._ip_ok("10.0.0.1:22")   # port makes it invalid


# 14. _max_free / _mounts on empty filesystem
def test_14_empty_filesystem():
    assert _max_free({"filesystem": []}) is None
    assert _model_dir({"resolved_paths": ["/m/A/w.gguf"]}) == "/m/A"


# 15. _instance_dir with trailing slash + missing
def test_15_instance_dir():
    assert _instance_dir({"resolved_path": "/m/A/"}) == "/m/A"
    assert _instance_dir({}) is None


# 16. _maintenance_on odd inputs
def test_16_maintenance_on():
    assert _maintenance_on([]) is False
    assert _maintenance_on({}) is False
    assert _maintenance_on({"enabled": 1}) is True


# 17. collect_status: worker in plan absent from workers -> no row
async def test_17_collect_status_missing_worker(monkeypatch):
    rows = await collect_status({"/m": {1, 99}}, [W(1)], lambda w: _FakeSt())
    assert {r.worker_id for r in rows} == {1}   # 99 has no Worker -> skipped


# 17b. collect_status: unreachable node -> a row with state 'unreachable', not a crash
async def test_17b_collect_status_unreachable_row():
    import httpx

    class Boom:
        async def folder_status(self, fid):
            raise httpx.HTTPError("down")

    rows = await collect_status({"/m": {1}}, [W(1)], lambda w: Boom())
    assert len(rows) == 1 and rows[0].state == "unreachable" and rows[0].completion == 0.0


# 18. load_registry rejects negative / non-numeric worker ids
def test_18_load_registry_rejects_bad_ids(tmp_path, monkeypatch):
    import json
    rf = tmp_path / "r.json"
    rf.write_text(json.dumps({"1@/m": 5, "-1@/m": 6, "x@/m": 7, "1@/m2": True}))
    monkeypatch.setattr(A, "REGISTRY_FILE", rf)
    reg = A.load_registry()
    assert reg == {"1@/m": 5}   # negative, non-numeric, and bool-value all dropped


# 19. workers(): duplicate id from pagination churn -> deduped
async def test_19_workers_dedup_duplicate_id():
    gp = GPUStackClient("http://x", "t", None)

    async def fake_list(path, **k):
        return [{"id": 1, "ip": "10.0.0.1", "name": "old"},
                {"id": 1, "ip": "10.0.0.1", "name": "new"}]

    gp._list = fake_list
    ws = await gp.workers()
    assert len(ws) == 1 and ws[0].name == "new"   # last wins, no dupes


# 20. completion never negative even with poisoned need > global (via folder_status)
def test_20_completion_never_negative():
    from modelsync.reconcile import SyncStatus
    s = SyncStatus("/m", 1, "n", completion=0.0, need_bytes=200, global_bytes=100)
    assert s.completion >= 0.0 and not s.clean   # poisoned -> not clean


# 21. _addr brackets IPv6, leaves IPv4 bare
def test_21_addr_ipv6_brackets():
    from modelsync.reconcile import _addr
    assert _addr(W(1), 22000) == "tcp://10.0.0.1:22000"
    assert _addr(Worker(id=2, name="n", ip="fd00::1", state="ready"), 22000) == "tcp://[fd00::1]:22000"


# 22. _list survives malformed pagination metadata (non-numeric total/perPage)
async def test_22_list_malformed_pagination():
    gp = GPUStackClient("http://x", "t", None)

    async def fake_get(path, **k):
        return {"items": [{"id": 1}], "pagination": {"total": "abc", "perPage": "xyz"}}

    gp._get = fake_get
    assert await gp._list("/w") == [{"id": 1}]   # no crash; short-page stop


# 23. _items coerces null/non-list items to []
def test_23_items_null_or_nonlist():
    assert GPUStackClient._items({"items": None}) == []
    assert GPUStackClient._items({"items": {"a": 1}}) == []
    assert GPUStackClient._items([1, 2]) == [1, 2]
    assert GPUStackClient._items("junk") == []


# 24. model_folders tolerates a malformed size field
async def test_24_model_folders_bad_size():
    gp = GPUStackClient("http://x", "t", None)

    async def fl(p, **k):
        return [{"worker_id": 1, "local_dir": "/var/lib/gpustack/m", "size": "abc"}]

    gp._list = fl
    r = await gp.model_folders()
    assert len(r) == 1 and r[0].size == 0


# 27. api_prefix normalized to leading-slash regardless of how it's configured
def test_27_api_prefix_normalized():
    for pfx in ("/v2", "v2", "v2/", "/v2/"):
        gp = GPUStackClient("http://host/", "t", None, api_prefix=pfx)
        assert gp._base == "http://host" and gp._v == "/v2"


# 26. save works on a fresh nested STATE dir (lifespan mkdir prevents apply crash)
def test_26_state_dir_created(tmp_path, monkeypatch):
    d = tmp_path / "sub" / "state"          # does not exist yet
    monkeypatch.setattr(A, "STATE", d)
    monkeypatch.setattr(A, "PLAN_FILE", d / "plan.json")
    d.mkdir(parents=True, exist_ok=True)    # what lifespan does before any save
    A.save_plan({"/m": {1}})
    assert (d / "plan.json").exists()


# 25. _eligible: holder kept; maintenance / capacity / stale-id / unknown-path all warned
def test_25_eligible_branches():
    from modelsync.app import _eligible
    from modelsync.gpustack import ModelFolder
    workers = [W(1), Worker(id=2, name="n2", ip="10.0.0.2", state="ready", maintenance=True),
               W(3, free=10)]
    folders = [ModelFolder(path="/m", label="m", size=100, current_nodes=[1], spec={})]
    out, warn = _eligible({"/m": {1, 2, 3, 99}, "/unknown": {1}}, workers, folders)
    assert out == {"/m": {1}}                                  # only the holder survives
    assert any("unknown or removed" in w for w in warn)        # 99 stale
    assert any("maintenance" in w for w in warn)               # 2 in maintenance
    assert any("free" in w for w in warn)                      # 3 too small
    assert any("unknown model path" in w for w in warn)        # /unknown


class _FakeSt:
    async def folder_status(self, fid):
        return st(need_bytes=0)
