"""_reconcile_core: the register/deregister automation (previously only covered
end-to-end on the live cluster). Fakes GPUStack + Syncthing to exercise the
logic: register a completed copy, deregister a removed one, and the id-verify
guard that prevents deleting the wrong model-file."""

import httpx

import modelsync.app as A
from modelsync.gpustack import ModelFolder, Worker


class FakeGP:
    def __init__(self, mf=None):
        self.registered, self.deleted, self.mf = [], [], mf or {}

    async def workers(self):
        return []

    async def model_folders(self):
        return []

    async def register_synced(self, wid, spec):
        self.registered.append((wid, dict(spec)))
        return 99

    async def get_model_file(self, mid):
        return self.mf.get(mid)

    async def delete_model_file(self, mid):
        self.deleted.append(mid)


def _status(complete=True, need=0, local=100, glob=100, state="idle", roc=0):
    return {"completion": 100.0 if not need else 50.0, "complete": complete,
            "state": state, "need_bytes": need, "global_bytes": glob,
            "local_bytes": local, "receive_only_changed": roc, "errors": 0}


class FakeSt:
    def __init__(self, complete=True, status=None, fail_at=None):
        self.st = status if status is not None else _status(complete=complete)
        self.overrode = self.reverted = False
        self.fail_at = fail_at  # "enforce" | "put_folder" | "owned" -> raises there

    def _maybe_fail(self, where):
        if self.fail_at == where:
            raise httpx.HTTPError(f"down@{where}")

    async def enforce_local_only(self, *a):
        self._maybe_fail("enforce")

    async def set_ignores(self, fid, patterns=None):
        pass

    async def remote_completion(self, fid, dev):
        raise httpx.HTTPError("no peer view in this fake")

    async def my_id(self):
        return f"DEV{id(self) % 1000}"

    async def put_device(self, *a):
        pass

    async def put_folder(self, *a, **k):
        self._maybe_fail("put_folder")

    async def owned_folders(self):
        self._maybe_fail("owned")
        return set()

    async def delete_folder(self, fid):
        pass

    async def revert(self, fid):
        self.reverted = True

    async def override(self, fid):
        self.overrode = True

    async def folder_status(self, fid):
        return self.st

    async def set_paused(self, fid, p):
        self.pauses = [*getattr(self, "pauses", []), p]

    async def is_paused(self, fid):
        return True

    async def reset_folder_db(self, fid):
        self.index_reset = True


def _setup(monkeypatch, complete=True, mf=None):
    A.state.plan = {}
    A.state.registry = {}
    A.state.members = set()
    A.state.resetting = set()
    A.state.pins = {}
    A.state.dev_ids = {}
    A.state.ev_tasks = {}
    A.state.stuck_seen = {}
    A.state.counters = {"registered": 0, "deregistered": 0, "stuck_resolved": 0, "reconciles": 0}
    A.state.metrics = {}
    gp = FakeGP(mf)
    A.state.gpustack = gp
    monkeypatch.setattr(A, "client_for", lambda w: FakeSt(complete))
    monkeypatch.setattr(A, "_ensure_event_watchers", lambda *a: None)  # no real watcher tasks in tests
    monkeypatch.setattr(A.settings, "register_in_gpustack", True)
    return gp


def W(i):
    return Worker(id=i, name=f"n{i}", ip=f"10.0.0.{i}", state="ready")


def F(path, nodes=(), size=100):
    return ModelFolder(path=path, label=path, size=size, current_nodes=list(nodes),
                       spec={"source": "huggingface", "huggingface_repo_id": "r"})


async def test_registers_completed_copy_not_yet_in_gpustack(monkeypatch):
    gp = _setup(monkeypatch, complete=True)
    res = await A._reconcile_core({"/m": {1}}, [W(1)], [F("/m", nodes=[])])
    assert gp.registered == [(1, {"source": "huggingface", "huggingface_repo_id": "r"})]
    assert A.state.registry == {"1@/m": 99}
    assert "1@/m" in res["registered"]


async def test_does_not_register_incomplete_or_already_present(monkeypatch):
    gp = _setup(monkeypatch, complete=False)  # not synced yet
    await A._reconcile_core({"/m": {1}}, [W(1)], [F("/m", nodes=[])])
    assert gp.registered == []                # incomplete -> no register
    gp = _setup(monkeypatch, complete=True)
    await A._reconcile_core({"/m": {1}}, [W(1)], [F("/m", nodes=[1])])  # already a holder
    assert gp.registered == []                # already in GPUStack -> skip


async def test_deregisters_removed_copy_with_id_verify(monkeypatch):
    gp = _setup(monkeypatch, mf={99: {"worker_id": 1}})  # id 99 still maps to our worker
    A.state.registry = {"1@/m": 99}
    res = await A._reconcile_core({}, [W(1)], [])  # /m removed from plan
    assert gp.deleted == [99] and A.state.registry == {} and "1@/m" in res["deregistered"]


async def test_deregister_skips_when_id_reused_for_other_worker(monkeypatch):
    gp = _setup(monkeypatch, mf={99: {"worker_id": 2}})  # id reused -> different worker
    A.state.registry = {"1@/m": 99}
    await A._reconcile_core({}, [W(1)], [])
    assert gp.deleted == []                  # must NOT delete someone else's model-file
    assert A.state.registry == {}            # but forget the stale mapping


async def test_resolves_stuck_folder_from_clean_copy(monkeypatch):
    # worker 1 clean (local==global, complete); worker 2 stuck (incomplete, idle,
    # needs bytes). The resolver overrides the clean copy and reverts the stuck one.
    _setup(monkeypatch)
    sts = {1: FakeSt(status=_status(complete=True, local=100, glob=100)),
           2: FakeSt(status=_status(complete=False, need=50, local=0, glob=100))}
    monkeypatch.setattr(A, "client_for", lambda w: sts[w.id])
    await A._reconcile_core({"/m": {1, 2}}, [W(1), W(2)], [F("/m", nodes=[1, 2])])
    assert sts[1].overrode      # clean copy forced authoritative
    assert sts[2].reverted      # stuck replica reverted toward it


async def test_stuck_escalates_to_heavy_reset_after_3_passes(monkeypatch):
    _setup(monkeypatch)
    sts = {1: FakeSt(status=_status(complete=True, local=100, glob=100)),
           2: FakeSt(status=_status(complete=False, need=50, local=0, glob=100))}
    monkeypatch.setattr(A, "client_for", lambda w: sts[w.id])
    for _ in range(3):  # pass 1+2: override+revert only; pass 3: heavy reset
        await A._reconcile_core({"/m": {1, 2}}, [W(1), W(2)], [F("/m", nodes=[1, 2])])
    assert getattr(sts[2], "index_reset", False)        # escalated on the 3rd pass
    assert not getattr(sts[1], "index_reset", False)    # clean source untouched
    assert A.state.stuck_seen[("/m", 2)] == 0           # counter reset after escalation


async def _run_reconcile(sts, have):
    from modelsync.reconcile import reconcile
    return await reconcile({"/m": {1, 2}}, [W(1), W(2)], lambda w: sts[w.id], 22000, {"/m": have})


async def test_reconcile_marks_node_unreachable_at_handshake():
    src = FakeSt(status=_status(complete=True))
    dead = FakeSt(fail_at="enforce")                 # fails device handshake
    unreachable, _ = await _run_reconcile({1: src, 2: dead}, {1})
    assert unreachable == [2]                         # reported, not fatal


async def test_reconcile_marks_node_unreachable_at_put_folder():
    src = FakeSt(status=_status(complete=True))
    rep = FakeSt(status=_status(complete=False, need=50, local=0, glob=100), fail_at="put_folder")
    unreachable, _ = await _run_reconcile({1: src, 2: rep}, {1})
    assert 2 in unreachable                           # wiring failure -> unreachable, no crash


async def test_reconcile_handles_gc_failure():
    src = FakeSt(status=_status(complete=True), fail_at="owned")  # GC listing fails
    rep = FakeSt(status=_status(complete=True))
    unreachable, _ = await _run_reconcile({1: src, 2: rep}, {1})
    assert 1 in unreachable                           # GC error -> node unreachable, not fatal


async def test_reconcile_skips_path_with_all_targets_unreachable():
    from modelsync.reconcile import reconcile
    a, b = FakeSt(fail_at="enforce"), FakeSt(fail_at="enforce")
    unreachable, _ = await reconcile(
        {"/m": {1, 2}}, [W(1), W(2)], lambda w: {1: a, 2: b}[w.id], 22000, {"/m": {1}})
    assert set(unreachable) == {1, 2}                 # both dead -> path skipped, no crash


# revert guard (data-loss-critical): a diverged, incomplete, less-complete replica
# IS reverted; a complete one, or one merely still downloading (no divergence), is NOT.
async def test_revert_guard_reverts_only_diverged_incomplete():
    src = FakeSt(status=_status(complete=True, local=100, glob=100))            # holder, 100%
    rep = FakeSt(status=_status(complete=False, need=50, local=50, glob=100, roc=10))  # diverged, 50%
    await _run_reconcile({1: src, 2: rep}, {1})
    assert rep.reverted


async def test_revert_guard_spares_complete_replica():
    src = FakeSt(status=_status(complete=True, local=100, glob=100))
    rep = FakeSt(status=_status(complete=True, local=100, glob=100))            # complete copy
    await _run_reconcile({1: src, 2: rep}, {1})
    assert not rep.reverted                                                     # never wipe a full copy


async def test_revert_guard_spares_downloading_replica():
    src = FakeSt(status=_status(complete=True, local=100, glob=100))
    rep = FakeSt(status=_status(complete=False, need=50, local=50, glob=100, roc=0))  # just pulling, no divergence
    await _run_reconcile({1: src, 2: rep}, {1})
    assert not rep.reverted                                                     # roc==0 -> not a conflict


async def test_stuck_folder_not_touched_while_resetting(monkeypatch):
    _setup(monkeypatch)
    A.state.resetting = {"/m"}   # an in-flight /reset owns this path
    sts = {1: FakeSt(status=_status(complete=True)),
           2: FakeSt(status=_status(complete=False, need=50, local=0, glob=100))}
    monkeypatch.setattr(A, "client_for", lambda w: sts[w.id])
    await A._reconcile_core({"/m": {1, 2}}, [W(1), W(2)], [F("/m", nodes=[1, 2])])
    assert not sts[1].overrode   # background must not fight the /reset
