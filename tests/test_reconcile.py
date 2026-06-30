"""Reconcile is the non-trivial logic: source choice, revert-safety, GC."""

from modelsync.gpustack import Worker
from modelsync.reconcile import folder_id, pick_source, reconcile


class FakeSync:
    """Records calls the reconciler makes to one node. `compl`/`diverged` drive
    folder_status so we can exercise source-choice and revert-safety."""

    def __init__(self, my, preset=(), compl=100.0, diverged=0, fail_status=False):
        self.my = my
        self.compl = compl
        self.diverged = diverged
        self.fail_status = fail_status
        self.local_only = 0
        self.devices: list[str] = []
        self.folders: dict[str, list[str]] = {f: [] for f in preset}
        self.types: dict[str, str] = {}
        self.deleted: list[str] = []
        self.reverted: list[str] = []

    async def enforce_local_only(self):
        self.local_only += 1

    async def my_id(self):
        return self.my

    async def put_device(self, device_id, name, address):
        self.devices.append(device_id)
        assert address.startswith("tcp://") and address.endswith(":22000")

    async def put_folder(self, fid, path, peer_ids, folder_type="sendreceive"):
        self.folders[fid] = peer_ids
        self.types[fid] = folder_type

    async def delete_folder(self, fid):
        self.deleted.append(fid)
        self.folders.pop(fid, None)

    async def owned_folders(self):
        return set(self.folders)

    async def folder_status(self, fid):
        if self.fail_status:
            import httpx
            raise httpx.HTTPError("boom")
        g = 1 if self.compl > 0 else 0
        return {"completion": self.compl, "complete": self.compl >= 100,
                "state": "idle", "need_bytes": 0, "global_bytes": g,
                "errors": 0, "receive_only_changed": self.diverged}

    async def revert(self, fid):
        self.reverted.append(fid)


def make(fakes):
    return lambda w: fakes[w.id]


def W(i):
    return Worker(id=i, name=f"n{i}", ip=f"10.0.0.{i}", cluster_id=1)


async def test_meshes_source_sendonly_replica_receiveonly_and_gc():
    workers = [W(1), W(2), W(3)]
    path = "/var/lib/gpustack/cache/huggingface/Qwen/Qwen2.5-7B"
    fid = folder_id(path)
    fakes = {1: FakeSync("DEV1"), 2: FakeSync("DEV2"), 3: FakeSync("DEV3", preset=[fid])}
    await reconcile({path: {1, 2}}, workers, make(fakes), have={path: {1}})

    assert all(f.local_only == 1 for f in fakes.values())
    assert fakes[1].types[fid] == "sendonly"      # confirmed holder = source
    assert fakes[2].types[fid] == "receiveonly"
    assert fakes[1].folders[fid] == ["DEV2"]
    assert fakes[2].folders[fid] == ["DEV1"]
    assert fakes[3].deleted == [fid]              # orphan we own -> GC'd
    assert fid not in fakes[3].folders


async def test_no_source_skips_path():
    # neither target is a confirmed holder and neither has data (compl 0)
    workers = [W(1), W(2)]
    fakes = {1: FakeSync("DEV1", compl=0), 2: FakeSync("DEV2", compl=0)}
    unreachable, warnings = await reconcile({"/c/m": {1, 2}}, workers, make(fakes), have={})
    fid = folder_id("/c/m")
    assert fid not in fakes[1].folders and fid not in fakes[2].folders  # not wired
    assert any("no source" in w for w in warnings)


async def test_diverged_replica_reverted_only_when_not_more_complete():
    path = "/c/m"
    fid = folder_id(path)
    workers = [W(1), W(2)]
    # source (1) fully complete; replica (2) diverged but LESS complete -> revert
    fakes = {1: FakeSync("DEV1", compl=100), 2: FakeSync("DEV2", compl=40, diverged=999)}
    await reconcile({path: {1, 2}}, workers, make(fakes), have={path: {1}})
    assert fakes[2].reverted == [fid]
    assert fakes[1].reverted == []

    # replica MORE complete than source -> never reverted (would wipe the better copy)
    fakes = {1: FakeSync("DEV1", compl=30), 2: FakeSync("DEV2", compl=90, diverged=999)}
    await reconcile({path: {1, 2}}, workers, make(fakes), have={path: {1}})
    assert fakes[2].reverted == []


async def test_complete_replica_never_reverted():
    # both nodes already hold the model (replica complete); a divergence must NOT
    # trigger a revert — that would destroy a good independently-present copy.
    path = "/c/m"
    workers = [W(1), W(2)]
    fakes = {1: FakeSync("DEV1", compl=100), 2: FakeSync("DEV2", compl=100, diverged=999)}
    await reconcile({path: {1, 2}}, workers, make(fakes), have={path: {1, 2}})
    assert fakes[2].reverted == []


async def test_unconfirmed_source_never_reverts():
    # no GPUStack-confirmed holder -> source is completion-inferred -> we must NOT
    # revert replicas toward it (could wipe a real copy toward a guess).
    path = "/c/m"
    fid = folder_id(path)
    workers = [W(1), W(2)]
    fakes = {1: FakeSync("DEV1", compl=100), 2: FakeSync("DEV2", compl=50, diverged=999)}
    await reconcile({path: {1, 2}}, workers, make(fakes), have={})  # no confirmed holder
    assert fakes[2].reverted == []


async def test_unknown_source_completion_never_reverts():
    # source is confirmed but its folder_status is unreadable -> src_compl=0 ->
    # the <= guard can never fire -> no replica is reverted.
    path = "/c/m"
    workers = [W(1), W(2)]
    fakes = {1: FakeSync("DEV1", fail_status=True), 2: FakeSync("DEV2", compl=50, diverged=999)}
    await reconcile({path: {1, 2}}, workers, make(fakes), have={path: {1}})
    assert fakes[2].reverted == []


async def test_folder_id_collision_resistant():
    assert folder_id("/models/a/b") != folder_id("/models/a-b")
    assert folder_id("/x/Model.v2-Q4") == folder_id("x/Model.v2-Q4/")  # trims edges


def test_pick_source_holder_only():
    assert pick_source({1, 2, 3}, {2, 3}) == 2   # lowest confirmed holder
    assert pick_source({1, 2}, set()) is None     # no holder -> None (never empty node)
    assert pick_source(set(), {1}) is None
