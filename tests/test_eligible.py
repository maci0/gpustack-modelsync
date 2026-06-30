"""Eligibility gating: don't push a model to a draining node or one without
disk, but never reject a node that already holds it."""

from modelsync.app import _eligible
from modelsync.gpustack import ModelFolder, Worker, free_for_path

GIB = 2**30


def w(id, *, state="ready", unreachable=False, maintenance=False, free=None):
    return Worker(
        id=id, name=f"n{id}", ip=f"10.0.0.{id}", cluster_id=1,
        state=state, unreachable=unreachable, maintenance=maintenance, free_bytes=free,
    )


def folder(path, size, have):
    return ModelFolder(path=path, label=path, size=size, current_nodes=have)


def test_state_and_capacity_gating():
    P = "/cache/m"
    workers = [
        w(1, free=50 * GIB),                 # ready, fits
        w(2, free=1 * GIB),                  # ready, too small
        w(3, state="not_ready", free=99 * GIB),  # draining
        w(4, maintenance=True, free=99 * GIB),    # maintenance
    ]
    folders = [folder(P, 10 * GIB, have=[])]
    plan, warn = _eligible({P: {1, 2, 3, 4}}, workers, folders)

    assert plan == {P: {1}}
    assert sum(1 for x in warn if "skipped" in x) == 3
    assert any("free" in x for x in warn)            # disk skip
    assert any("not ready" in x for x in warn)        # state skip


def test_already_present_never_rejected():
    P = "/cache/m"
    # node 2 has tiny disk AND is in maintenance, but already holds the model
    workers = [w(2, maintenance=True, free=1 * GIB)]
    folders = [folder(P, 10 * GIB, have=[2])]
    plan, warn = _eligible({P: {2}}, workers, folders)
    assert plan == {P: {2}}
    assert warn == []


def test_free_for_path_longest_prefix_mount():
    w = Worker(id=1, name="n", ip="10.0.0.1", free_bytes=5 * GIB, mounts=[
        {"mount_point": "/", "free": 5 * GIB},
        {"mount_point": "/var/lib/gpustack", "free": 900 * GIB},
    ])
    # the cache mount (longest prefix), not the root mount or the max
    assert free_for_path(w, "/var/lib/gpustack/cache/m") == 900 * GIB
    assert free_for_path(w, "/other") == 5 * GIB           # only root matches
    # no mounts -> fall back to free_bytes
    assert free_for_path(Worker(id=2, name="n", ip="10.0.0.2", free_bytes=7 * GIB), "/x") == 7 * GIB


def test_unknown_free_is_allowed():
    P = "/cache/m"
    workers = [w(1, free=None)]  # disk unknown -> don't block
    folders = [folder(P, 10 * GIB, have=[1])]  # node already holds it = the source
    plan, warn = _eligible({P: {1}}, workers, folders)
    assert plan == {P: {1}}
    assert warn == []  # unknown disk doesn't block, and node 1 is a valid source
