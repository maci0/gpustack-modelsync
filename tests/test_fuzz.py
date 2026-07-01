"""Property-based fuzzing of every parser on the untrusted trust boundaries:
GPUStack JSON, Syncthing JSON, tampered state files, and path strings. Contract:
a parser must NEVER raise on arbitrary input — it returns a valid-typed default.
"""


from hypothesis import given, settings
from hypothesis import strategies as st

import modelsync.app as A
from modelsync.gpustack import (
    _as_dir,
    _gpu_summary,
    _instance_dir,
    _maintenance_on,
    _max_free,
    _model_dir,
    _mounts,
    _under_roots,
    free_for_path,
)
from modelsync.reconcile import folder_id
from modelsync.gpustack import Worker

# Arbitrary JSON-ish values: the shape a hostile/buggy API or corrupt file yields.
json_val = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=True)
    | st.text() | st.binary().map(lambda b: b.decode("latin1")),
    lambda c: st.lists(c, max_size=5) | st.dictionaries(st.text(max_size=8), c, max_size=5),
    max_leaves=20,
)
json_dict = st.dictionaries(st.text(max_size=8), json_val, max_size=8)

# Targeted: the REAL GPUStack key structure, but every leaf value fuzzed. This
# reaches the int()/float() coercion that random-key dicts never would.
gpu_status = st.fixed_dictionaries({
    "gpu_devices": st.lists(st.dictionaries(
        st.sampled_from(["name", "memory", "core", "total", "used", "utilization_rate"]),
        json_val, max_size=4) | json_val, max_size=3) | json_val,
    "filesystem": st.lists(st.dictionaries(
        st.sampled_from(["mount_point", "mountPoint", "free", "available"]),
        json_val, max_size=4) | json_val, max_size=3) | json_val,
})
mf_dict = st.dictionaries(
    st.sampled_from(["local_dir", "local_path", "resolved_paths"]), json_val, max_size=3)


@settings(max_examples=400)
@given(gpu_status | json_dict)
def test_gpu_summary_never_crashes(d):
    out = _gpu_summary(d)
    assert isinstance(out["vram_total"], int) and isinstance(out["vram_used"], int)
    assert out["gpu_util"] is None or isinstance(out["gpu_util"], float)


@settings(max_examples=400)
@given(gpu_status | json_dict)
def test_mounts_and_maxfree_never_crash(d):
    ms = _mounts(d)
    assert isinstance(ms, list)
    assert all(isinstance(m["free"], int) and isinstance(m["mount_point"], str) for m in ms)
    mf = _max_free(d)
    assert mf is None or isinstance(mf, int)


@settings(max_examples=400)
@given(mf_dict | json_dict)
def test_model_and_instance_dir_never_crash(d):
    for f in (_model_dir, _instance_dir):
        r = f(d)
        assert r is None or isinstance(r, str)


@settings(max_examples=300)
@given(json_val)
def test_maintenance_on_never_crashes(v):
    assert isinstance(_maintenance_on(v), bool)


@settings(max_examples=300)
@given(st.text())
def test_folder_id_always_valid(s):
    fid = folder_id(s)
    assert fid and all(c.isalnum() or c in "._-" for c in fid)  # syncthing-safe id


@settings(max_examples=300)
@given(st.text(), st.lists(st.text(), max_size=4))
def test_under_roots_never_crashes(path, roots):
    assert isinstance(_under_roots(path, roots), bool)


@settings(max_examples=300)
@given(st.text())
def test_as_dir_never_crashes(p):
    assert isinstance(_as_dir(p), str)


mount_strat = st.lists(
    st.fixed_dictionaries({"mount_point": st.text(max_size=10),
                           "free": st.integers(min_value=0, max_value=10**15)}),
    max_size=4)


@settings(max_examples=200)
@given(mount_strat, st.text())
def test_free_for_path_never_crashes(mounts, path):
    w = Worker(id=1, name="n", ip="10.0.0.1", state="ready", mounts=mounts, free_bytes=0)
    r = free_for_path(w, path)
    assert r is None or isinstance(r, int)


@settings(max_examples=300)
@given(json_val)
def test_plan_coercion_never_crashes(v):
    # the type-coercion core the state loaders rely on (set(v) used to crash)
    assert isinstance(A._int_ids(v), set)


@settings(max_examples=200)
@given(st.dictionaries(st.text(max_size=8), json_val, max_size=6))
def test_prune_plan_never_crashes(plan):
    typed = {k: A._int_ids(v) for k, v in plan.items()}
    assert isinstance(A._prune_plan(typed, set()), dict)


@settings(max_examples=300)
@given(json_dict)
def test_folder_status_parse_never_crashes(d):
    from modelsync.syncthing import parse_folder_status
    out = parse_folder_status(d)  # coerces raw Syncthing db/status JSON
    assert isinstance(out["global_bytes"], int) and out["completion"] >= 0.0
