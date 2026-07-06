"""Property-based invariants for choose_source — the safety-critical source pick
(a wrong choice propagates a bad copy). Complements the example-based tests."""

import itertools

from hypothesis import given, settings
from hypothesis import strategies as st

from modelsync.reconcile import _is_clean, choose_source

_status_entry = st.one_of(
    st.none(),
    st.fixed_dictionaries({
        "complete": st.booleans(),
        "state": st.sampled_from(["idle", "syncing", "scanning", "error"]),
        "errors": st.integers(0, 3),
        "receive_only_changed": st.integers(0, 200),
        "global_bytes": st.integers(0, 1000),
        "local_bytes": st.integers(0, 1000),
        # discrete so equal-completion TIES are common: that's the only case where
        # the completion-fallback could pick order-dependently (determinism teeth).
        "completion": st.sampled_from([0.0, 25.0, 50.0, 100.0]),
    }),
)


@settings(max_examples=400)
@given(
    targets=st.lists(st.integers(1, 6), unique=True, max_size=5),
    have=st.sets(st.integers(1, 6), max_size=6),
    status=st.dictionaries(st.integers(1, 6), _status_entry, max_size=6),
)
def test_choose_source_invariants(targets, have, status):
    r = choose_source(targets, have, status)
    # 1. never invents a node
    assert r is None or r in targets
    # 2. if ANY clean copy exists among targets, the pick MUST be clean (integrity
    #    first: never propagate a dirty/poisoned copy when a verified one exists)
    if any(_is_clean(status.get(t)) for t in targets):
        assert r is not None and _is_clean(status.get(r))
    # 3. fully order-independent: permuting targets never changes the pick
    #    (guards against source flapping across reconcile passes)
    for perm in itertools.permutations(targets):
        assert choose_source(list(perm), have, status) == r


def test_choose_source_fallback_tie_is_order_independent():
    # No confirmed holder, none clean, two nodes tied at max completion: the pick
    # must be deterministic (lowest id), independent of the target list order —
    # else the source flaps between reconcile passes as GPUStack reorders workers.
    dirty = {"complete": False, "state": "syncing", "errors": 0,
             "receive_only_changed": 0, "global_bytes": 100, "local_bytes": 50,
             "completion": 50.0}
    status = {2: dict(dirty), 3: dict(dirty)}
    assert choose_source([2, 3], set(), status) == 2
    assert choose_source([3, 2], set(), status) == 2   # reversed input -> same pick
