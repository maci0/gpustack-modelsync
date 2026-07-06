"""Redirect the app's state files (plan/registry/members/pins) into a per-test
tmp dir, so tests never litter the repo root with runtime state."""

import pytest

import modelsync.app as A


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    for name in ("PLAN_FILE", "REGISTRY_FILE", "MEMBERS_FILE", "PINS_FILE"):
        monkeypatch.setattr(A, name, tmp_path / getattr(A, name).name)
