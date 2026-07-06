"""The three daemon loops — the last untested logic: wake conditions, event
cursor advance, ReadTimeout fast-reconnect, and reconcile-loop crash immunity."""

import asyncio
import contextlib

import httpx

import modelsync.app as A
from modelsync.gpustack import Worker


async def _cancel(t):
    t.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await t


class GPWatch:
    """Fake GPUStack watch: optionally fail once, then yield lines, then park."""

    def __init__(self, lines, fail_first=None):
        self.lines = lines
        self.fail_first = fail_first
        self.calls = 0

    async def watch(self, resource):
        self.calls += 1
        if self.calls == 1 and self.fail_first is not None:
            raise self.fail_first
        for line in self.lines:
            yield line
        await asyncio.Event().wait()


async def test_watch_loop_wakes_on_create_not_on_update():
    A.state.wake = asyncio.Event()
    A.state.gpustack = GPWatch([b'data: {"type": "UPDATE", "id": 3}'])
    t = asyncio.create_task(A._gpustack_watch_loop("workers"))
    await asyncio.sleep(0.05)
    assert not A.state.wake.is_set()          # status heartbeat: no wake
    await _cancel(t)

    A.state.gpustack = GPWatch([b'data: {"type": "create", "id": 3}'])  # any case
    t = asyncio.create_task(A._gpustack_watch_loop("workers"))
    await asyncio.sleep(0.05)
    assert A.state.wake.is_set()              # create wakes the reconcile loop
    await _cancel(t)


async def test_watch_loop_read_timeout_reconnects_without_backoff():
    A.state.wake = asyncio.Event()
    gp = GPWatch([b'data: {"type": "DELETE"}'], fail_first=httpx.ReadTimeout("idle"))
    A.state.gpustack = gp
    t = asyncio.create_task(A._gpustack_watch_loop("workers"))
    await asyncio.sleep(0.1)                  # far below the 5s error backoff
    assert gp.calls == 2 and A.state.wake.is_set()  # timed out once, reconnected NOW
    await _cancel(t)


async def test_syncthing_event_loop_cursor_and_wake(monkeypatch):
    A.state.wake = asyncio.Event()
    seen: list[int] = []

    class St:
        async def events(self, since, kinds):
            seen.append(since)
            if len(seen) == 1:   # partial completion: must NOT wake
                return [{"id": 3, "type": "FolderCompletion", "data": {"completion": 50}}]
            if len(seen) == 2:   # finished folder: must wake
                return [{"id": 7, "type": "FolderCompletion", "data": {"completion": 100}}]
            await asyncio.Event().wait()
            return []  # unreachable: parked forever above

    monkeypatch.setattr(A, "client_for", lambda w: St())
    t = asyncio.create_task(
        A._syncthing_event_loop(Worker(id=1, name="n", ip="10.0.0.1", state="ready")))
    await asyncio.sleep(0.05)
    assert A.state.wake.is_set()
    assert seen[:3] == [0, 3, 7]   # cursor advanced past BOTH events (no dead-loop)
    await _cancel(t)


async def test_background_loop_survives_reconcile_errors(monkeypatch):
    calls: list[int] = []

    async def boom():
        calls.append(1)
        A.state.wake.set()        # keep the loop hot so the test stays fast
        raise RuntimeError("kaboom")

    monkeypatch.setattr(A, "reconcile_all", boom)
    monkeypatch.setattr(A.settings, "reconcile_interval", 999)
    A.state.wake = asyncio.Event()
    A.state.wake.set()
    t = asyncio.create_task(A._background_loop())
    await asyncio.sleep(2.4)      # two cycles (1s debounce each)
    await _cancel(t)
    assert len(calls) >= 2        # an exception never kills the loop
