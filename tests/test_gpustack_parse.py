"""Parsers over untrusted GPUStack JSON (trust boundary): must never crash on
missing/None/wrong-typed fields, and must extract the right values."""

from modelsync.gpustack import (
    GPUStackClient,
    _gpu_summary,
    _maintenance_on,
    _max_free,
    _mounts,
)


def test_gpu_summary_robust():
    assert _gpu_summary(None) == {"gpu_name": None, "vram_total": 0, "vram_used": 0, "gpu_util": None}
    assert _gpu_summary({}) == {"gpu_name": None, "vram_total": 0, "vram_used": 0, "gpu_util": None}
    assert _gpu_summary({"gpu_devices": None})["vram_total"] == 0
    assert _gpu_summary({"gpu_devices": ["notadict"]})["vram_total"] == 0
    # missing memory/core sub-objects must not crash
    assert _gpu_summary({"gpu_devices": [{"name": "G"}]}) == {
        "gpu_name": "G", "vram_total": 0, "vram_used": 0, "gpu_util": None}
    # two devices sum VRAM, average util
    s = _gpu_summary({"gpu_devices": [
        {"name": "A", "memory": {"total": 100, "used": 40}, "core": {"utilization_rate": 20}},
        {"name": "B", "memory": {"total": 100, "used": 60}, "core": {"utilization_rate": 40}},
    ]})
    assert s["vram_total"] == 200 and s["vram_used"] == 100 and s["gpu_util"] == 30.0


def test_mounts_and_max_free_robust():
    assert _mounts(None) == [] and _max_free(None) is None
    assert _mounts({"filesystem": "notalist"}) == []
    fs = {"filesystem": [
        {"mount_point": "/", "free": 10},
        {"mount_point": "/data", "available": 900},   # free missing -> use available
        {"mount_point": "/x"},                          # no free/available -> dropped
        "notadict",
    ]}
    assert _mounts(fs) == [{"mount_point": "/", "free": 10}, {"mount_point": "/data", "free": 900}]
    assert _max_free(fs) == 900


def test_maintenance_on():
    assert _maintenance_on({"enabled": True}) is True
    assert _maintenance_on({"enabled": False}) is False
    assert _maintenance_on(None) is False
    assert _maintenance_on(True) is True


async def test_workers_parse_and_ip_filter():
    gp = GPUStackClient("http://x", "t", None, allowed_cidrs=["10.0.0.0/8"])

    async def fake_list(path, **k):
        return [
            {"id": 1, "ip": "10.0.0.5", "name": "a", "state": "ready",
             "maintenance": {"enabled": False}, "unreachable": False,
             "worker_version": "v2.2.0",
             "status": {"gpu_devices": [{"name": "GB10", "memory": {"total": 137, "used": 4},
                                         "core": {"utilization_rate": 0}}],
                        "filesystem": [{"mount_point": "/var/lib/gpustack", "free": 3000}]}},
            {"id": 2, "ip": "8.8.8.8", "name": "public"},   # public IP -> SSRF filter drops
            {"id": 3, "name": "noip"},                       # no ip -> skipped
            {"ip": "10.0.0.9", "name": "noid"},              # no id -> skipped
        ]

    gp._list = fake_list
    ws = await gp.workers()
    assert [w.id for w in ws] == [1]                         # only the valid private-IP worker
    w = ws[0]
    assert w.gpu_name == "GB10" and w.vram_total == 137 and w.vram_used == 4
    assert w.free_bytes == 3000 and w.mounts == [{"mount_point": "/var/lib/gpustack", "free": 3000}]
    assert w.worker_version == "v2.2.0" and w.syncable
