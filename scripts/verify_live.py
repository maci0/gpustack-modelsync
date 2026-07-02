"""Live check: two real Syncthing instances, wired by our SyncthingClient.

Validates the REST contract (enforce_local_only, put_device, put_folder,
completion) and that a file actually replicates A->B over a static-IP, no
discovery link. Disk-backed homes under ~/.cache. Run: uv run python scripts/verify_live.py
"""

import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modelsync.syncthing import SyncthingClient  # noqa: E402

ROOT = Path.home() / ".cache" / "modelsync-verify"
KEY = "verifykey-do-not-use-in-prod"
NODES = {
    "a": {"gui": 8388, "data": 22010},
    "b": {"gui": 8389, "data": 22011},
}


def start(name, cfg) -> subprocess.Popen:
    home = ROOT / name
    home.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "STGUIAPIKEY": KEY, "STNODEFAULTFOLDER": "1"}
    return subprocess.Popen(
        [
            "syncthing", "serve", "--no-browser",
            f"--gui-address=127.0.0.1:{cfg['gui']}",
            f"--home={home}",
        ],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


async def wait_ready(http, gui):
    for _ in range(60):
        try:
            r = await http.get(
                f"http://127.0.0.1:{gui}/rest/system/ping",
                headers={"X-API-Key": KEY}, timeout=2,
            )
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.5)
    raise RuntimeError(f"syncthing on {gui} never came up")


async def main() -> int:
    if ROOT.exists():
        shutil.rmtree(ROOT)
    procs = {n: start(n, c) for n, c in NODES.items()}
    ok = False
    try:
        async with httpx.AsyncClient() as http:
            cli = {
                n: SyncthingClient(f"http://127.0.0.1:{c['gui']}", KEY, http)
                for n, c in NODES.items()
            }
            for n, c in NODES.items():
                await wait_ready(http, c["gui"])

            ids = {n: await cli[n].my_id() for n in NODES}
            for n in NODES:
                await cli[n].enforce_local_only()

            # confirm local-only actually took
            r = await http.get(
                f"http://127.0.0.1:{NODES['a']['gui']}/rest/config/options",
                headers={"X-API-Key": KEY},
            )
            assert r.json()["globalAnnounceEnabled"] is False, "local-only failed"
            print("local-only enforced OK")

            # static-IP device wiring, both sides (no discovery)
            await cli["a"].put_device(ids["b"], "b", f"tcp://127.0.0.1:{NODES['b']['data']}")
            await cli["b"].put_device(ids["a"], "a", f"tcp://127.0.0.1:{NODES['a']['data']}")
            # set each node's own data listen address
            for n, c in NODES.items():
                await http.patch(
                    f"http://127.0.0.1:{c['gui']}/rest/config/options",
                    headers={"X-API-Key": KEY},
                    json={"listenAddresses": [f"tcp://127.0.0.1:{c['data']}"]},
                )

            fdir = {n: ROOT / n / "data" for n in NODES}
            fdir["a"].mkdir(parents=True, exist_ok=True)
            (fdir["a"] / "weights.bin").write_bytes(os.urandom(2_000_000))
            fdir["b"].mkdir(parents=True, exist_ok=True)

            await cli["a"].put_folder("model1", str(fdir["a"]), [ids["b"]])
            await cli["b"].put_folder("model1", str(fdir["b"]), [ids["a"]])
            print("device + folder wiring OK; owned:", await cli["a"].owned_folders())

            print("waiting for replication A->B ...")
            for _ in range(60):
                st = await cli["b"].folder_status("model1")
                if (fdir["b"] / "weights.bin").exists() and st["complete"]:
                    print(f"replicated OK, B completion={st['completion']:.0f}%")
                    ok = True
                    break
                await asyncio.sleep(1)
            if not ok:
                st = await cli["b"].folder_status("model1")
                print(f"NOT replicated, B completion={st['completion']:.0f}% state={st['state']}")

            await cli["a"].delete_folder("model1")
            assert "model1" not in await cli["a"].owned_folders()
            assert (fdir["a"] / "weights.bin").exists(), "unshare deleted local files!"
            print("unshare keeps local files OK")
    finally:
        for p in procs.values():
            p.terminate()
        for p in procs.values():
            p.wait()
        shutil.rmtree(ROOT, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
