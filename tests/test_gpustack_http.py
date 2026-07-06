"""GPUStackClient over real HTTP semantics: a GPUStack-shaped ASGI fake behind
httpx.ASGITransport. Covers what the _list/_get stubs in other tests bypass —
URL construction, auth header, pagination metadata, query params, fetch-mutate-
put round-trips, and the SSE watch stream."""

import json

import httpx
import pytest
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from modelsync.gpustack import GPUStackClient

TOKEN = "gp-tok"


def make_fake():
    """Minimal GPUStack /v2 fake. `db` is mutable so tests can assert writes."""
    db = {
        "workers": [{"id": i, "ip": f"10.0.0.{i}", "name": f"n{i}", "state": "ready",
                     "labels": {"keep": "me"}} for i in (1, 2, 3)],
        "models": {5: {"id": 5, "name": "m", "worker_selector": {"zone": "a"},
                       "replicas": 2}},
        "model_files": {},
        "next_id": 100,
        "registered": [],
    }
    gp = FastAPI()

    def auth(authorization: str | None):
        if authorization != f"Bearer {TOKEN}":
            raise HTTPException(status_code=401, detail="bad token")

    @gp.get("/v2/workers")
    async def workers(page: int = 1, perPage: int = 100,
                      authorization: str | None = Header(None)):
        auth(authorization)
        # deliberately serve ONE item per page regardless of requested perPage
        # (server-side cap) — the client must follow pagination metadata.
        items = db["workers"][page - 1:page]
        return {"items": items,
                "pagination": {"total": len(db["workers"]), "totalPage": len(db["workers"]),
                               "perPage": 1}}

    @gp.get("/v2/models/{mid}")
    async def get_model(mid: int, authorization: str | None = Header(None)):
        auth(authorization)
        if mid not in db["models"]:
            raise HTTPException(status_code=404)
        return db["models"][mid]

    @gp.put("/v2/models/{mid}")
    async def put_model(mid: int, request: Request,
                        authorization: str | None = Header(None)):
        auth(authorization)
        db["models"][mid] = json.loads(await request.body())
        return db["models"][mid]

    @gp.get("/v2/workers/{wid}")
    async def get_worker(wid: int, authorization: str | None = Header(None)):
        auth(authorization)
        w = next((w for w in db["workers"] if w["id"] == wid), None)
        if w is None:
            raise HTTPException(status_code=404)
        return w

    @gp.put("/v2/workers/{wid}")
    async def put_worker(wid: int, request: Request,
                         authorization: str | None = Header(None)):
        auth(authorization)
        body = json.loads(await request.body())
        for i, w in enumerate(db["workers"]):
            if w["id"] == wid:
                db["workers"][i] = body
        return body

    @gp.post("/v2/model-files")
    async def create_mf(request: Request, authorization: str | None = Header(None)):
        auth(authorization)
        body = json.loads(await request.body())
        mid = db["next_id"]
        db["next_id"] += 1
        db["model_files"][mid] = body
        db["registered"].append(body)
        return {"id": mid, **body}

    @gp.get("/v2/model-files/{mid}")
    async def get_mf(mid: int, authorization: str | None = Header(None)):
        auth(authorization)
        if mid not in db["model_files"]:
            raise HTTPException(status_code=404)
        return db["model_files"][mid]

    @gp.delete("/v2/model-files/{mid}")
    async def delete_mf(mid: int, request: Request,
                        authorization: str | None = Header(None)):
        auth(authorization)
        if mid not in db["model_files"]:
            raise HTTPException(status_code=404)
        del db["model_files"][mid]
        db["last_delete_cleanup"] = request.query_params.get("cleanup")
        return {"ok": True}

    @gp.get("/v2/model-instances")
    async def instances(request: Request, authorization: str | None = Header(None)):
        auth(authorization)
        if request.query_params.get("watch") == "true":
            async def sse():
                yield b'data: {"type": "CREATE", "id": 1}\n\n'
                yield b'data: {"type": "UPDATE", "id": 1}\n\n'
            return StreamingResponse(sse(), media_type="text/event-stream")
        return {"items": [], "pagination": {"total": 0, "totalPage": 1, "perPage": 1}}

    return gp, db


@pytest.fixture
def gp_client():
    gp, db = make_fake()
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=gp))
    return GPUStackClient("http://gp", TOKEN, http), db


async def test_pagination_follows_metadata_not_requested_size(gp_client):
    client, _db = gp_client
    ws = await client.workers()
    # server caps perPage to 1; all 3 workers must still arrive
    assert sorted(w.id for w in ws) == [1, 2, 3]


async def test_bad_token_raises_not_empty(gp_client):
    _client, _db = gp_client
    gp, _ = make_fake()
    bad = GPUStackClient("http://gp", "WRONG",
                         httpx.AsyncClient(transport=httpx.ASGITransport(app=gp)))
    with pytest.raises(httpx.HTTPStatusError):
        await bad.workers()  # a 401 must never read as "no workers"


async def test_register_and_delete_roundtrip(gp_client):
    client, db = gp_client
    mid = await client.register_synced(2, {"source": "hf", "huggingface_repo_id": "o/r"})
    assert isinstance(mid, int)
    assert db["registered"] == [{"source": "hf", "huggingface_repo_id": "o/r", "worker_id": 2}]
    await client.delete_model_file(mid, cleanup=True)
    assert db["model_files"] == {} and db["last_delete_cleanup"] == "true"
    await client.delete_model_file(mid)  # already gone: 404 must be tolerated


async def test_get_model_file_404_over_http(gp_client):
    client, _db = gp_client
    assert await client.get_model_file(424242) is None


async def test_set_model_selector_preserves_other_fields(gp_client):
    client, db = gp_client
    await client.set_model_selector(5, {"pin": "true"})
    m = db["models"][5]
    assert m["worker_selector"] == {"pin": "true"}
    assert m["replicas"] == 2 and m["name"] == "m"  # fetch-mutate-put keeps the rest


async def test_set_worker_labels_roundtrip(gp_client):
    client, db = gp_client
    await client.set_worker_labels(1, {"modelsync-x": "true"})
    w = next(w for w in db["workers"] if w["id"] == 1)
    assert w["labels"] == {"modelsync-x": "true"}
    assert w["name"] == "n1"  # other fields preserved


async def test_watch_sse_yields_data_lines(gp_client):
    client, _db = gp_client
    lines = [line async for line in client.watch("model-instances")]
    assert len(lines) == 2
    assert all(line.startswith(b"data:") for line in lines)
    assert b"CREATE" in lines[0].upper()
