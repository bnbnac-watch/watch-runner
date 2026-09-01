import asyncio
import uuid

import pytest

import jobs


@pytest.fixture(autouse=True)
def _clear_pending():
    jobs._pending.clear()
    yield
    jobs._pending.clear()


async def test_wait_for_job_returns_row_immediately_if_already_done(monkeypatch):
    job_id = str(uuid.uuid4())

    async def fake_get_job(jid):
        return {"status": "done", "result": {"result": "요약"}}
    monkeypatch.setattr(jobs.db, "get_job", fake_get_job)

    row = await jobs.wait_for_job(job_id, timeout=1)

    assert row["status"] == "done"
    assert job_id not in jobs._pending


async def test_wait_for_job_resolves_when_notified(monkeypatch):
    job_id = str(uuid.uuid4())

    async def fake_get_job(jid):
        return None
    monkeypatch.setattr(jobs.db, "get_job", fake_get_job)

    async def notify_later():
        await asyncio.sleep(0.01)
        jobs._resolve(job_id, {"status": "done", "result": {"result": "요약"}})
    asyncio.create_task(notify_later())

    row = await jobs.wait_for_job(job_id, timeout=1)

    assert row["status"] == "done"


async def test_wait_for_job_times_out_and_cleans_up_pending(monkeypatch):
    job_id = str(uuid.uuid4())

    async def fake_get_job(jid):
        return None
    monkeypatch.setattr(jobs.db, "get_job", fake_get_job)

    row = await jobs.wait_for_job(job_id, timeout=0.05)

    assert row is None
    assert job_id not in jobs._pending


async def test_on_pg_notify_resolves_matching_future(monkeypatch):
    job_id = str(uuid.uuid4())
    fut = asyncio.get_event_loop().create_future()
    jobs._pending[job_id] = fut

    async def fake_get_job(jid):
        return {"status": "done", "result": {"result": "요약"}}
    monkeypatch.setattr(jobs.db, "get_job", fake_get_job)

    await jobs._on_pg_notify(None, None, "async_job_done", job_id)

    assert fut.done()
    assert fut.result()["status"] == "done"


async def test_on_pg_notify_ignores_unknown_job_id(monkeypatch):
    async def fake_get_job(jid):
        return {"status": "done"}
    monkeypatch.setattr(jobs.db, "get_job", fake_get_job)

    await jobs._on_pg_notify(None, None, "async_job_done", str(uuid.uuid4()))
    # 매칭되는 future가 없어도 예외 없이 조용히 지나가면 통과


async def test_fallback_poll_resolves_completed_pending_jobs(monkeypatch):
    job_id = str(uuid.uuid4())
    fut = asyncio.get_event_loop().create_future()
    jobs._pending[job_id] = fut

    async def fake_get_job(jid):
        return {"status": "done", "result": {"result": "요약"}}
    monkeypatch.setattr(jobs.db, "get_job", fake_get_job)
    monkeypatch.setattr(jobs, "FALLBACK_POLL_INTERVAL_S", 0.01)

    poll_task = asyncio.create_task(jobs._fallback_poll_loop())
    await asyncio.sleep(0.03)
    poll_task.cancel()

    assert fut.done()
