import uuid

import db


async def test_get_job_returns_row_as_dict(fake_pool, fake_conn, monkeypatch):
    monkeypatch.setattr(db, "_pool", fake_pool)
    job_id = uuid.uuid4()
    fake_conn.fetchrow_return = {
        "id": job_id, "status": "done", "result": {"result": "요약"},
        "error": None, "retryable": True,
    }

    row = await db.get_job(job_id)

    assert row["status"] == "done"
    assert row["result"] == {"result": "요약"}


async def test_get_job_returns_none_when_missing(fake_pool, fake_conn, monkeypatch):
    monkeypatch.setattr(db, "_pool", fake_pool)

    row = await db.get_job(uuid.uuid4())

    assert row is None


async def test_increment_fail_count_writes_error_message(fake_pool, fake_conn, monkeypatch):
    fake_conn.fetchrow_return = {"fail_count": 3}
    monkeypatch.setattr(db, "_pool", fake_pool)

    result = await db.increment_fail_count(7, "TimeoutError: goto timeout")

    assert result == 3
    query, args = fake_conn.fetchrow_calls[0]
    assert "last_error" in query
    assert args == (7, "TimeoutError: goto timeout")


async def test_update_success_clears_last_error(fake_pool, fake_conn, monkeypatch):
    monkeypatch.setattr(db, "_pool", fake_pool)

    await db.update_success(4)

    query, args = fake_conn.execute_calls[0]
    assert "last_error = NULL" in query
    assert args == (4,)


async def test_increment_summary_attempts_returns_new_count(fake_pool, fake_conn, monkeypatch):
    fake_conn.fetchrow_return = {"attempts": 2}
    monkeypatch.setattr(db, "_pool", fake_pool)

    result = await db.increment_summary_attempts(9, "vid-1")

    assert result == 2
    query, args = fake_conn.fetchrow_calls[0]
    assert "ON CONFLICT" in query
    assert "pending_summaries" in query
    assert args == (9, "vid-1")


async def test_clear_summary_attempts_deletes_row(fake_pool, fake_conn, monkeypatch):
    monkeypatch.setattr(db, "_pool", fake_pool)

    await db.clear_summary_attempts(9, "vid-1")

    query, args = fake_conn.execute_calls[0]
    assert "DELETE FROM pending_summaries" in query
    assert args == (9, "vid-1")
