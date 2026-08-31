import db


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
