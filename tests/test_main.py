import main


async def test_resolve_summaries_keeps_item_on_success(monkeypatch):
    async def fake_resolve(url):
        return f"summary:{url}", False
    monkeypatch.setattr(main, "_resolve_item_summary", fake_resolve)

    cleared = []
    async def fake_clear(crawler_id, item_id):
        cleared.append((crawler_id, item_id))
    monkeypatch.setattr(main.db, "clear_summary_attempts", fake_clear)

    items = [{"id": "v1", "url": "https://youtu.be/v1"}]
    resolved = await main._resolve_summaries(3, items)

    assert len(resolved) == 1
    assert resolved[0]["summary"] == "summary:https://youtu.be/v1"
    assert cleared == [(3, "v1")]


async def test_resolve_summaries_holds_back_item_under_attempt_cap(monkeypatch):
    async def fake_resolve(url):
        return None, False
    monkeypatch.setattr(main, "_resolve_item_summary", fake_resolve)
    monkeypatch.setattr(main, "MAX_SUMMARY_ATTEMPTS", 3)

    async def fake_increment(crawler_id, item_id):
        return 1
    monkeypatch.setattr(main.db, "increment_summary_attempts", fake_increment)

    items = [{"id": "v1", "url": "https://youtu.be/v1"}]
    resolved = await main._resolve_summaries(3, items)

    assert resolved == []


async def test_resolve_summaries_gives_up_after_max_attempts(monkeypatch):
    async def fake_resolve(url):
        return None, False
    monkeypatch.setattr(main, "_resolve_item_summary", fake_resolve)
    monkeypatch.setattr(main, "MAX_SUMMARY_ATTEMPTS", 3)

    async def fake_increment(crawler_id, item_id):
        return 3
    monkeypatch.setattr(main.db, "increment_summary_attempts", fake_increment)

    cleared = []
    async def fake_clear(crawler_id, item_id):
        cleared.append((crawler_id, item_id))
    monkeypatch.setattr(main.db, "clear_summary_attempts", fake_clear)

    items = [{"id": "v1", "url": "https://youtu.be/v1"}]
    resolved = await main._resolve_summaries(3, items)

    assert len(resolved) == 1
    assert resolved[0]["summary"] is None
    assert cleared == [(3, "v1")]


async def test_resolve_summaries_gives_up_immediately_on_permanent_failure(monkeypatch):
    async def fake_resolve(url):
        return None, True  # 자막 없음 등 영구 실패
    monkeypatch.setattr(main, "_resolve_item_summary", fake_resolve)

    increment_calls = []
    async def fake_increment(crawler_id, item_id):
        increment_calls.append((crawler_id, item_id))
        return 1
    monkeypatch.setattr(main.db, "increment_summary_attempts", fake_increment)

    cleared = []
    async def fake_clear(crawler_id, item_id):
        cleared.append((crawler_id, item_id))
    monkeypatch.setattr(main.db, "clear_summary_attempts", fake_clear)

    items = [{"id": "v1", "url": "https://youtu.be/v1"}]
    resolved = await main._resolve_summaries(3, items)

    assert len(resolved) == 1
    assert resolved[0]["summary"] is None
    assert increment_calls == []  # 영구 실패는 attempts 카운트를 건드리지 않는다
    assert cleared == [(3, "v1")]


async def test_resolve_summaries_mixed_batch_routes_each_item_correctly(monkeypatch):
    async def fake_resolve(url):
        if url == "https://youtu.be/ok":
            return "summary:ok", False
        if url == "https://youtu.be/hold":
            return None, False
        if url == "https://youtu.be/giveup":
            return None, False
        raise AssertionError(f"unexpected url {url}")
    monkeypatch.setattr(main, "_resolve_item_summary", fake_resolve)
    monkeypatch.setattr(main, "MAX_SUMMARY_ATTEMPTS", 3)

    async def fake_increment(crawler_id, item_id):
        return {"hold": 1, "giveup": 3}[item_id]
    monkeypatch.setattr(main.db, "increment_summary_attempts", fake_increment)

    cleared = []
    async def fake_clear(crawler_id, item_id):
        cleared.append(item_id)
    monkeypatch.setattr(main.db, "clear_summary_attempts", fake_clear)

    items = [
        {"id": "ok", "url": "https://youtu.be/ok"},
        {"id": "hold", "url": "https://youtu.be/hold"},
        {"id": "giveup", "url": "https://youtu.be/giveup"},
    ]
    resolved = await main._resolve_summaries(9, items)

    resolved_by_id = {item["id"]: item for item in resolved}
    assert set(resolved_by_id) == {"ok", "giveup"}
    assert resolved_by_id["ok"]["summary"] == "summary:ok"
    assert resolved_by_id["giveup"]["summary"] is None
    assert cleared == ["ok", "giveup"]


async def test_summarize_returns_none_on_permanent_failure(monkeypatch):
    async def fake_call(url):
        raise main._PermanentSummaryFailure("404 자막 없음")
    monkeypatch.setattr(main, "_call_summarize_api", fake_call)

    result = await main._summarize("https://youtu.be/v1")

    assert result is None


async def test_run_crawler_does_not_notify_or_mark_seen_when_summary_held_back(monkeypatch):
    crawler = {"id": 7, "container": "crawler-yt-channels", "post_process": {"type": "summarize"}}

    async def fake_execute(c):
        return [{"id": "v1", "url": "https://youtu.be/v1", "title": "t"}]
    monkeypatch.setattr(main.executor, "execute", fake_execute)

    async def fake_filter_new(crawler_id, items):
        return items
    monkeypatch.setattr(main.deduplicator, "filter_new", fake_filter_new)

    async def fake_resolve_item_summary(url):
        return None, False  # 일시적 실패 → 보류
    monkeypatch.setattr(main, "_resolve_item_summary", fake_resolve_item_summary)

    async def fake_increment(crawler_id, item_id):
        return 1  # 첫 실패, cap(3) 미도달
    monkeypatch.setattr(main.db, "increment_summary_attempts", fake_increment)

    notified = []
    async def fake_notify(crawler_id, items):
        notified.append(items)
    monkeypatch.setattr(main, "_notify_items", fake_notify)

    marked_seen = []
    async def fake_mark_seen(crawler_id, item_ids):
        marked_seen.append(item_ids)
    monkeypatch.setattr(main.deduplicator, "mark_seen", fake_mark_seen)

    async def fake_update_success(crawler_id):
        pass
    monkeypatch.setattr(main.db, "update_success", fake_update_success)

    await main.run_crawler(crawler)

    assert notified == []
    assert marked_seen == []
