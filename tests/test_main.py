import main


async def test_resolve_summaries_keeps_item_on_success(monkeypatch):
    async def fake_summarize(url):
        return f"summary:{url}"
    monkeypatch.setattr(main, "_summarize", fake_summarize)

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
    async def fake_summarize(url):
        return None
    monkeypatch.setattr(main, "_summarize", fake_summarize)
    monkeypatch.setattr(main, "MAX_SUMMARY_ATTEMPTS", 3)

    async def fake_increment(crawler_id, item_id):
        return 1
    monkeypatch.setattr(main.db, "increment_summary_attempts", fake_increment)

    items = [{"id": "v1", "url": "https://youtu.be/v1"}]
    resolved = await main._resolve_summaries(3, items)

    assert resolved == []


async def test_resolve_summaries_gives_up_after_max_attempts(monkeypatch):
    async def fake_summarize(url):
        return None
    monkeypatch.setattr(main, "_summarize", fake_summarize)
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
