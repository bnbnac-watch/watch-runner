import pytest

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


async def test_attach_image_summaries_sets_separate_field_without_touching_summary(monkeypatch):
    async def fake_extract(container, url):
        return ["https://img/1.jpg"]
    monkeypatch.setattr(main, "_extract_images", fake_extract)

    async def fake_build(image_urls):
        return "https://watch-gallery/grid/abc"
    monkeypatch.setattr(main, "_build_image_grid", fake_build)

    items = [{"id": "p1", "url": "https://pf.kakao.com/x/1", "title": "t", "summary": "요약"}]
    await main._attach_image_summaries("crawler-kakao-channels", items)

    assert items[0]["summary"] == "요약"  # summary는 건드리지 않는다
    assert items[0]["image_grid_url"] == "https://watch-gallery/grid/abc"


async def test_attach_image_summaries_skips_field_when_no_images(monkeypatch):
    async def fake_extract(container, url):
        return []
    monkeypatch.setattr(main, "_extract_images", fake_extract)

    items = [{"id": "p1", "url": "https://pf.kakao.com/x/1", "title": "t"}]
    await main._attach_image_summaries("crawler-kakao-channels", items)

    assert "image_grid_url" not in items[0]


async def test_attach_image_summaries_skips_field_when_grid_build_fails(monkeypatch):
    async def fake_extract(container, url):
        return ["https://img/1.jpg"]
    monkeypatch.setattr(main, "_extract_images", fake_extract)

    async def fake_build(image_urls):
        return None
    monkeypatch.setattr(main, "_build_image_grid", fake_build)

    items = [{"id": "p1", "url": "https://pf.kakao.com/x/1", "title": "t"}]
    await main._attach_image_summaries("crawler-kakao-channels", items)

    assert "image_grid_url" not in items[0]


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
        return 0
    monkeypatch.setattr(main.db, "update_success", fake_update_success)

    await main.run_crawler(crawler)

    assert notified == []
    assert marked_seen == []


_CRAWLER = {"id": 4, "container": "crawler-kakao-channels"}


def _stub_failing_crawl(monkeypatch, fail_count):
    async def fake_execute(c):
        raise Exception("render 실패 (500): Page.goto: Timeout 30000ms exceeded.")
    monkeypatch.setattr(main.executor, "execute", fake_execute)

    async def fake_increment(crawler_id, error):
        return fail_count
    monkeypatch.setattr(main.db, "increment_fail_count", fake_increment)

    errors, disabled = [], []

    async def fake_notify_error(crawler_id, error, count, disabled=False):
        errors.append({"crawler_id": crawler_id, "fail_count": count, "disabled": disabled})
    monkeypatch.setattr(main, "_notify_error", fake_notify_error)

    async def fake_disable(crawler_id):
        disabled.append(crawler_id)
    monkeypatch.setattr(main.db, "disable_crawler", fake_disable)

    monkeypatch.setattr(main, "ALERT_FAIL_THRESHOLD", 3)
    monkeypatch.setattr(main, "MAX_FAIL_COUNT", 5)
    return errors, disabled


def _stub_successful_crawl(monkeypatch, previous_fail_count):
    async def fake_execute(c):
        return []
    monkeypatch.setattr(main.executor, "execute", fake_execute)

    async def fake_filter_new(crawler_id, items):
        return []
    monkeypatch.setattr(main.deduplicator, "filter_new", fake_filter_new)

    async def fake_update_success(crawler_id):
        return previous_fail_count
    monkeypatch.setattr(main.db, "update_success", fake_update_success)

    recovered = []

    async def fake_notify_recovered(crawler_id, prev):
        recovered.append((crawler_id, prev))
    monkeypatch.setattr(main, "_notify_recovered", fake_notify_recovered)

    monkeypatch.setattr(main, "ALERT_FAIL_THRESHOLD", 3)
    return recovered


@pytest.mark.parametrize("fail_count", [1, 2])
async def test_run_crawler_stays_silent_below_alert_threshold(monkeypatch, fail_count):
    errors, disabled = _stub_failing_crawl(monkeypatch, fail_count)

    await main.run_crawler(_CRAWLER)

    assert errors == []
    assert disabled == []


async def test_run_crawler_alerts_once_when_alert_threshold_reached(monkeypatch):
    errors, disabled = _stub_failing_crawl(monkeypatch, 3)

    await main.run_crawler(_CRAWLER)

    assert errors == [{"crawler_id": 4, "fail_count": 3, "disabled": False}]
    assert disabled == []


async def test_run_crawler_stays_silent_between_threshold_and_disable(monkeypatch):
    errors, disabled = _stub_failing_crawl(monkeypatch, 4)

    await main.run_crawler(_CRAWLER)

    assert errors == []
    assert disabled == []


async def test_run_crawler_alerts_and_disables_when_max_fail_count_reached(monkeypatch):
    errors, disabled = _stub_failing_crawl(monkeypatch, 5)

    await main.run_crawler(_CRAWLER)

    assert errors == [{"crawler_id": 4, "fail_count": 5, "disabled": True}]
    assert disabled == [4]


async def test_run_crawler_sends_single_alert_when_threshold_equals_max_fail_count(monkeypatch):
    errors, disabled = _stub_failing_crawl(monkeypatch, 5)
    monkeypatch.setattr(main, "ALERT_FAIL_THRESHOLD", 5)

    await main.run_crawler(_CRAWLER)

    assert errors == [{"crawler_id": 4, "fail_count": 5, "disabled": True}]


async def test_run_crawler_still_disables_when_error_alert_delivery_fails(monkeypatch):
    errors, disabled = _stub_failing_crawl(monkeypatch, 5)

    async def failing_notify_error(crawler_id, error, count, disabled=False):
        raise RuntimeError("sender down")
    monkeypatch.setattr(main, "_notify_error", failing_notify_error)

    await main.run_crawler(_CRAWLER)

    assert disabled == [4]


async def test_run_crawler_announces_recovery_after_alerted_failures(monkeypatch):
    recovered = _stub_successful_crawl(monkeypatch, previous_fail_count=3)

    await main.run_crawler(_CRAWLER)

    assert recovered == [(4, 3)]


@pytest.mark.parametrize("previous_fail_count", [0, 1, 2])
async def test_run_crawler_does_not_announce_recovery_below_alert_threshold(monkeypatch, previous_fail_count):
    recovered = _stub_successful_crawl(monkeypatch, previous_fail_count)

    await main.run_crawler(_CRAWLER)

    assert recovered == []


async def test_run_crawler_does_not_count_failed_recovery_alert_as_crawler_failure(monkeypatch):
    _stub_successful_crawl(monkeypatch, previous_fail_count=3)

    async def failing_notify_recovered(crawler_id, prev):
        raise RuntimeError("sender down")
    monkeypatch.setattr(main, "_notify_recovered", failing_notify_recovered)

    increments = []
    async def fake_increment(crawler_id, error):
        increments.append(crawler_id)
        return 1
    monkeypatch.setattr(main.db, "increment_fail_count", fake_increment)

    await main.run_crawler(_CRAWLER)

    assert increments == []


class _RecordingClient:
    def __init__(self):
        self.posts = []

    async def post(self, url, json, timeout):
        self.posts.append((url, json))


async def test_notify_error_posts_disabled_flag_to_sender(monkeypatch):
    client = _RecordingClient()
    monkeypatch.setattr(main, "_http_client", client)

    await main._notify_error(4, "boom", 5, disabled=True)

    assert client.posts == [
        (f"{main.WATCH_SENDER_URL}/error",
         {"crawler_id": 4, "error": "boom", "fail_count": 5, "disabled": True}),
    ]


async def test_notify_recovered_posts_previous_fail_count_to_sender(monkeypatch):
    client = _RecordingClient()
    monkeypatch.setattr(main, "_http_client", client)

    await main._notify_recovered(4, 3)

    assert client.posts == [
        (f"{main.WATCH_SENDER_URL}/recovered", {"crawler_id": 4, "previous_fail_count": 3}),
    ]


class _FakeSummarizeResponse:
    def __init__(self, job_id="job-1"):
        self._job_id = job_id

    def raise_for_status(self):
        pass

    def json(self):
        return {"job_id": self._job_id}


async def test_call_summarize_api_returns_result_on_done(monkeypatch):
    async def fake_post(url, json, timeout):
        return _FakeSummarizeResponse()
    monkeypatch.setattr(main, "_http_client", type("C", (), {"post": staticmethod(fake_post)})())

    async def fake_wait_for_job(job_id, timeout):
        assert job_id == "job-1"
        return {"status": "done", "result": {"result": "요약"}}
    monkeypatch.setattr(main.jobs, "wait_for_job", fake_wait_for_job)

    result = await main._call_summarize_api("https://x")

    assert result == "요약"


async def test_call_summarize_api_raises_permanent_failure_when_not_retryable(monkeypatch):
    async def fake_post(url, json, timeout):
        return _FakeSummarizeResponse()
    monkeypatch.setattr(main, "_http_client", type("C", (), {"post": staticmethod(fake_post)})())

    async def fake_wait_for_job(job_id, timeout):
        return {"status": "failed", "retryable": False, "error": "자막 없음"}
    monkeypatch.setattr(main.jobs, "wait_for_job", fake_wait_for_job)

    with pytest.raises(main._PermanentSummaryFailure):
        await main._call_summarize_api("https://x")


async def test_call_summarize_api_returns_none_when_wait_times_out(monkeypatch):
    async def fake_post(url, json, timeout):
        return _FakeSummarizeResponse()
    monkeypatch.setattr(main, "_http_client", type("C", (), {"post": staticmethod(fake_post)})())

    async def fake_wait_for_job(job_id, timeout):
        return None
    monkeypatch.setattr(main.jobs, "wait_for_job", fake_wait_for_job)

    result = await main._call_summarize_api("https://x")

    assert result is None


async def test_call_summarize_api_returns_none_on_transient_failure(monkeypatch):
    async def fake_post(url, json, timeout):
        return _FakeSummarizeResponse()
    monkeypatch.setattr(main, "_http_client", type("C", (), {"post": staticmethod(fake_post)})())

    async def fake_wait_for_job(job_id, timeout):
        return {"status": "failed", "retryable": True, "error": "요약 시간 초과"}
    monkeypatch.setattr(main.jobs, "wait_for_job", fake_wait_for_job)

    result = await main._call_summarize_api("https://x")

    assert result is None
