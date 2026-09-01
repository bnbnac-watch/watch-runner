import asyncio
import time

import executor


class _FakeResponse:
    status_code = 200

    def json(self):
        return []


class _FakeClient:
    def __init__(self, delay=0.05):
        self.delay = delay
        self.max_concurrent = 0
        self._current = 0

    async def post(self, url, json, timeout):
        self._current += 1
        self.max_concurrent = max(self.max_concurrent, self._current)
        try:
            await asyncio.sleep(self.delay)
            return _FakeResponse()
        finally:
            self._current -= 1


async def test_execute_runs_concurrently_across_different_crawlers():
    client = _FakeClient(delay=0.05)
    executor.set_client(client)
    crawlers = [{"container": f"crawler-{i}", "params": {}} for i in range(4)]

    start = time.monotonic()
    await asyncio.gather(*[executor.execute(c) for c in crawlers])
    elapsed = time.monotonic() - start

    assert client.max_concurrent == 4
    assert elapsed < 0.05 * 2  # 지금처럼 직렬화되면 4 * 0.05 = 0.2초 이상 걸림
