import asyncio
import httpx

_semaphore = asyncio.Semaphore(2)
_http_client: httpx.AsyncClient | None = None


def set_client(client: httpx.AsyncClient):
    global _http_client
    _http_client = client


async def execute(crawler: dict) -> list[dict]:
    target = crawler["container"]
    params = crawler.get("params") or {}
    async with _semaphore:
        res = await _http_client.post(f"http://{target}:8080/crawl", json=params, timeout=60)
        res.raise_for_status()
        return res.json()
