import asyncio
import httpx

_semaphore = asyncio.Semaphore(2)


async def execute(crawler: dict) -> list[dict]:
    target = crawler["container"]
    params = crawler.get("params") or {}
    async with _semaphore:
        async with httpx.AsyncClient(timeout=60) as client:
            res = await client.post(f"http://{target}:8080/crawl", json=params)
            res.raise_for_status()
            return res.json()
