import asyncio
import httpx

_semaphore = asyncio.Semaphore(2)


async def execute(crawler_id: str) -> list[dict]:
    async with _semaphore:
        async with httpx.AsyncClient(timeout=60) as client:
            res = await client.post(f"http://{crawler_id}:8080/crawl")
            res.raise_for_status()
            return res.json()
