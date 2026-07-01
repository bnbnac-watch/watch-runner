import os
from contextlib import asynccontextmanager
import httpx
import uvicorn
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import db
import executor
import deduplicator
from scheduler import create_scheduler, sync_jobs

MAX_FAIL_COUNT = int(os.getenv("MAX_FAIL_COUNT", "5"))

_scheduler: AsyncIOScheduler | None = None


async def _notify_items(crawler_id: str, items: list[dict]):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            "http://watch-sender:8080/notify",
            json={"crawler_id": crawler_id, "items": items},
        )


async def _notify_error(crawler_id: str, error: str, fail_count: int):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            "http://watch-sender:8080/error",
            json={"crawler_id": crawler_id, "error": error, "fail_count": fail_count},
        )


async def run_crawler(crawler_id: str):
    try:
        items = await executor.execute(crawler_id)
        new_items = await deduplicator.filter_new(crawler_id, items)
        if new_items:
            await _notify_items(crawler_id, new_items)
            await deduplicator.mark_seen(crawler_id, [item["id"] for item in new_items])
        await db.update_success(crawler_id)
    except Exception as e:
        fail_count = await db.increment_fail_count(crawler_id)
        try:
            await _notify_error(crawler_id, str(e), fail_count)
        except Exception:
            pass
        if fail_count >= MAX_FAIL_COUNT:
            await db.disable_crawler(crawler_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    await db.init()
    _scheduler = await create_scheduler(run_crawler)
    _scheduler.start()
    yield
    _scheduler.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    return {
        "jobs": [
            {"id": job.id, "next_run": str(job.next_run_time)}
            for job in _scheduler.get_jobs()
        ]
    }


@app.post("/reload")
async def reload():
    await sync_jobs(_scheduler, run_crawler)
    return {"status": "reloaded"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, loop="asyncio")
