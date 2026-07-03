import asyncio
import logging
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MAX_FAIL_COUNT = int(os.getenv("MAX_FAIL_COUNT", "5"))
SUMMARIZE_CONCURRENCY = int(os.getenv("SUMMARIZE_CONCURRENCY", "4"))
WATCH_AI_URL = os.getenv("WATCH_AI_URL", "http://watch-ai:8080")
WATCH_SENDER_URL = os.getenv("WATCH_SENDER_URL", "http://watch-sender:8080")

_summarize_sem = asyncio.Semaphore(SUMMARIZE_CONCURRENCY)

_scheduler: AsyncIOScheduler | None = None


async def _notify_items(crawler_id: str, items: list[dict]):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"{WATCH_SENDER_URL}/notify",
            json={"crawler_id": crawler_id, "items": items},
        )


async def _notify_error(crawler_id: str, error: str, fail_count: int):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"{WATCH_SENDER_URL}/error",
            json={"crawler_id": crawler_id, "error": error, "fail_count": fail_count},
        )


async def _summarize(url: str) -> str | None:
    async with _summarize_sem:
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                res = await client.post(f"{WATCH_AI_URL}/summarize", json={"url": url})
                res.raise_for_status()
                return res.json().get("result")
        except Exception as e:
            logger.error("watch-ai 호출 실패 (%s): %s", url, e)
            return None


async def run_crawler(crawler: dict):
    crawler_id = crawler["id"]
    logger.info("[%s] job 시작", crawler_id)
    try:
        items = await executor.execute(crawler)
        new_items = await deduplicator.filter_new(crawler_id, items)
        logger.info("[%s] 새 아이템 %d개 (전체 %d개)", crawler_id, len(new_items), len(items))
        if new_items:
            post = crawler.get("post_process") or {}
            if post.get("type") == "summarize":
                summaries = await asyncio.gather(*[_summarize(item["url"]) for item in new_items])
                for item, summary in zip(new_items, summaries):
                    item["summary"] = summary
            await _notify_items(crawler_id, new_items)
            await deduplicator.mark_seen(crawler_id, [item["id"] for item in new_items])
        await db.update_success(crawler_id)
        logger.info("[%s] job 완료", crawler_id)
    except Exception as e:
        logger.error("[%s] 오류: %s", crawler_id, e)
        fail_count = await db.increment_fail_count(crawler_id)
        try:
            await _notify_error(crawler_id, str(e), fail_count)
        except Exception:
            pass
        if fail_count >= MAX_FAIL_COUNT:
            await db.disable_crawler(crawler_id)


async def run_batch(group_name: str):
    logger.info("[batch:%s] 시작", group_name)
    crawlers = await db.get_crawlers_by_batch_group(group_name)
    entries = []

    for crawler in crawlers:
        crawler_id = crawler["id"]
        try:
            items = await executor.execute(crawler)
            new_items = await deduplicator.filter_new(crawler_id, items)
            logger.info("[%s] 새 아이템 %d개", crawler_id, len(new_items))
            if new_items:
                post = crawler.get("post_process") or {}
                if post.get("type") == "summarize":
                    summaries = await asyncio.gather(*[_summarize(item["url"]) for item in new_items])
                    for item, summary in zip(new_items, summaries):
                        item["summary"] = summary
                await deduplicator.mark_seen(crawler_id, [item["id"] for item in new_items])
                entries.append({"crawler_id": crawler_id, "items": new_items})
            await db.update_success(crawler_id)
        except Exception as e:
            logger.error("[%s] 오류: %s", crawler_id, e)
            await db.increment_fail_count(crawler_id)

    if entries:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(f"{WATCH_SENDER_URL}/notify/batch", json={"entries": entries})
        except Exception as e:
            logger.error("[batch:%s] 발송 실패: %s", group_name, e)

    logger.info("[batch:%s] 완료 (%d개 crawler 처리)", group_name, len(crawlers))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    await db.init()
    _scheduler = await create_scheduler(run_crawler, run_batch)
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
    await sync_jobs(_scheduler, run_crawler, run_batch)
    return {"status": "reloaded"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, loop="asyncio")
