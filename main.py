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
import jobs
from scheduler import create_scheduler, sync_jobs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MAX_FAIL_COUNT = int(os.getenv("MAX_FAIL_COUNT", "5"))
# 요약이 이 횟수만큼 연속 실패하면 포기하고 요약 없이(링크만) 발송한다.
# 재시도는 크롤 사이클 단위로 일어나므로(watch-ai 자체 재시도와 별개),
# 실제 최대 지연 시간은 이 값 × 해당 크롤러의 크롤 주기다.
MAX_SUMMARY_ATTEMPTS = int(os.getenv("MAX_SUMMARY_ATTEMPTS", "3"))
WATCH_AI_URL = os.getenv("WATCH_AI_URL", "http://watch-ai:8080")
WATCH_SENDER_URL = os.getenv("WATCH_SENDER_URL", "http://watch-sender:8080")
WATCH_GALLERY_URL = os.getenv("WATCH_GALLERY_URL", "http://watch-gallery:8080")

# /extract_images 엔드포인트를 구현한 크롤러만 여기 추가한다. DB 스키마 변경 없이
# 컨테이너 이름으로 판단 - 두 번째 크롤러가 필요해지면 crawlers 테이블에 플래그를
# 추가하는 걸 고려할 것.
IMAGE_CAPABLE_CONTAINERS = {"crawler-kakao-channels"}

_scheduler: AsyncIOScheduler | None = None
_http_client: httpx.AsyncClient | None = None


async def _notify_items(crawler_id: str, items: list[dict]):
    await _http_client.post(
        f"{WATCH_SENDER_URL}/notify",
        json={"crawler_id": crawler_id, "items": items},
        timeout=10,
    )


async def _notify_error(crawler_id: str, error: str, fail_count: int):
    await _http_client.post(
        f"{WATCH_SENDER_URL}/error",
        json={"crawler_id": crawler_id, "error": error, "fail_count": fail_count},
        timeout=10,
    )


class _PermanentSummaryFailure(Exception):
    """자막 없음(404) 등 재시도해도 절대 성공할 수 없는 실패. 재시도 대상에서 제외하고 즉시 포기하기 위한 신호."""


async def _call_summarize_api(url: str) -> str | None:
    # job 생성 요청은 가벼운 INSERT + 202 응답이라 10초면 충분하다. 실제 요약
    # 작업 대기는 jobs.wait_for_job의 300초 상한이 담당. watch-ai 큐잉이
    # 300초를 넘기면 이 호출은 포기하고 None을 반환한다 — pending_summaries가
    # 다음 크롤 사이클에 재시도한다. 유실은 없지만(재시도되므로), 300초 안에
    # 못 끝난 job이 뒤늦게 완료되면 그 Gemini 호출은 낭비된다(재사용 안 함).
    try:
        res = await _http_client.post(f"{WATCH_AI_URL}/summarize", json={"url": url}, timeout=10)
        res.raise_for_status()
        job_id = res.json()["job_id"]
    except Exception as e:
        logger.error("watch-ai 요청 실패 (%s): %s", url, e)
        return None

    row = await jobs.wait_for_job(job_id, timeout=300)
    if row is None:
        return None
    if row["status"] == "done":
        return row["result"]["result"]
    if not row["retryable"]:
        raise _PermanentSummaryFailure(row["error"])
    logger.warning("watch-ai 요약 실패 (%s): %s", url, row["error"])
    return None


async def _summarize(url: str) -> str | None:
    try:
        return await _call_summarize_api(url)
    except _PermanentSummaryFailure:
        return None


async def _resolve_item_summary(url: str) -> tuple[str | None, bool]:
    """(summary, permanent) 반환. permanent=True면 자막 없음 등으로 재시도해도 소용없다는 뜻."""
    try:
        return await _call_summarize_api(url), False
    except _PermanentSummaryFailure as e:
        logger.warning("watch-ai 요약 불가(재시도 무의미): %s", e)
        return None, True


async def _resolve_summaries(crawler_id: int, items: list[dict]) -> list[dict]:
    results = await asyncio.gather(*[_resolve_item_summary(item["url"]) for item in items])
    resolved = []
    for item, (summary, permanent) in zip(items, results):
        if summary is not None:
            await db.clear_summary_attempts(crawler_id, item["id"])
            item["summary"] = summary
            resolved.append(item)
            continue

        if permanent:
            logger.warning(
                "[%s] 요약 불가(자막 없음 등), 재시도 없이 포기하고 링크만 발송: %s", crawler_id, item["url"]
            )
            await db.clear_summary_attempts(crawler_id, item["id"])
            item["summary"] = None
            resolved.append(item)
            continue

        attempts = await db.increment_summary_attempts(crawler_id, item["id"])
        if attempts >= MAX_SUMMARY_ATTEMPTS:
            logger.warning(
                "[%s] 요약 %d회 실패, 포기하고 링크만 발송: %s", crawler_id, attempts, item["url"]
            )
            await db.clear_summary_attempts(crawler_id, item["id"])
            item["summary"] = None
            resolved.append(item)
        else:
            logger.warning(
                "[%s] 요약 실패 %d/%d, 다음 크롤에서 재시도 보류: %s",
                crawler_id, attempts, MAX_SUMMARY_ATTEMPTS, item["url"],
            )
    return resolved


async def _extract_images(container: str, url: str) -> list[str]:
    try:
        res = await _http_client.post(
            f"http://{container}:8080/extract_images",
            json={"url": url},
            timeout=60,
        )
        res.raise_for_status()
        return res.json().get("image_urls", [])
    except Exception as e:
        logger.warning("이미지 추출 실패 (%s): %s", url, e)
        return []


async def _build_image_grid(image_urls: list[str]) -> str | None:
    try:
        res = await _http_client.post(
            f"{WATCH_GALLERY_URL}/build",
            json={"image_urls": image_urls},
            timeout=120,
        )
        res.raise_for_status()
        return res.json().get("public_url")
    except Exception as e:
        logger.warning("이미지 그리드 생성 실패: %s", e)
        return None


async def _attach_image_summaries(container: str, items: list[dict]):
    # watch-playwright가 단일 동시성이라(MAX_CONCURRENCY=1), 여기서도 순차 처리해
    # 렌더 요청이 한꺼번에 몰리지 않게 한다.
    #
    # summary가 아니라 별도 필드(image_grid_url)에 담는다 - summary는 원본 url을
    # 이미 포함하고 있다는 전제(YouTube 요약이 타임스탬프에 {url}을 인라인으로
    # 박아 넣는 방식)로 formatters.format_items가 summary 있으면 url을 생략하는데,
    # 그리드 url은 원본 게시글 url이 아니라서 여기 합치면 게시글 url이 통째로 사라진다.
    for item in items:
        image_urls = await _extract_images(container, item["url"])
        if not image_urls:
            continue
        grid_url = await _build_image_grid(image_urls)
        if not grid_url:
            continue
        item["image_grid_url"] = grid_url


def _apply_filter(crawler: dict, items: list[dict]) -> list[dict]:
    flt = crawler.get("filter") or {}
    title_keywords = flt.get("title_keywords")
    description_keywords = flt.get("description_keywords")
    if not title_keywords and not description_keywords:
        return items

    def matches(item: dict) -> bool:
        if title_keywords and any(k.lower() in item["title"].lower() for k in title_keywords):
            return True
        if description_keywords:
            description = (item.get("data") or {}).get("description") or ""
            if any(k.lower() in description.lower() for k in description_keywords):
                return True
        return False

    return [item for item in items if matches(item)]


async def run_crawler(crawler: dict):
    crawler_id = crawler["id"]
    logger.info("[%s] job 시작", crawler_id)
    try:
        items = await executor.execute(crawler)
        items = _apply_filter(crawler, items)
        new_items = await deduplicator.filter_new(crawler_id, items)
        logger.info("[%s] 새 아이템 %d개 (전체 %d개)", crawler_id, len(new_items), len(items))
        if new_items:
            post = crawler.get("post_process") or {}
            if post.get("type") == "summarize":
                new_items = await _resolve_summaries(crawler_id, new_items)
            if new_items and crawler["container"] in IMAGE_CAPABLE_CONTAINERS:
                await _attach_image_summaries(crawler["container"], new_items)
            if new_items:
                await _notify_items(crawler_id, new_items)
                await deduplicator.mark_seen(crawler_id, [item["id"] for item in new_items])
        await db.update_success(crawler_id)
        logger.info("[%s] job 완료", crawler_id)
    except Exception as e:
        logger.error("[%s] 오류: %s", crawler_id, e)
        fail_count = await db.increment_fail_count(crawler_id, str(e))
        try:
            await _notify_error(crawler_id, str(e), fail_count)
        except Exception:
            pass
        if fail_count >= MAX_FAIL_COUNT:
            await db.disable_crawler(crawler_id)


async def run_batch(group_name: str):
    logger.info("[batch:%s] 시작", group_name)
    crawlers = await db.get_crawlers_by_batch_group(group_name)
    crawler_ids = [c["id"] for c in crawlers]
    entries = []

    for crawler in crawlers:
        crawler_id = crawler["id"]
        try:
            items = await executor.execute(crawler)
            items = _apply_filter(crawler, items)
            new_items = await deduplicator.filter_new_batch(crawler_ids, items)
            logger.info("[%s] 새 아이템 %d개", crawler_id, len(new_items))
            if new_items:
                post = crawler.get("post_process") or {}
                if post.get("type") == "summarize":
                    # 주의: run_crawler()와 달리 여기는 보류/재시도 로직이 없다 — 요약 실패 시
                    # 즉시 링크만 발송하고 영구 mark_seen된다. summarize를 쓰는 크롤러는
                    # batch_group을 절대 설정하지 말 것(현재 DB상 전부 batch_group IS NULL).
                    summaries = await asyncio.gather(*[_summarize(item["url"]) for item in new_items])
                    for item, summary in zip(new_items, summaries):
                        item["summary"] = summary
                await deduplicator.mark_seen_batch([crawler_id], [item["id"] for item in new_items])
                entries.append({"crawler_id": crawler_id, "items": new_items})
            await db.update_success(crawler_id)
        except Exception as e:
            logger.error("[%s] 오류: %s", crawler_id, e)
            await db.increment_fail_count(crawler_id, str(e))

    if entries:
        try:
            await _http_client.post(f"{WATCH_SENDER_URL}/notify/batch", json={"entries": entries}, timeout=10)
        except Exception as e:
            logger.error("[batch:%s] 발송 실패: %s", group_name, e)

    logger.info("[batch:%s] 완료 (%d개 crawler 처리)", group_name, len(crawlers))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler, _http_client
    await db.init()
    async with httpx.AsyncClient() as client:
        _http_client = client
        executor.set_client(client)
        listener_task = await jobs.start_listener(os.environ["DATABASE_URL"])
        fallback_task = jobs.start_fallback_poll()
        _scheduler = await create_scheduler(run_crawler, run_batch)
        _scheduler.start()
        yield
        _scheduler.shutdown()
        listener_task.cancel()
        fallback_task.cancel()


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
