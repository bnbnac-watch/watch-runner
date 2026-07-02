from collections import defaultdict
from typing import Callable, Awaitable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import db

_TZ = "Asia/Seoul"


def _add_job(scheduler: AsyncIOScheduler, crawler: dict, run_fn: Callable[[dict], Awaitable[None]]):
    scheduler.add_job(
        run_fn,
        CronTrigger.from_crontab(crawler["schedule"], timezone=_TZ),
        args=[crawler],
        id=crawler["id"],
        max_instances=1,
        misfire_grace_time=30,
    )


def _add_batch_job(scheduler: AsyncIOScheduler, group_name: str, group_crawlers: list[dict],
                   batch_run_fn: Callable[[str], Awaitable[None]]):
    schedule = group_crawlers[0]["schedule"]
    scheduler.add_job(
        batch_run_fn,
        CronTrigger.from_crontab(schedule, timezone=_TZ),
        args=[group_name],
        id=f"batch:{group_name}",
        max_instances=1,
        misfire_grace_time=60,
    )


async def create_scheduler(run_fn: Callable[[dict], Awaitable[None]],
                           batch_run_fn: Callable[[str], Awaitable[None]]) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=_TZ)
    batch_groups: dict[str, list[dict]] = defaultdict(list)

    for crawler in await db.get_enabled_crawlers():
        if crawler["batch_group"] is None:
            _add_job(scheduler, crawler, run_fn)
        else:
            batch_groups[crawler["batch_group"]].append(crawler)

    for group_name, group_crawlers in batch_groups.items():
        _add_batch_job(scheduler, group_name, group_crawlers, batch_run_fn)

    return scheduler


async def sync_jobs(scheduler: AsyncIOScheduler, run_fn: Callable[[dict], Awaitable[None]],
                    batch_run_fn: Callable[[str], Awaitable[None]]):
    crawlers = await db.get_enabled_crawlers()
    batch_groups: dict[str, list[dict]] = defaultdict(list)
    independent = []

    for crawler in crawlers:
        if crawler["batch_group"] is None:
            independent.append(crawler)
        else:
            batch_groups[crawler["batch_group"]].append(crawler)

    db_job_ids = {c["id"] for c in independent} | {f"batch:{g}" for g in batch_groups}
    current_job_ids = {job.id for job in scheduler.get_jobs()}

    for job_id in current_job_ids - db_job_ids:
        scheduler.remove_job(job_id)

    for crawler in independent:
        if crawler["id"] in current_job_ids:
            scheduler.remove_job(crawler["id"])
        _add_job(scheduler, crawler, run_fn)

    for group_name, group_crawlers in batch_groups.items():
        job_id = f"batch:{group_name}"
        if job_id in current_job_ids:
            scheduler.remove_job(job_id)
        _add_batch_job(scheduler, group_name, group_crawlers, batch_run_fn)
