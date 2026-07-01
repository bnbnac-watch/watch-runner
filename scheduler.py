from typing import Callable, Awaitable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import db

_TZ = "Asia/Seoul"


def _add_job(scheduler: AsyncIOScheduler, crawler: dict, run_fn: Callable[[str], Awaitable[None]]):
    scheduler.add_job(
        run_fn,
        CronTrigger.from_crontab(crawler["schedule"], timezone=_TZ),
        args=[crawler["id"]],
        id=crawler["id"],
        max_instances=1,
        misfire_grace_time=30,
    )


async def create_scheduler(run_fn: Callable[[str], Awaitable[None]]) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=_TZ)
    for crawler in await db.get_enabled_crawlers():
        _add_job(scheduler, crawler, run_fn)
    return scheduler


async def sync_jobs(scheduler: AsyncIOScheduler, run_fn: Callable[[str], Awaitable[None]]):
    crawlers = await db.get_enabled_crawlers()
    db_ids = {c["id"]: c for c in crawlers}
    job_ids = {job.id for job in scheduler.get_jobs()}

    for crawler_id, crawler in db_ids.items():
        if crawler_id not in job_ids:
            _add_job(scheduler, crawler, run_fn)

    for job_id in job_ids - db_ids.keys():
        scheduler.remove_job(job_id)
