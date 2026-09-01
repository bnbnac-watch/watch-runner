import asyncio
import logging

import asyncpg

import db

logger = logging.getLogger(__name__)

FALLBACK_POLL_INTERVAL_S = 30

_pending: dict[str, asyncio.Future] = {}


def _resolve(job_id: str, row: dict):
    fut = _pending.pop(job_id, None)
    if fut and not fut.done():
        fut.set_result(row)


async def wait_for_job(job_id: str, timeout: int = 300) -> dict | None:
    fut = asyncio.get_event_loop().create_future()
    _pending[job_id] = fut
    try:
        row = await db.get_job(job_id)
        if row is not None and row["status"] != "pending":
            _pending.pop(job_id, None)
            return row
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("job 대기 타임아웃 (%s)", job_id)
        return None
    finally:
        _pending.pop(job_id, None)


async def _on_pg_notify(conn, pid, channel, payload):
    job_id = payload
    row = await db.get_job(job_id)
    if row is not None:
        _resolve(job_id, row)


async def _fallback_poll_loop():
    while True:
        await asyncio.sleep(FALLBACK_POLL_INTERVAL_S)
        for job_id in list(_pending.keys()):
            row = await db.get_job(job_id)
            if row is not None and row["status"] != "pending":
                _resolve(job_id, row)


async def start_listener(dsn: str) -> asyncio.Task:
    """전용 LISTEN 커넥션을 열고 유지하다가, 끊기면 재연결하는 supervisor
    task를 반환한다. 호출자는 앱 종료 시 이 task를 cancel()하면 된다."""

    async def _run():
        while True:
            conn = None
            try:
                conn = await asyncpg.connect(dsn)
                closed = asyncio.Event()
                conn.add_termination_listener(lambda c: closed.set())
                await conn.add_listener("async_job_done", _on_pg_notify)
                logger.info("job 리스너 커넥션 연결됨")
                await closed.wait()
                logger.warning("job 리스너 커넥션 끊김, 재연결 시도")
            except Exception as exc:
                logger.warning("job 리스너 커넥션 오류: %s", exc)
            finally:
                if conn is not None and not conn.is_closed():
                    await conn.close()
            await asyncio.sleep(5)

    return asyncio.create_task(_run())


def start_fallback_poll() -> asyncio.Task:
    return asyncio.create_task(_fallback_poll_loop())
