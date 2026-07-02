import os
import asyncpg

_pool: asyncpg.Pool | None = None


async def init():
    global _pool
    _pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])


def get_pool() -> asyncpg.Pool:
    return _pool


async def get_enabled_crawlers() -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, schedule, container, params, post_process, batch_group "
            "FROM crawlers WHERE enabled = true"
        )
        return [dict(r) for r in rows]


async def get_crawlers_by_batch_group(group: str) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, schedule, container, params, post_process, batch_group "
            "FROM crawlers WHERE batch_group = $1 AND enabled = true",
            group,
        )
        return [dict(r) for r in rows]


async def update_success(crawler_id: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE crawlers SET last_run = NOW(), fail_count = 0 WHERE id = $1",
            crawler_id,
        )


async def increment_fail_count(crawler_id: str) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE crawlers SET fail_count = fail_count + 1 WHERE id = $1 RETURNING fail_count",
            crawler_id,
        )
        return row["fail_count"]


async def disable_crawler(crawler_id: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE crawlers SET enabled = false WHERE id = $1", crawler_id
        )
