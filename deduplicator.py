import logging

import db

logger = logging.getLogger(__name__)


def _dedupe_by_id(items: list[dict]) -> list[dict]:
    seen_ids = set()
    deduped = []
    for item in items:
        if item["id"] in seen_ids:
            continue
        seen_ids.add(item["id"])
        deduped.append(item)
    if len(deduped) != len(items):
        logger.warning(
            "크롤링 결과 내 id 중복 %d개 제거 (%d -> %d)",
            len(items) - len(deduped), len(items), len(deduped),
        )
    return deduped


async def filter_new(crawler_id: str, items: list[dict]) -> list[dict]:
    if not items:
        return []
    items = _dedupe_by_id(items)
    item_ids = [item["id"] for item in items]
    async with db.get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT item_id FROM seen_items WHERE crawler_id = $1 AND item_id = ANY($2)",
            crawler_id,
            item_ids,
        )
    seen_ids = {row["item_id"] for row in rows}
    return [item for item in items if item["id"] not in seen_ids]


async def mark_seen(crawler_id: str, item_ids: list[str]):
    async with db.get_pool().acquire() as conn:
        await conn.executemany(
            "INSERT INTO seen_items (crawler_id, item_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            [(crawler_id, item_id) for item_id in item_ids],
        )


async def filter_new_batch(crawler_ids: list[int], items: list[dict]) -> list[dict]:
    if not items:
        return []
    items = _dedupe_by_id(items)
    item_ids = [item["id"] for item in items]
    async with db.get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT item_id FROM seen_items WHERE crawler_id = ANY($1) AND item_id = ANY($2)",
            crawler_ids,
            item_ids,
        )
    seen_ids = {row["item_id"] for row in rows}
    return [item for item in items if item["id"] not in seen_ids]


async def mark_seen_batch(crawler_ids: list[int], item_ids: list[str]):
    async with db.get_pool().acquire() as conn:
        await conn.executemany(
            "INSERT INTO seen_items (crawler_id, item_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            [(crawler_id, item_id) for crawler_id in crawler_ids for item_id in item_ids],
        )
