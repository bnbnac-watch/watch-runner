import db


async def filter_new(crawler_id: str, items: list[dict]) -> list[dict]:
    if not items:
        return []
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
