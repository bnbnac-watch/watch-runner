# watch-runner

스케줄러 + 중복 감지 + 오케스트레이션. 크롤러를 호출하고, 새 아이템을 걸러내고, 필요하면 요약을 붙여서 `watch-sender`에 발송을 위임한다. 실제 파싱이나 발송은 하지 않는다 — 그 둘은 각각 크롤러와 `watch-sender`의 책임이다.

## 동작 흐름

### 독립 크롤러 (`batch_group = null`)

```
run_crawler(crawler)
├── executor.execute(crawler)          → POST http://{container}:8080/crawl
├── _apply_filter()                    → filter.title_keywords 매칭 아이템만 통과
├── deduplicator.filter_new()          → seen_items와 비교해 새 아이템만 남김
├── post_process.type == "summarize"?  → watch-ai 병렬 호출(asyncio.gather)
├── watch-sender POST /notify
└── deduplicator.mark_seen()
```

### 배치 그룹 (`batch_group` 값 있음)

```
run_batch(group_name)
├── batch_group의 crawlers row 전부 순차 처리
│   ├── executor.execute() → filter → deduplicator.filter_new_batch(그룹 전체 crawler_ids)
│   ├── summarize (해당 시)
│   └── deduplicator.mark_seen_batch([자신의 crawler_id]만)  ← 쓰기 증폭 방지
└── 루프 종료 후 watch-sender POST /notify/batch (그룹 전체를 한 번에)
```

배치 그룹의 중복 감지는 그룹 전체 `seen_items`를 기준으로 읽지만(같은 아이템을 그룹 내 다른 row가 먼저 찾았으면 걸러짐), 기록은 실제로 찾은 crawler_id 하나에만 한다.

## 중복 감지 (`deduplicator.py`)

`Item.id`가 유일한 중복 감지 키다. `seen_items(crawler_id, item_id)` 테이블과 대조한다.

- `filter_new(crawler_id, items)` / `mark_seen(crawler_id, item_ids)` — 독립 크롤러용
- `filter_new_batch(crawler_ids, items)` / `mark_seen_batch(crawler_ids, item_ids)` — 배치 그룹용, `crawler_id = ANY(...)`로 그룹 전체 조회
- `_dedupe_by_id(items)` — 두 filter 함수 모두 DB 조회 전에 먼저 호출된다. **크롤링 한 번의 결과 리스트 안에서 같은 id가 중복으로 들어온 경우**(사이트가 같은 아이템을 페이지네이션 등으로 두 번 반환하는 경우 등)를 걸러낸다. `seen_items`에 `ON CONFLICT DO NOTHING`이 걸려 있어 DB 쓰기 단계에서는 중복이 조용히 사라지지만, 그 이전에 `watch-sender`로 넘어가는 알림 리스트 자체에 중복이 남아있어 사용자에게 같은 글이 여러 번 발송되는 문제가 있었다 — 이를 막기 위한 전처리.

## API

### GET /health

`{"status": "ok"}`

### GET /status

```json
{
  "jobs": [
    {"id": "3", "next_run": "2026-07-12 23:15:00+09:00"}
  ]
}
```

APScheduler에 등록된 잡 목록. 독립 크롤러는 `id`가 `crawlers.id` 문자열, 배치 그룹은 `batch:<group_name>`.

### POST /reload

DB의 `crawlers` 테이블을 다시 읽어 스케줄러 잡을 갱신한다(`sync_jobs`). DB에서 사라졌거나 `enabled=false`가 된 잡은 제거하고, 남은 잡은 전부 지웠다가 다시 등록한다. 크롤러 추가/스케줄 변경을 재배포 없이 반영할 때 호출.

## 환경변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | PostgreSQL 연결 문자열 |
| `MAX_FAIL_COUNT` | 5 | 연속 실패 시 크롤러를 자동 비활성화하는 기준 |
| `SUMMARIZE_CONCURRENCY` | 4 | watch-ai 호출 동시성 제한 (`asyncio.Semaphore`) |
| `WATCH_AI_URL` | `http://watch-ai:8080` | |
| `WATCH_SENDER_URL` | `http://watch-sender:8080` | |

## 포트

| 포트 | 용도 |
|---|---|
| 8080 (컨테이너), `8001`로 외부 노출 | FastAPI — `/health`, `/status`, `/reload` 수동 호출용 |

## 실패 처리

크롤러 호출/파싱이 예외를 던지면 `fail_count`를 증가시키고 `watch-sender`의 `/error`로 알림을 보낸다. `fail_count`가 `MAX_FAIL_COUNT`에 도달하면 해당 크롤러를 `enabled=false`로 비활성화한다(다음 `/reload` 또는 재시작 시 스케줄러에서 빠짐). 성공 시 `fail_count`는 0으로 리셋된다.

## 알려진 제약

같은 `batch_group`에 속한 모든 `crawlers` row는 `schedule` 값이 동일해야 한다. 배치 잡은 그룹당 하나만 등록되고 그룹의 첫 row(`group_crawlers[0]`) schedule만 사용되는데, `db.get_enabled_crawlers()`에 정렬 기준이 없어 "첫 row"가 무엇인지 보장되지 않는다. 그룹 내 한 row만 schedule을 바꾸면 경고 없이 무시된다.
