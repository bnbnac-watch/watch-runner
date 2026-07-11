import asyncio
import re

import httpx

_semaphore = asyncio.Semaphore(1)
_http_client: httpx.AsyncClient | None = None

# 크롤러 에러 메시지에 API 키 등이 쿼리스트링으로 섞여 알림까지 새는 것을 막는 방어선.
# 개별 크롤러가 놓친 경우를 대비한 것이라 시크릿 패턴을 열거하지 않고 URL 쿼리스트링 자체를 제거한다.
_QUERYSTRING_RE = re.compile(r"(https?://[^\s'\"]+?)\?[^\s'\"]*")


def set_client(client: httpx.AsyncClient):
    global _http_client
    _http_client = client


def _strip_querystrings(text: str) -> str:
    return _QUERYSTRING_RE.sub(r"\1", text)


async def execute(crawler: dict) -> list[dict]:
    target = crawler["container"]
    params = crawler.get("params") or {}
    async with _semaphore:
        res = await _http_client.post(f"http://{target}:8080/crawl", json=params, timeout=60)
        if res.status_code != 200:
            body = _strip_querystrings(res.text[:300])
            raise Exception(f"{target} 응답 {res.status_code}: {body}")
        return res.json()
