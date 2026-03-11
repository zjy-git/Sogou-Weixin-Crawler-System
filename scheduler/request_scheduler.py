from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from typing import Optional
from urllib.parse import quote_plus


@dataclass(slots=True)
class RequestTask:
    keyword: str
    page: int
    retry: int = 0

    def build_url(self) -> str:
        query = quote_plus(self.keyword)
        return f"https://weixin.sogou.com/weixin?type=2&query={query}&page={self.page}&ie=utf8"


class RequestScheduler:
    def __init__(self, max_retries: int = 2) -> None:
        self.max_retries = max_retries
        self._queue: Queue[RequestTask] = Queue()

    def add_keyword_tasks(self, keyword: str, pages: int) -> None:
        for page in range(1, pages + 1):
            self._queue.put(RequestTask(keyword=keyword, page=page, retry=0))

    def get_task(self, timeout: float = 1.0) -> Optional[RequestTask]:
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return None

    def retry_task(self, task: RequestTask) -> bool:
        if task.retry >= self.max_retries:
            return False
        self._queue.put(RequestTask(keyword=task.keyword, page=task.page, retry=task.retry + 1))
        return True

    def task_done(self) -> None:
        self._queue.task_done()

    def join(self) -> None:
        self._queue.join()

    def pending_count(self) -> int:
        return self._queue.qsize()
