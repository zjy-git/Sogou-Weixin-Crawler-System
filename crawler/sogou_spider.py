from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Sequence

from crawler.worker import Worker
from detector.antispider_detector import AntiSpiderDetector
from parser.sogou_parser import SogouParser
from proxy_pool.proxy_pool import ProxyPool
from scheduler.request_scheduler import RequestScheduler
from session_pool.session_factory import SessionFactory
from session_pool.session_pool import SessionPool
from utils.rate_limiter import GlobalRateLimiter


class InMemoryStorage:
    def __init__(self) -> None:
        self._records: List[dict[str, str]] = []
        self._lock = threading.Lock()

    def save_records(self, records: Sequence[dict[str, str]]) -> None:
        if not records:
            return
        with self._lock:
            self._records.extend(records)

    def all_records(self) -> List[dict[str, str]]:
        with self._lock:
            return list(self._records)

    def clear(self) -> None:
        with self._lock:
            self._records.clear()

    def dump_json(self, path: str | Path) -> None:
        target = Path(path)
        target.write_text(
            json.dumps(self.all_records(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


@dataclass(slots=True)
class SpiderConfig:
    session_pool_size: int = 6
    worker_count: int = 2
    global_rate_limit: float = 2.0
    max_retries: int = 2
    fail_threshold: int = 5
    cooldown_range: tuple[int, int] = (30, 60)
    request_interval_range: tuple[float, float] = (1.0, 1.5)  # (3.0, 8.0)
    max_requests_range: tuple[int, int] = (80, 120)
    request_timeout: float = 10.0
    proxies: List[str] = field(default_factory=list)


class SogouSpider:
    def __init__(self, config: SpiderConfig) -> None:
        self.config = config

        self.proxy_pool = ProxyPool(config.proxies)
        self.session_factory = SessionFactory(
            proxy_pool=self.proxy_pool,
            request_interval_range=config.request_interval_range,
            max_requests_range=config.max_requests_range,
            warmup_timeout=config.request_timeout,
        )
        self.session_pool = SessionPool(
            factory=self.session_factory,
            pool_size=config.session_pool_size,
            fail_threshold=config.fail_threshold,
            cooldown_range=config.cooldown_range,
        )

        self.rate_limiter = GlobalRateLimiter(config.global_rate_limit)
        self.detector = AntiSpiderDetector()
        self.parser = SogouParser()
        self.storage = InMemoryStorage()

        self.stop_event = threading.Event()
        self.workers: List[Worker] = []

    def crawl(self, keyword: str, pages: int) -> List[dict[str, str]]:
        if pages <= 0:
            return []

        scheduler = RequestScheduler(max_retries=self.config.max_retries)
        scheduler.add_keyword_tasks(keyword=keyword, pages=pages)

        self.storage.clear()
        self.stop_event.clear()
        self.workers = [
            Worker(
                worker_id=index + 1,
                scheduler=scheduler,
                session_pool=self.session_pool,
                parser=self.parser,
                detector=self.detector,
                storage=self.storage,
                rate_limiter=self.rate_limiter,
                stop_event=self.stop_event,
                request_timeout=self.config.request_timeout,
            )
            for index in range(self.config.worker_count)
        ]

        for worker in self.workers:
            worker.start()

        scheduler.join()
        self.stop_event.set()

        for worker in self.workers:
            worker.join(timeout=3.0)

        return self.storage.all_records()

    def close(self) -> None:
        self.stop_event.set()
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=1.0)
        self.session_pool.shutdown()

    def __enter__(self) -> "SogouSpider":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
