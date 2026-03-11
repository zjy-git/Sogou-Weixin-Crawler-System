from __future__ import annotations

import logging
import threading
from typing import Protocol, Sequence

import requests

from detector.antispider_detector import (
    AntiSpiderDetector,
    DetectionStatus,
    RequestResultType,
)
from parser.sogou_parser import SogouParser
from scheduler.request_scheduler import RequestScheduler
from session_pool.session_pool import SessionPool
from utils.rate_limiter import GlobalRateLimiter


class StorageProtocol(Protocol):
    def save_records(self, records: Sequence[dict[str, str]]) -> None:
        ...


class Worker(threading.Thread):
    def __init__(
        self,
        worker_id: int,
        scheduler: RequestScheduler,
        session_pool: SessionPool,
        parser: SogouParser,
        detector: AntiSpiderDetector,
        storage: StorageProtocol,
        rate_limiter: GlobalRateLimiter,
        stop_event: threading.Event,
        request_timeout: float = 10.0,
    ) -> None:
        super().__init__(name=f"worker-{worker_id}", daemon=True)
        self._logger = logging.getLogger(self.name)
        self.worker_id = worker_id
        self.scheduler = scheduler
        self.session_pool = session_pool
        self.parser = parser
        self.detector = detector
        self.storage = storage
        self.rate_limiter = rate_limiter
        self.stop_event = stop_event
        self.request_timeout = request_timeout

    def run(self) -> None:
        while not self.stop_event.is_set() or self.scheduler.pending_count() > 0:
            task = self.scheduler.get_task(timeout=1.0)
            if task is None:
                continue

            session_client = None
            result = RequestResultType.PARSE_ERROR
            retryable_failure = False

            try:
                session_client = self.session_pool.borrow_session(timeout=15.0)
                if session_client is None:
                    result = RequestResultType.NETWORK_ERROR
                    retryable_failure = True
                    continue

                self.rate_limiter.acquire()
                response = session_client.session.get(
                    task.build_url(),
                    timeout=self.request_timeout,
                    allow_redirects=False,
                )

                detect_result = self.detector.detect(response)
                if detect_result.status == DetectionStatus.BLOCKED:
                    result = detect_result.result_type
                    retryable_failure = True
                    continue
                if detect_result.status == DetectionStatus.FAIL:
                    result = detect_result.result_type
                    retryable_failure = self._is_retryable(result)
                    continue

                articles = self.parser.parse_search_results(response.text)
                if not articles:
                    result = RequestResultType.PARSE_ERROR
                    retryable_failure = False
                    continue

                # Probe jump URL once to mimic user behavior and inspect redirect chain.
                for article in articles:
                    if article.sogou_url:
                        self.rate_limiter.acquire()
                        self.parser.probe_sogou_redirect(
                            session_client.session,
                            article.sogou_url,
                            timeout=self.request_timeout,
                        )

                self.storage.save_records([article.to_dict() for article in articles])
                result = RequestResultType.SUCCESS

            except requests.RequestException as exc:
                self._logger.debug("request error on task %s: %s", task, exc)
                result = RequestResultType.NETWORK_ERROR
                retryable_failure = True
            except Exception:
                self._logger.exception("unexpected error")
                result = RequestResultType.PARSE_ERROR
                retryable_failure = False
            finally:
                if session_client is not None:
                    self.session_pool.return_session(session_client, result)
                if retryable_failure:
                    self.scheduler.retry_task(task)
                self.scheduler.task_done()

    @staticmethod
    def _is_retryable(result: RequestResultType) -> bool:
        return result in {
            RequestResultType.NETWORK_ERROR,
            RequestResultType.SERVER_ERROR,
            RequestResultType.ANTISPIDER,
            RequestResultType.RATE_LIMIT,
        }
