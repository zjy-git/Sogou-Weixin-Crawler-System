from __future__ import annotations

import logging
import random
import threading
import time
from datetime import datetime
from typing import Dict, Optional

from detector.antispider_detector import RequestResultType
from session_pool.session_client import SessionClient, SessionState
from session_pool.session_factory import SessionFactory


class SessionPool:
    """
    Production-style session pool.
    - borrow_session(): LRU + health score + weighted random
    - return_session(): feedback health, cooldown, rotate/destroy
    """

    def __init__(
        self,
        factory: SessionFactory,
        pool_size: int = 6,
        fail_threshold: int = 5,
        cooldown_range: tuple[int, int] = (30, 60),
    ) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._factory = factory
        self._pool_size = pool_size
        self._fail_threshold = fail_threshold
        self._cooldown_range = cooldown_range

        self._sessions: Dict[str, SessionClient] = {}
        self._cooldown_queue: set[str] = set()

        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._stop_event = threading.Event()

        self._build_initial_pool()

        self._cooldown_recover_thread = threading.Thread(
            target=self._recover_cooldown_loop,
            name="session-cooldown-recover",
            daemon=True,
        )
        self._cooldown_recover_thread.start()

    def _build_initial_pool(self) -> None:
        for _ in range(self._pool_size):
            session = self._safe_create_session()
            if session:
                self._sessions[session.session_id] = session

    def borrow_session(self, timeout: float = 10.0) -> Optional[SessionClient]:
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                now = datetime.utcnow()
                ready_sessions = [
                    item
                    for item in self._sessions.values()
                    if item.is_ready(now)
                ]

                if ready_sessions:
                    session = self._pick_session(ready_sessions)
                    session.record_borrow(now)
                    return session

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(timeout=min(0.5, remaining))

    def return_session(self, session: SessionClient, result: RequestResultType) -> None:
        with self._condition:
            current = self._sessions.get(session.session_id)
            if current is None:
                return

            now = datetime.utcnow()

            if result == RequestResultType.SUCCESS:
                current.record_success(now)
                if current.should_rotate():
                    self._replace_session(current.session_id)

            elif result in (RequestResultType.NETWORK_ERROR, RequestResultType.SERVER_ERROR):
                current.record_failure(now)
                if current.consecutive_failures >= self._fail_threshold or current.should_rotate():
                    self._replace_session(current.session_id)
                else:
                    current.state = SessionState.AVAILABLE

            elif result == RequestResultType.PARSE_ERROR:
                # Parse failure generally means HTML layout changed, do not over-penalize session.
                current.consecutive_failures = 0
                current.health_score = max(-1.0, current.health_score - 0.03)
                current.state = SessionState.AVAILABLE
                current.last_used_time = now
                if current.should_rotate():
                    self._replace_session(current.session_id)

            elif result in (RequestResultType.ANTISPIDER, RequestResultType.RATE_LIMIT):
                current.record_failure(now)
                if current.consecutive_failures >= self._fail_threshold:
                    self._replace_session(current.session_id)
                else:
                    cooldown_seconds = random.randint(*self._cooldown_range)
                    current.mark_cooldown(cooldown_seconds, now)
                    self._cooldown_queue.add(current.session_id)

            self._condition.notify_all()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._cooldown_recover_thread.join(timeout=2.0)

        with self._condition:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            self._cooldown_queue.clear()

        for session in sessions:
            self._factory.destroy_session(session)

    def _pick_session(self, candidates: list[SessionClient]) -> SessionClient:
        # 1) LRU shortlist: prefer sessions with earlier last_used_time.
        lru_sorted = sorted(candidates, key=lambda item: item.last_used_time)
        shortlist_size = 1 if len(lru_sorted) == 1 else max(2, len(lru_sorted) // 2)
        shortlist = lru_sorted[:shortlist_size]

        # 2) Weighted random in shortlist: healthier session has higher chance.
        weights = [max(0.1, session.health_score + 1.1) for session in shortlist]
        return random.choices(shortlist, weights=weights, k=1)[0]

    def _recover_cooldown_loop(self) -> None:
        while not self._stop_event.is_set():
            recovered = False
            with self._condition:
                now = datetime.utcnow()
                for session_id in list(self._cooldown_queue):
                    session = self._sessions.get(session_id)
                    if session is None:
                        self._cooldown_queue.discard(session_id)
                        continue
                    if session.cooldown_until and now >= session.cooldown_until:
                        session.cooldown_until = None
                        session.state = SessionState.AVAILABLE
                        session.consecutive_failures = max(0, session.consecutive_failures - 1)
                        self._cooldown_queue.discard(session_id)
                        recovered = True

                if recovered:
                    self._condition.notify_all()
            self._stop_event.wait(1.0)

    def _replace_session(self, session_id: str) -> None:
        old = self._sessions.pop(session_id, None)
        if old is None:
            return

        self._cooldown_queue.discard(session_id)
        self._factory.destroy_session(old)

        new_session = self._safe_create_session()
        if new_session:
            self._sessions[new_session.session_id] = new_session

    def _safe_create_session(self) -> Optional[SessionClient]:
        try:
            return self._factory.create_session()
        except Exception:
            self._logger.exception("Create session failed")
            return None
