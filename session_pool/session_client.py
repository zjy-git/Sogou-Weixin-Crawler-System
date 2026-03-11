from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

import requests


class SessionState(str, Enum):
    CREATED = "create"
    AVAILABLE = "available"
    BORROWED = "borrowed"
    COOLDOWN = "cooldown"
    DESTROYED = "destroy"


@dataclass
class SessionClient:
    session: requests.Session
    session_id: str
    proxy: Optional[str]
    user_agent: str
    headers_profile: dict[str, str]
    cookies: requests.cookies.RequestsCookieJar
    create_time: datetime
    last_used_time: datetime
    request_count: int = 0
    success_count: int = 0
    fail_count: int = 0
    health_score: float = 1.0
    cooldown_until: Optional[datetime] = None
    max_requests: int = 100
    request_interval: float = 5.0
    state: SessionState = SessionState.CREATED
    consecutive_failures: int = 0

    def is_in_cooldown(self, now: datetime) -> bool:
        return self.cooldown_until is not None and now < self.cooldown_until

    def is_ready(self, now: datetime) -> bool:
        if self.state != SessionState.AVAILABLE:
            return False
        if self.is_in_cooldown(now):
            return False
        elapsed = (now - self.last_used_time).total_seconds()
        return elapsed >= self.request_interval

    def record_borrow(self, now: datetime) -> None:
        self.request_count += 1
        self.last_used_time = now
        self.state = SessionState.BORROWED

    def record_success(self, now: datetime) -> None:
        self.success_count += 1
        self.consecutive_failures = 0
        self.state = SessionState.AVAILABLE
        self.last_used_time = now
        self._update_health_score()

    def record_failure(self, now: datetime) -> None:
        self.fail_count += 1
        self.consecutive_failures += 1
        self.state = SessionState.AVAILABLE
        self.last_used_time = now
        self._update_health_score()

    def mark_cooldown(self, seconds: int, now: datetime) -> None:
        self.cooldown_until = now + timedelta(seconds=seconds)
        self.state = SessionState.COOLDOWN

    def should_rotate(self) -> bool:
        return self.request_count >= self.max_requests

    def _update_health_score(self) -> None:
        total = self.success_count + self.fail_count
        if total <= 0:
            self.health_score = 1.0
            return

        success_rate = self.success_count / total
        fail_penalty = (self.fail_count / total) * 0.8
        streak_penalty = min(0.5, self.consecutive_failures * 0.05)
        self.health_score = max(-1.0, min(1.0, success_rate - fail_penalty - streak_penalty))
