from __future__ import annotations

import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import requests

from proxy_pool.proxy_pool import ProxyPool
from session_pool.session_client import SessionClient, SessionState
from utils.headers_profiles import random_headers_profile


class SessionFactory:
    def __init__(
        self,
        proxy_pool: Optional[ProxyPool] = None,
        request_interval_range: tuple[float, float] = (1.0, 1.5),  # (3.0, 8.0)
        max_requests_range: tuple[int, int] = (80, 120),
        warmup_timeout: float = 8.0,
    ) -> None:
        self.proxy_pool = proxy_pool
        self.request_interval_range = request_interval_range
        self.max_requests_range = max_requests_range
        self.warmup_timeout = warmup_timeout

    def create_session(self) -> SessionClient:
        session = requests.Session()
        session_id = uuid.uuid4().hex

        user_agent, headers_profile = random_headers_profile()
        session.headers.update(headers_profile)

        proxy = self.proxy_pool.acquire_proxy(session_id) if self.proxy_pool else None
        if proxy:
            session.proxies.update({"http": proxy, "https": proxy})

        request_interval = random.uniform(*self.request_interval_range)
        max_requests = random.randint(*self.max_requests_range)
        now = datetime.utcnow()

        session_client = SessionClient(
            session=session,
            session_id=session_id,
            proxy=proxy,
            user_agent=user_agent,
            headers_profile=headers_profile,
            cookies=session.cookies,
            create_time=now,
            last_used_time=now - timedelta(seconds=request_interval),
            request_count=0,
            success_count=0,
            fail_count=0,
            health_score=1.0,
            cooldown_until=None,
            max_requests=max_requests,
            request_interval=request_interval,
            state=SessionState.AVAILABLE,
        )

        self._warmup_session(session_client)
        return session_client

    def destroy_session(self, session_client: SessionClient) -> None:
        session_client.state = SessionState.DESTROYED
        if self.proxy_pool:
            self.proxy_pool.release_proxy(session_client.session_id)
        session_client.session.close()

    def _warmup_session(self, session_client: SessionClient) -> None:
        warmup_urls = [
            "https://weixin.sogou.com/",
            "https://weixin.sogou.com/weixin?type=2&query=test",
        ]
        for url in warmup_urls:
            try:
                session_client.session.get(url, timeout=self.warmup_timeout, allow_redirects=True)
                time.sleep(random.uniform(0.2, 0.8))
            except requests.RequestException:
                break
