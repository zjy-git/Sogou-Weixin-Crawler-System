from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import requests


class DetectionStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"


class RequestResultType(str, Enum):
    NETWORK_ERROR = "NETWORK_ERROR"
    SERVER_ERROR = "SERVER_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    ANTISPIDER = "ANTISPIDER"
    RATE_LIMIT = "RATE_LIMIT"
    SUCCESS = "SUCCESS"


@dataclass(slots=True)
class DetectionResult:
    status: DetectionStatus
    result_type: RequestResultType
    reason: str = ""


class AntiSpiderDetector:
    CAPTCHA_KEYWORDS = [
        "验证码",
        "请输入验证码",
        "security verification",
        "captcha",
    ]
    BLOCKED_KEYWORDS = [
        "antispider",
        "访问过于频繁",
        "异常访问",
        "请求过于频繁",
        "forbidden",
    ]

    def detect(self, response: requests.Response) -> DetectionResult:
        status_code = response.status_code
        location = response.headers.get("Location", "").lower()
        text = response.text or ""
        text_lower = text.lower()

        if status_code == 429:
            return DetectionResult(
                status=DetectionStatus.BLOCKED,
                result_type=RequestResultType.RATE_LIMIT,
                reason="HTTP 429",
            )

        if status_code == 403:
            return DetectionResult(
                status=DetectionStatus.BLOCKED,
                result_type=RequestResultType.ANTISPIDER,
                reason="HTTP 403",
            )

        if status_code in (301, 302, 303, 307, 308) and "antispider" in location:
            return DetectionResult(
                status=DetectionStatus.BLOCKED,
                result_type=RequestResultType.ANTISPIDER,
                reason="redirect to anti-spider",
            )

        if any(keyword in text for keyword in self.CAPTCHA_KEYWORDS):
            return DetectionResult(
                status=DetectionStatus.BLOCKED,
                result_type=RequestResultType.ANTISPIDER,
                reason="captcha detected",
            )

        if any(keyword in text_lower for keyword in self.BLOCKED_KEYWORDS):
            return DetectionResult(
                status=DetectionStatus.BLOCKED,
                result_type=RequestResultType.ANTISPIDER,
                reason="blocked keyword detected",
            )

        if status_code >= 500:
            return DetectionResult(
                status=DetectionStatus.FAIL,
                result_type=RequestResultType.SERVER_ERROR,
                reason=f"HTTP {status_code}",
            )

        if status_code >= 400:
            return DetectionResult(
                status=DetectionStatus.FAIL,
                result_type=RequestResultType.SERVER_ERROR,
                reason=f"HTTP {status_code}",
            )

        if len(text.strip()) < 300:
            return DetectionResult(
                status=DetectionStatus.FAIL,
                result_type=RequestResultType.PARSE_ERROR,
                reason="abnormal short html",
            )

        return DetectionResult(
            status=DetectionStatus.SUCCESS,
            result_type=RequestResultType.SUCCESS,
            reason="ok",
        )
