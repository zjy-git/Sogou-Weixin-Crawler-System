from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


@dataclass(slots=True)
class SogouArticle:
    title: str
    account_name: str
    publish_time: str
    article_desc: str
    image_url: str
    sogou_url: str

    def to_dict(self) -> dict[str, str]:
        return {
            "title": self.title,
            "account_name": self.account_name,
            "publish_time": self.publish_time,
            "article_desc": self.article_desc,
            "image_url": self.image_url,
            "sogou_url": self.sogou_url,
        }


class SogouParser:
    _date_re = re.compile(r"(20\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)")
    _unix_ts_re = re.compile(r"timeConvert\(\s*['\"]?(\d{10,13})['\"]?\s*\)")

    def __init__(self, base_url: str = "https://weixin.sogou.com/") -> None:
        self.base_url = base_url

    def parse_search_results(self, html: str) -> List[SogouArticle]:
        soup = BeautifulSoup(html, "html.parser")
        article_nodes = soup.select("ul.news-list > li")
        if not article_nodes:
            article_nodes = soup.select("li")

        results: List[SogouArticle] = []
        for node in article_nodes:
            title_node = node.select_one("h3 a")
            if title_node is None:
                continue

            sogou_url = (title_node.get("href") or "").strip()
            if not sogou_url:
                continue

            title = self._clean_text(title_node.get_text(" ", strip=True))
            account_name = self._extract_account_name(node)
            publish_time = self._extract_publish_time(node)
            article_desc = self._extract_desc(node)
            image_url = self._extract_image(node)

            results.append(
                SogouArticle(
                    title=title,
                    account_name=account_name,
                    publish_time=publish_time,
                    article_desc=article_desc,
                    image_url=image_url,
                    sogou_url=urljoin(self.base_url, sogou_url),
                )
            )
        return results

    def probe_sogou_redirect(
        self,
        session: requests.Session,
        sogou_url: str,
        timeout: float = 10.0,
    ) -> Optional[str]:
        try:
            response = session.get(sogou_url, timeout=timeout, allow_redirects=False)
        except requests.RequestException:
            return None

        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("Location", "").strip()
            if location:
                return urljoin(sogou_url, location)
        return None

    def _extract_account_name(self, node: Tag) -> str:
        selectors = [
            "span.all-time-y2",
            "div.s-p span.all-time-y2",
            "div.s-p a",
            "a.account",
            "div.txt-box p.info a",
            "p.txt-info a",
            "span.account",
        ]
        for selector in selectors:
            item = node.select_one(selector)
            if item:
                text = self._clean_text(item.get_text(" ", strip=True))
                if text:
                    return text
        return ""

    def _extract_desc(self, node: Tag) -> str:
        selectors = ["p.txt-info", "p.s-p", "div.txt-box > p"]
        for selector in selectors:
            item = node.select_one(selector)
            if item:
                text = self._clean_text(item.get_text(" ", strip=True))
                if text:
                    return text
        return ""

    def _extract_image(self, node: Tag) -> str:
        image = node.select_one("img")
        if image is None:
            return ""

        src = (image.get("src") or image.get("data-src") or image.get("data-original") or "").strip()
        if not src:
            return ""
        if src.startswith("//"):
            return f"https:{src}"
        return urljoin(self.base_url, src)

    def _extract_publish_time(self, node: Tag) -> str:
        selectors = ["div.s-p span.s2", "span.s2", "span.time", "div.s-p span", "p.s-p span", "p.info span"]
        for selector in selectors:
            item = node.select_one(selector)
            if not item:
                continue

            text = self._clean_text(item.get_text(" ", strip=True))
            if text:
                date_match = self._date_re.search(text)
                if date_match:
                    return date_match.group(1)

                ts_match = self._unix_ts_re.search(text)
                if ts_match:
                    return self._format_unix_timestamp(ts_match.group(1))

            script = item.select_one("script")
            if script:
                script_text = self._clean_text(script.get_text(" ", strip=True))
                ts_match = self._unix_ts_re.search(script_text)
                if ts_match:
                    return self._format_unix_timestamp(ts_match.group(1))

        raw_html = str(node)
        date_match = self._date_re.search(raw_html)
        if date_match:
            return date_match.group(1)

        ts_match = self._unix_ts_re.search(raw_html)
        if ts_match:
            return self._format_unix_timestamp(ts_match.group(1))
        return ""

    @staticmethod
    def _format_unix_timestamp(ts_value: str) -> str:
        try:
            ts_int = int(ts_value)
            # 13-digit timestamps are milliseconds.
            if ts_int >= 10**12:
                ts_int = ts_int // 1000
            return datetime.fromtimestamp(ts_int).strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError):
            return ""

    @staticmethod
    def _clean_text(text: str) -> str:
        return " ".join(text.replace("\xa0", " ").split())
