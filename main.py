from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import List

from crawler.sogou_spider import SogouSpider, SpiderConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sogou Weixin Search crawler")
    parser.add_argument('--keyword', '-k', type=str, default="AI", help='Search keyword')
    parser.add_argument("--pages", type=int, default=1, help="How many pages to crawl")
    parser.add_argument("--workers", type=int, default=2, help="Worker thread count")
    parser.add_argument("--session-pool-size", type=int, default=6, help="Session pool size")
    parser.add_argument("--rate-limit", type=float, default=10, help="Global req/sec")
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Retry times for retryable failures",
    )
    # parser.add_argument(
    #     "--output",
    #     default="sogou_results.json",
    #     help="Output JSON file path",
    # )
    parser.add_argument(
        "--proxies",
        default="",
        help="Optional proxy list, comma separated (e.g. http://ip1:port,http://ip2:port)",
    )
    return parser.parse_args()


def parse_proxies(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    args = parse_args()

    config = SpiderConfig(
        session_pool_size=args.session_pool_size,
        worker_count=args.workers,
        global_rate_limit=args.rate_limit,
        max_retries=args.max_retries,
        proxies=parse_proxies(args.proxies),
    )

    with SogouSpider(config) as spider:
        results = spider.crawl(keyword=args.keyword, pages=args.pages)

    # output_path = Path(args.output).resolve()
    # output_path.write_text(
    #     json.dumps(results, ensure_ascii=False, indent=2),
    #     encoding="utf-8",
    # )

    print(f"Keyword: {args.keyword}")
    print(f"Pages: {args.pages}")
    print(f"Total records: {len(results)}")
    # print(f"Saved to: {output_path}")
    if results:
        for item in results:
            print("---")
            print(f"标题：{item['title']}")
            print(f"公众号：{item['account_name']}")
            print(f"发布时间：{item['publish_time']}")
            print(f"内容简介：{item['article_desc']}")
            print(f"检索配图：{item['image_url']}")
            print(f"跳转链接：{item['sogou_url']}")


if __name__ == "__main__":
    main()
