"""
Standalone script to verify Playwright and the scraping service environment.

Usage:
    python playwright_env_check.py --url https://example.com

The script:
1. Creates a lightweight ScrapingService instance with stubbed Mongo collections.
2. Attempts to scrape the provided URL using Playwright.
3. Reports success/failure along with content stats and title.
"""

import argparse
import sys
from dataclasses import dataclass, field

from scraping_service import ScrapingService


@dataclass
class _StubCollection:
    """Minimal stub collection that satisfies ScrapingService initialization."""

    name: str
    indexes: list = field(default_factory=list)

    def create_index(self, *args, **kwargs):
        self.indexes.append((args, kwargs))


class _StubMongoDB:
    """Stub Mongo database that returns stub collections."""

    def __init__(self):
        self._collections = {}

    def get_collection(self, name: str):
        if name not in self._collections:
            self._collections[name] = _StubCollection(name=name)
        return self._collections[name]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate Playwright installation by running a ScrapingService test scrape."
    )
    parser.add_argument(
        "--url",
        default="https://example.com",
        help="URL to scrape for the test (default: %(default)s)",
    )
    parser.add_argument(
        "--dynamic",
        action="store_true",
        help="Enable dynamic content loading (uses Playwright's networkidle wait).",
    )
    parser.add_argument(
        "--merge-dynamic",
        action="store_true",
        help="Run the hybrid dynamic merge pass (slower, but thorough).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30000,
        help="Playwright navigation timeout in milliseconds (default: %(default)s).",
    )
    parser.add_argument(
        "--expand-accordions",
        action="store_true",
        help="Attempt to expand accordions/tabs while scraping.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("🔍 Starting Playwright environment verification...")
    print(f"   URL: {args.url}")
    print(f"   Dynamic content: {args.dynamic}")
    print(f"   Merge dynamic: {args.merge_dynamic}")
    print(f"   Expand accordions: {args.expand_accordions}")
    print(f"   Timeout: {args.timeout} ms")

    stub_db = _StubMongoDB()
    scraping_service = ScrapingService(
        client=None,
        mongo_db=stub_db,
        vector_store_id=None,
    )

    content, title, error, html_content, file_links = scraping_service.scrape_url(
        args.url,
        expand_accordions=args.expand_accordions,
        timeout=args.timeout,
        extract_files=False,
        load_dynamic_content=args.dynamic,
        merge_dynamic_content=args.merge_dynamic,
    )

    if error:
        print("❌ Playwright or scraping test failed.")
        print(f"   Error: {error}")
        sys.exit(1)

    print("✅ Playwright successfully scraped the page.")
    print(f"   Title: {title}")
    print(f"   Content length: {len(content)} characters")
    print(f"   HTML length: {len(html_content or '')} characters")
    print("   No file extraction performed in this test.")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Verification interrupted by user.")
        sys.exit(130)

