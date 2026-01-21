"""
Standalone script to verify Playwright and the scraping service environment.

Usage:
    python playwright_env_check.py --url https://example.com --target-type div --selector id=main
    python playwright_env_check.py --url https://example.com --target-type div --selector class="content area"
    python playwright_env_check.py --url https://example.com --target-type div --selector data-foo=bar --selector _ngcontent-skd-c1=

The script:
1. Creates a lightweight ScrapingService instance with stubbed Mongo collections.
2. Attempts to extract target element(s) from the provided URL using Playwright.
3. Reports success/failure along with match stats.
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
        description="Validate Playwright installation by running a ScrapingService target scrape."
    )
    parser.add_argument(
        "--url",
        default="https://example.com",
        help="URL to scrape for the test (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30000,
        help="Playwright timeout in milliseconds (default: %(default)s).",
    )
    parser.add_argument(
        "--target-type",
        default="div",
        help="Target element tag name, e.g. div, p, h1 (default: %(default)s).",
    )
    parser.add_argument(
        "--selector",
        action="append",
        default=[],
        help=(
            "Target selector in key=value form. Repeatable. "
            "Use an empty value for presence checks, e.g. _ngcontent-skd-c1="
        ),
    )
    parser.add_argument(
        "--option",
        action="append",
        default=[],
        help="URL query option in key=value form (repeatable).",
    )
    parser.add_argument(
        "--max-matches",
        type=int,
        default=50,
        help="Maximum number of matching elements to return (default: %(default)s).",
    )
    return parser.parse_args()

def _parse_kv_list(items):
    parsed = {}
    for raw in items or []:
        if raw is None:
            continue
        s = str(raw)
        if "=" not in s:
            raise ValueError(f"Invalid key=value pair: {raw}")
        k, v = s.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid key in key=value pair: {raw}")
        # Preserve empty string for presence checks
        parsed[k] = v
    return parsed


def main():
    args = parse_args()

    print("🔍 Starting Playwright environment verification...")
    print(f"   URL: {args.url}")
    print(f"   Timeout: {args.timeout} ms")
    print(f"   Target: {args.target_type} selectors={args.selector}")
    print(f"   Options: {args.option}")

    stub_db = _StubMongoDB()
    scraping_service = ScrapingService(
        client=None,
        mongo_db=stub_db,
        vector_store_id=None,
    )

    try:
        selectors = _parse_kv_list(args.selector)
        options = _parse_kv_list(args.option)
        target = {"type": args.target_type, "selectors": selectors}
        matches = scraping_service.scrape_target_elements(
            args.url,
            options=options or None,
            target=target,
            timeout_ms=args.timeout,
            max_matches=args.max_matches,
        )
    except Exception as e:
        print("❌ Playwright or target scrape test failed.")
        print(f"   Error: {e}")
        sys.exit(1)

    if not matches:
        print("❌ Target scrape returned no matches.")
        sys.exit(1)

    first = matches[0]
    extracted_info = first.get("extracted_information") or {}
    print("✅ Playwright successfully scraped target element(s).")
    print(f"   Matches: {len(matches)}")
    print(f"   First text length: {len((first.get('text') or ''))} characters")
    print(f"   Extracted information keys: {len(extracted_info)}")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Verification interrupted by user.")
        sys.exit(130)

