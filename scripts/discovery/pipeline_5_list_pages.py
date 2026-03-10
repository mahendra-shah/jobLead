"""
Pipeline 5 — List Pages Discovery.

Use curated "list of job boards / job sites" pages as seeds, extract outbound links
that look like job sources, and write them into discovery_sources_test.json.

This is still Phase 1 (discovery only).

Usage:
  cd jobLead
  source venv/bin/activate
  venv/bin/python scripts/discovery/pipeline_5_list_pages.py --delay 5
"""
import argparse
import sys
from pathlib import Path
from urllib.parse import urljoin

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

import httpx
from bs4 import BeautifulSoup

from scripts.discovery.base import (
    DATA_DIR,
    DISCOVERY_JSON_PATH,
    load_discovery_sources_json,
    save_discovery_sources_json,
)
from scripts.discovery.domain_rate_limiter import rate_limit_before_request
from scripts.discovery.proxy_pool import get_next_proxy

REQUEST_TIMEOUT = 25.0
HEADERS = {
    "User-Agent": "PlacementDiscovery/1.0 (ListPages)",
    "Accept": "text/html,application/xhtml+xml",
}


def load_seed_pages() -> list[str]:
    path = DATA_DIR / "seed_job_list_pages.json"
    if not path.exists():
        return []
    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("seeds") or []


def is_candidate_job_site(url: str) -> bool:
    """Heuristic filter for job boards / job sites from list pages."""
    u = url.lower()
    # Skip obvious non-sources
    skip_fragments = ["twitter.com", "facebook.com", "instagram.com", "youtube.com", "linkedin.com/share", "#"]
    if any(s in u for s in skip_fragments):
        return False
    # Require at least one job-related token
    positive = ["job", "jobs", "career", "careers", "hiring", "work", "remote"]
    if not any(tok in u for tok in positive):
        return False
    return True


def extract_links_from_list_page(base_url: str, html: str) -> list[str]:
    """Extract outbound links that look like job sources."""
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        full = urljoin(base_url, href)
        if not full.startswith(("http://", "https://")):
            continue
        if full in seen:
            continue
        seen.add(full)
        if is_candidate_job_site(full):
            out.append(full)
    return out


def run(delay: float, dry_run: bool, out_path: Path) -> tuple[int, int]:
    seeds = load_seed_pages()
    if not seeds:
        print("No seeds in app/data/seed_job_list_pages.json")
        return 0, 0

    sources = load_discovery_sources_json(out_path)
    inserted, skipped = 0, 0

    proxies = get_next_proxy()
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, proxies=proxies) as client:
        for i, url in enumerate(seeds, 1):
            print(f"[{i}/{len(seeds)}] {url} ... ", end="", flush=True)
            try:
                rate_limit_before_request(url)
                resp = client.get(url, headers=HEADERS)
                resp.raise_for_status()
            except Exception as e:
                print(f"FAILED: {e}")
                continue

            links = extract_links_from_list_page(url, resp.text)
            print(f"{len(links)} candidate links")
            for link in links:
                if dry_run:
                    inserted += 1
                    continue
                from scripts.discovery.base import upsert_discovery_source_into_list

                if upsert_discovery_source_into_list(
                    sources,
                    link,
                    name="",
                    source_type=None,
                    discovery_origin="seed_list",
                ):
                    inserted += 1
                else:
                    skipped += 1

    if not dry_run:
        save_discovery_sources_json(sources, out_path)
    return inserted, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline 5: List Pages Discovery")
    parser.add_argument("--delay", type=float, default=5.0, help="Seconds between seed page requests (domain limiter still applies)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output")
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path")
    args = parser.parse_args()

    out_path = DISCOVERY_JSON_PATH if args.out is None else (args.out if args.out.is_absolute() else JOBLEAD_ROOT / args.out)

    ins, sk = run(args.delay, args.dry_run, out_path)
    print(f"Inserted: {ins}, Skipped: {sk}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

