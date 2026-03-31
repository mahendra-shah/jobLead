"""
Dynamic Website Analyzer — visit discovered URL, analyze structure, generate crawl strategy.
Run after discovery, before Phase-2. Phase-2 crawler uses metadata.crawl_strategy.

Analyzes:
  - robots.txt (Disallow, Sitemap)
  - HTML: job links (/jobs, /careers, /openings, ...), sitemap link, pagination
  - Builds crawl_strategy: entry_urls, sitemap_url, robots_disallow, pagination_type, crawl_ready

Usage:
  python scripts/discovery/analyze_source.py --max 20 --delay 3
  python scripts/discovery/analyze_source.py --dedup-first --dedup-only
"""
import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

import httpx
from bs4 import BeautifulSoup

from scripts.discovery.base import (
    DISCOVERY_JSON_PATH,
    JOBLEAD_ROOT,
    iso_now,
    load_discovery_sources_json,
    save_discovery_sources_json,
    rate_limit_sleep,
    dedup_sources,
    dedup_sources_in_file,
)
from scripts.discovery.domain_rate_limiter import rate_limit_before_request
from scripts.discovery.proxy_pool import get_next_proxy

REQUEST_TIMEOUT = 15.0
FETCH_HEADERS = {
    "User-Agent": "PlacementDiscovery/1.0 (Source Analyzer)",
    "Accept": "text/html,application/xhtml+xml",
}

# Path segments that indicate job listing pages
JOB_PATH_PATTERN = re.compile(
    r"/(jobs?|careers?|openings?|positions?|vacancies?|internships?)(/|$)",
    re.I,
)
SITEMAP_PATTERN = re.compile(r"sitemap|sitemap_index|sitemap\.xml", re.I)
# Pagination: next link text or href
PAGINATION_NEXT = re.compile(r"next|›|»|page\s*\d+|older", re.I)
PAGINATION_PAGE_QUERY = re.compile(r"[?&]page=\d+", re.I)
PAGINATION_PAGE_PATH = re.compile(r"/page/\d+|/p/\d+", re.I)


def find_job_page_urls(base_url: str, html: str) -> list[str]:
    """Extract absolute URLs from page that look like job listing paths."""
    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    out = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        full = urljoin(base_url, href)
        path = urlparse(full).path or "/"
        if JOB_PATH_PATTERN.search(path):
            if full not in seen:
                seen.add(full)
                out.append(full)
    return out[:10]  # cap


def find_sitemap_url(base_url: str, html: str) -> str | None:
    """Find sitemap URL from link rel or common paths."""
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("link", href=True):
        href = link.get("href", "")
        rel = (link.get("rel") or [])
        if isinstance(rel, str):
            rel = [rel]
        if SITEMAP_PATTERN.search(href) or any(SITEMAP_PATTERN.search(r) for r in rel):
            return urljoin(base_url, href)
    for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"]:
        candidate = urljoin(base_url, path)
        if candidate not in (base_url, base_url + "/"):
            return candidate
    return None


def fetch_robots_txt(origin: str, client: httpx.Client) -> str | None:
    """Fetch robots.txt for origin. Returns body or None."""
    robots_url = f"{origin.rstrip('/')}/robots.txt"
    try:
        r = client.get(robots_url, headers=FETCH_HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass
    return None


def parse_robots_txt(text: str) -> dict:
    """
    Parse robots.txt. Returns:
      disallow_paths: list of path prefixes to avoid (e.g. ["/api/", "/admin"])
      sitemap_urls: list of Sitemap: URLs
    """
    disallow_paths = []
    sitemap_urls = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key, value = key.strip().lower(), value.strip()
        if key == "disallow" and value:
            disallow_paths.append(value)
        elif key == "sitemap" and value:
            sitemap_urls.append(value)
    return {"disallow_paths": disallow_paths, "sitemap_urls": sitemap_urls}


def find_pagination(base_url: str, html: str) -> dict:
    """
    Detect pagination: next-link, query (?page=), or path (/page/2).
    Returns: { type: "next_link"|"query"|"path"|None, sample_url: str|None }
    """
    soup = BeautifulSoup(html, "html.parser")
    sample_url = None
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        text = (a.get_text() or "").strip()
        if PAGINATION_NEXT.search(text) or PAGINATION_NEXT.search(href):
            full = urljoin(base_url, href)
            if urlparse(full).netloc == urlparse(base_url).netloc:
                sample_url = full
                return {"type": "next_link", "sample_url": sample_url}
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        full = urljoin(base_url, href)
        if PAGINATION_PAGE_QUERY.search(full):
            return {"type": "query", "sample_url": full}
        if PAGINATION_PAGE_PATH.search(full):
            return {"type": "path", "sample_url": full}
    return {"type": None, "sample_url": None}


def build_crawl_strategy(
    job_page_urls: list[str],
    sitemap_url: str | None,
    robots: dict,
    pagination: dict,
    status_code: int | None,
) -> dict:
    """
    Generate crawl strategy for Phase-2.
    entry_urls: where to start (job listing pages or homepage fallback)
    sitemap_url: from HTML or robots; crawler can use for discovery
    robots_disallow: paths to avoid
    pagination_type: how to follow pages
    crawl_ready: True if we have at least one entry and no hard block
    """
    entry_urls = job_page_urls[:5] if job_page_urls else []
    disallow = (robots.get("disallow_paths") or [])[:20]
    sitemaps = robots.get("sitemap_urls") or []
    sitemap = sitemap_url or (sitemaps[0] if sitemaps else None)
    pagination_type = pagination.get("type")
    blocked = status_code in (403, 401, 429)
    crawl_ready = bool(entry_urls or sitemap) and not blocked
    return {
        "entry_urls": entry_urls,
        "sitemap_url": sitemap,
        "robots_disallow": disallow,
        "pagination_type": pagination_type,
        "pagination_sample_url": pagination.get("sample_url"),
        "crawl_ready": crawl_ready,
    }


def analyze_one(url: str, simulation: bool) -> dict:
    """
    Fetch URL + robots.txt, parse HTML and robots, detect job pages/sitemap/pagination.
    Returns result with crawl_strategy for Phase-2.
    """
    result = {
        "analyzer_status": "error",
        "analyzer_checked_at": iso_now(),
        "status_code": None,
        "job_page_detected": False,
        "job_page_urls": [],
        "sitemap_url": None,
        "robots_disallow_paths": [],
        "robots_sitemap_urls": [],
        "pagination_type": None,
        "pagination_sample_url": None,
        "crawl_strategy": None,
        "error": None,
    }
    if simulation:
        result["analyzer_status"] = "simulation"
        return result
    try:
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        proxies = get_next_proxy()
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, proxies=proxies) as client:
            # 1) robots.txt
            robots_text = fetch_robots_txt(origin, client)
            robots = parse_robots_txt(robots_text) if robots_text else {}
            result["robots_disallow_paths"] = robots.get("disallow_paths", [])
            result["robots_sitemap_urls"] = robots.get("sitemap_urls", [])

            # 2) Main page
            rate_limit_before_request(url)
            resp = client.get(url, headers=FETCH_HEADERS)
            result["status_code"] = resp.status_code
            if resp.status_code >= 400:
                result["analyzer_status"] = "blocked" if resp.status_code in (403, 429) else "error"
                result["error"] = f"HTTP {resp.status_code}"
                result["crawl_strategy"] = build_crawl_strategy(
                    [], None, robots, {"type": None, "sample_url": None}, resp.status_code
                )
                return result
            result["analyzer_status"] = "ok"
            html = resp.text

            job_urls = find_job_page_urls(url, html)
            if job_urls:
                result["job_page_detected"] = True
                result["job_page_urls"] = job_urls
            sitemap = find_sitemap_url(url, html)
            if sitemap:
                result["sitemap_url"] = sitemap
            if not sitemap and robots.get("sitemap_urls"):
                result["sitemap_url"] = robots["sitemap_urls"][0]

            pagination = find_pagination(url, html)
            result["pagination_type"] = pagination.get("type")
            result["pagination_sample_url"] = pagination.get("sample_url")

            result["crawl_strategy"] = build_crawl_strategy(
                result["job_page_urls"],
                result["sitemap_url"],
                robots,
                pagination,
                result["status_code"],
            )
    except httpx.TimeoutException:
        result["analyzer_status"] = "timeout"
        result["error"] = "timeout"
    except Exception as e:
        result["analyzer_status"] = "error"
        result["error"] = str(e)[:200]
    return result


def run(
    simulation: bool,
    delay_sec: float,
    max_sources: int | None,
    in_path: Path,
    dedup_first: bool,
) -> tuple[int, int]:
    sources = load_discovery_sources_json(in_path)
    if dedup_first:
        sources = dedup_sources(sources, keep="highest_score")
        save_discovery_sources_json(sources, in_path)
        print("Dedup done; re-loaded for analysis.")

    # Prefer un-analyzed or web sources
    to_analyze = [s for s in sources if not (s.get("metadata") or {}).get("analyzer_checked_at")]
    if not to_analyze:
        to_analyze = [s for s in sources if (s.get("type") or "").lower() in ("website", "job_board", "company_career")]
    if not to_analyze:
        to_analyze = sources
    to_analyze = to_analyze[: (max_sources or len(to_analyze))]

    updated = 0
    for i, s in enumerate(to_analyze, 1):
        url = (s.get("url") or "").strip()
        if not url:
            continue
        print(f"[{i}/{len(to_analyze)}] {url[:60]}... ", end="", flush=True)
        rate_limit_sleep(delay_sec, simulation=simulation)
        result = analyze_one(url, simulation)
        meta = dict(s.get("metadata") or {})
        meta["analyzer_status"] = result["analyzer_status"]
        meta["analyzer_checked_at"] = result["analyzer_checked_at"]
        if result.get("status_code") is not None:
            meta["status_code"] = result["status_code"]
        if result.get("job_page_detected"):
            meta["job_page_detected"] = True
            meta["job_page_urls"] = result.get("job_page_urls") or []
        if result.get("sitemap_url"):
            meta["sitemap_url"] = result["sitemap_url"]
        if result.get("robots_disallow_paths"):
            meta["robots_disallow_paths"] = result["robots_disallow_paths"]
        if result.get("robots_sitemap_urls"):
            meta["robots_sitemap_urls"] = result["robots_sitemap_urls"]
        if result.get("pagination_type"):
            meta["pagination_type"] = result["pagination_type"]
            if result.get("pagination_sample_url"):
                meta["pagination_sample_url"] = result["pagination_sample_url"]
        if result.get("crawl_strategy"):
            meta["crawl_strategy"] = result["crawl_strategy"]
        if result.get("error"):
            meta["analyzer_error"] = result["error"]
        s["metadata"] = meta
        s["last_checked"] = result["analyzer_checked_at"]
        updated += 1
        print(result["analyzer_status"], end="")
        if result.get("job_page_detected"):
            print(f" job_pages={len(result.get('job_page_urls', []))}", end="")
        strategy = result.get("crawl_strategy") or {}
        if strategy.get("crawl_ready"):
            print(" crawl_ready", end="")
        print()

    save_discovery_sources_json(sources, in_path)
    return updated, len(to_analyze)


def main():
    parser = argparse.ArgumentParser(description="Analyze discovered sources: detect job pages, sitemap")
    parser.add_argument("--simulation", action="store_true", help="Do not fetch URLs")
    parser.add_argument("--delay", type=float, default=3.0, help="Seconds between requests")
    parser.add_argument("--max", type=int, default=None, help="Max sources to analyze")
    parser.add_argument("--file", type=Path, default=None, help="JSON path (default: discovery_sources_test.json)")
    parser.add_argument("--dedup-first", action="store_true", help="Run URL dedup before analyzing")
    parser.add_argument("--dedup-only", action="store_true", help="Only dedup JSON by normalized URL, then exit")
    args = parser.parse_args()
    path = (JOBLEAD_ROOT / args.file) if args.file else DISCOVERY_JSON_PATH
    if args.file and not str(args.file).startswith("/"):
        path = JOBLEAD_ROOT / args.file

    if not path.exists():
        print(f"File not found: {path}")
        return 1

    if args.dedup_only:
        before = len(load_discovery_sources_json(path))
        after = dedup_sources_in_file(path, keep="highest_score")
        print(f"Dedup: {before} → {after} sources")
        return 0

    updated, total = run(args.simulation, args.delay, args.max, path, args.dedup_first)
    print(f"Updated: {updated}/{total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
