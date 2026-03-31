"""
Scrape job data from shortlisted discovery sources using BeautifulSoup.

- Fetches sources from GET /api/v1/discovery/sources/shortlisted
- For testing: uses first 20 web sources only
- Fetches each URL and extracts job links/titles with generic + site-specific selectors
- Cleans/filters junk nav links before saving
- Outputs to JSON (default: app/data/scraped_jobs_from_sources.json)

Usage:
  cd jobLead && python scripts/scrape_jobs_from_shortlisted_sources.py
  python scripts/scrape_jobs_from_shortlisted_sources.py --api-base http://localhost:8000 --limit 20 --out app/data/jobs_test.json
"""

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import List
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# ── Project root ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

DEFAULT_API_BASE = "http://localhost:8000"
DEFAULT_OUT = "app/data/scraped_jobs_from_sources.json"
REQUEST_TIMEOUT = 25.0
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Site-specific homepage → jobs-page remapping ──────────────────────────────
SITE_JOBS_PATH_MAP = [
    (r"builtin\w+\.in",     "/jobs"),
    (r"builtin\.com",       "/jobs"),
    (r"techgig\.com",       "/jobs"),
    (r"internshala\.com",   "/jobs/fresher-jobs"),
    (r"naukri\.com",        "/jobs"),
    (r"indeed\.co\.in",     "/jobs"),
    (r"in\.indeed\.com",    "/jobs"),
    (r"linkedin\.com",      "/jobs/search/?location=India"),
    (r"shine\.com",         "/job-search/jobs-in-india"),
    (r"foundit\.in",        "/jobs"),
    (r"timesjobs\.com",     "/jobs/jobs-in-india"),
    (r"freshersworld\.com", "/jobs/freshers-jobs"),
    (r"hackerearth\.com",   "/jobs"),
    (r"wellfound\.com",     "/jobs"),
    (r"angel\.co",          "/jobs"),
    (r"cutshort\.io",       "/jobs"),
    (r"instahyre\.com",     "/jobs"),
    (r"geeksforgeeks\.org", "/jobs"),
    (r"iimjobs\.com",       "/jobs"),
]

# ── Path filters ──────────────────────────────────────────────────────────────
SKIP_PATH_FRAGMENTS = (
    "/login", "/signup", "/register",
    "/category/", "/tag/", "/author/",
    "/about", "/contact", "/privacy", "/terms",
    "/employer", "/hire", "/post-a-job",
    "/articles/", "/blog/", "/news/",
    "/auth/", "/oauth",
)

JOB_PATH_INDICATORS = (
    "/job/", "/jobs/", "/career/", "/careers/",
    "/opening/", "/openings/",
    "/position/", "/positions/",
    "/vacancy/", "/vacancies/",
    "/apply/", "/listing/", "/listings/",
    "/internship/", "/internships/",
)

# ── Junk title blacklist ──────────────────────────────────────────────────────
JUNK_TITLES = {
    "remote", "careers", "all cities", "all designation",
    "we're hiring", "jobs in delhi", "jobs in mumbai",
    "jobs in chennai", "jobs in bangalore", "jobs in noida",
    "jobs in hyderabad", "jobs in gurgaon", "jobs in kolkata",
    "jobs in pune", "jobs in ahmedabad", "jobs in bengaluru",
    "view jobs", "view profile", "see all", "load more",
    "be part of our talent community",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_jobs_url(source_url: str) -> str:
    """Remap bare homepages to dedicated jobs pages."""
    parsed = urlparse(source_url)
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if path in ("", "/", "/jobs", "/careers"):
        for pattern, jobs_path in SITE_JOBS_PATH_MAP:
            if re.search(pattern, netloc):
                return f"{parsed.scheme}://{parsed.netloc}{jobs_path}"
    return source_url


def is_job_like_path(path: str) -> bool:
    if not path or path.strip() in ("/", "#"):
        return False
    path_lower = path.lower()
    for skip in SKIP_PATH_FRAGMENTS:
        if skip in path_lower:
            return False
    return any(ind in path_lower for ind in JOB_PATH_INDICATORS)


def normalize_url(url: str) -> str:
    """Strip query/fragment, lowercase, remove trailing slash — for dedup."""
    return url.split("?")[0].split("#")[0].rstrip("/").lower()


def is_real_job(job: dict) -> bool:
    """
    Post-scrape quality filter.
    Removes nav links, category pages, and other junk that slips through.
    """
    title = job.get("title", "").strip()
    url   = job.get("url", "")

    # Too short / single word titles are nav items
    if len(title.split()) <= 1:
        return False

    # Blacklisted titles (exact match, case-insensitive)
    if title.lower() in JUNK_TITLES:
        return False

    # TechGig: reject city/skill filter pages e.g. /jobs/city/Ahmedabad/Java
    if "techgig.com/jobs/city/" in url:
        parts = [p for p in urlparse(url).path.split("/") if p]
        # /jobs/city/CityName/Keyword → 4 parts → nav link
        if len(parts) >= 4 and parts[1] == "city":
            return False

    # TechGig designation index/filter pages
    if re.search(r"techgig\.com/jobs/designation", url):
        parts = [p for p in urlparse(url).path.split("/") if p]
        if len(parts) <= 3:
            return False

    # TechGig city index pages e.g. /jobs/city/Bangalore-Jobs
    if re.search(r"techgig\.com/jobs/city/[^/]+-Jobs$", url):
        return False

    # Reject cross-domain employer/careers pages
    if "employers.builtin.com" in url:
        return False

    # Must still look like a job path
    parsed_path = urlparse(url).path
    if not is_job_like_path(parsed_path):
        return False

    return True


# ── HTML extractor ────────────────────────────────────────────────────────────

def extract_jobs_from_html(
    html: str,
    page_url: str,
    source_name: str,
    source_id: str,
) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen_hashes: set = set()
    jobs: List[dict] = []

    def add(title: str, href: str, location: str = "", company: str = "") -> None:
        if not href or not title:
            return
        title = title.strip()
        if len(title) < 3 or len(title) > 500:
            return
        full_url = urljoin(page_url, href)
        parsed_path = urlparse(full_url).path
        if not is_job_like_path(parsed_path):
            return
        norm = normalize_url(full_url)
        h = hashlib.md5(norm.encode()).hexdigest()
        if h in seen_hashes:
            return
        seen_hashes.add(h)
        entry: dict = {
            "title": title[:500],
            "url": full_url,
            "source_name": source_name,
            "source_id": source_id,
        }
        if location:
            entry["location"] = location.strip()
        if company:
            entry["company"] = company.strip()
        jobs.append(entry)

    # 1) Schema.org JobPosting microdata
    for node in soup.find_all(attrs={"itemtype": re.compile(r"JobPosting", re.I)}):
        link = node.find("a", href=True)
        title_node = (
            node.find(attrs={"itemprop": "title"})
            or node.find(attrs={"itemprop": "name"})
            or node.find(["h2", "h3", "h4"])
        )
        if link and title_node:
            add(title_node.get_text(separator=" ", strip=True), link["href"])

    # 2) Built In network cards
    for card in soup.select("div[class*='job-card'], li[class*='job-card']"):
        link = card.find("a", href=True)
        title_node = card.find(["h2", "h3", "h4", "span"])
        if link and title_node:
            add(title_node.get_text(separator=" ", strip=True), link["href"])

    # 3) TechGig cards
    for card in soup.select("li.job_listing, div.job-list-box, div[class*='list-job']"):
        link = card.find("a", href=True)
        title_node = card.find(["h2", "h3", "h4", "a"])
        if link and title_node:
            add(title_node.get_text(separator=" ", strip=True), link["href"])

    # 4) Generic job card patterns
    generic_selectors = [
        "[class*='job-card']",
        "[class*='job-item']",
        "[class*='job-listing']",
        "[class*='job-tile']",
        "[class*='job-result']",
        "[class*='listing-item']",
        "[class*='search-result']",
        "[data-job-id]",
        "[data-jk]",
        "article",
    ]
    for sel in generic_selectors:
        try:
            for card in soup.select(sel):
                link = card.find("a", href=True)
                title_node = card.find(["h2", "h3", "h4"]) or link
                if link and title_node:
                    company_node = card.find(class_=re.compile(r"company|employer|org", re.I))
                    loc_node     = card.find(class_=re.compile(r"location|loc|city", re.I))
                    add(
                        title_node.get_text(separator=" ", strip=True),
                        link["href"],
                        location=loc_node.get_text(strip=True)    if loc_node     else "",
                        company =company_node.get_text(strip=True) if company_node else "",
                    )
        except Exception:
            continue

    # 5) Direct <a> scan — href contains job keywords
    job_href_re = re.compile(
        r"/(job|jobs|career|careers|opening|openings|position|positions"
        r"|vacancy|vacancies|internship|listing)/",
        re.I,
    )
    for a in soup.find_all("a", href=True):
        if job_href_re.search(a["href"]):
            title = a.get_text(separator=" ", strip=True)
            if 5 <= len(title) <= 400:
                add(title, a["href"])

    return jobs


# ── Per-source scraper (with pagination) ─────────────────────────────────────

def scrape_source(
    client: httpx.Client,
    source: dict,
    limit_per_source: int = 100,
) -> List[dict]:
    raw_url = source.get("url") or ""
    name    = source.get("name") or "Unknown"
    sid     = str(source.get("id") or "")

    if not raw_url:
        return []

    url = get_jobs_url(raw_url)
    all_jobs: List[dict] = []
    visited: set = set()
    page_url = url

    for _ in range(1, 6):   # up to 5 pages
        if page_url in visited:
            break
        visited.add(page_url)

        try:
            resp = client.get(page_url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            break

        jobs = extract_jobs_from_html(resp.text, page_url, name, sid)

        existing_urls = {j["url"] for j in all_jobs}
        new_jobs = [j for j in jobs if j["url"] not in existing_urls]
        all_jobs.extend(new_jobs)

        if len(all_jobs) >= limit_per_source:
            break

        # Follow "next page" link if present
        soup = BeautifulSoup(resp.text, "html.parser")
        next_link = (
            soup.find("a", string=re.compile(r"next|›|»|load more", re.I))
            or soup.find("a", attrs={"aria-label": re.compile(r"next", re.I)})
            or soup.find("a", rel=re.compile(r"next", re.I))
        )
        if next_link and next_link.get("href"):
            next_url = urljoin(page_url, next_link["href"])
            if next_url != page_url and next_url not in visited:
                page_url = next_url
                time.sleep(0.5)
            else:
                break
        else:
            break

    return all_jobs[:limit_per_source]


# ── API helper ────────────────────────────────────────────────────────────────

def fetch_shortlisted_sources(api_base: str, page: int = 1, page_size: int = 50) -> list:
    url    = f"{api_base.rstrip('/')}/api/v1/discovery/sources/shortlisted"
    params = {"page": page, "page_size": page_size}
    with httpx.Client(timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"}) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    return data.get("sources") or []


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape jobs from shortlisted discovery sources (BeautifulSoup)"
    )
    parser.add_argument("--api-base",   default=DEFAULT_API_BASE)
    parser.add_argument("--limit",      type=int, default=20,  help="Max sources to scrape")
    parser.add_argument("--out",        default=DEFAULT_OUT)
    parser.add_argument("--per-source", type=int, default=100, help="Max jobs per source")
    args = parser.parse_args()

    out_path = JOBLEAD_ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sources     = fetch_shortlisted_sources(args.api_base)
    web_sources = [s for s in sources if (s.get("platform") or "").lower() == "web"]
    to_scrape   = web_sources[: args.limit]

    print(f"Shortlisted sources: {len(sources)}, web: {len(web_sources)}, scraping first {len(to_scrape)}")

    raw_jobs: List[dict] = []
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        for i, src in enumerate(to_scrape, 1):
            name       = src.get("name") or src.get("url") or "?"
            raw_url    = src.get("url") or ""
            remapped   = get_jobs_url(raw_url)
            remap_note = f" (→ {remapped})" if remapped != raw_url else ""
            print(f"  [{i}/{len(to_scrape)}] {name}{remap_note} ... ", end="", flush=True)
            jobs = scrape_source(client, src, limit_per_source=args.per_source)
            print(f"{len(jobs)} raw")
            raw_jobs.extend(jobs)

    # ── Quality filter ────────────────────────────────────────────────────────
    all_jobs = [j for j in raw_jobs if is_real_job(j)]
    removed  = len(raw_jobs) - len(all_jobs)
    print(f"\nFiltered out {removed} junk entries → {len(all_jobs)} clean jobs")

    payload = {
        "sources_scraped": len(to_scrape),
        "total_jobs": len(all_jobs),
        "jobs": all_jobs,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"Wrote {payload['total_jobs']} jobs from {payload['sources_scraped']} sources to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())