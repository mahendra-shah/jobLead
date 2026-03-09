"""
scrape_all_jobs.py — ONE script, ALL sources, LOTS of jobs.

Sources:
  1. Your shortlisted sources API  (Built In Bengaluru, Hyderabad, Mumbai, Pune, etc.)
  2. RemoteOK API                  (free, no auth)
  3. Remotive API                  (free, no auth, 10 categories)
  4. Arbeitnow API                 (free, no auth, paginated)
  5. GitHub: remoteintech/remote-jobs  (500+ company career links)
  6. GitHub: awesome-remote-job        (curated job board links)
  7. HackerNews "Who is Hiring"    (monthly startup jobs thread)

Output: app/data/all_jobs.json

Usage:
  cd jobLead
  python scripts/scrape_all_jobs.py
  python scripts/scrape_all_jobs.py --per-source 300 --limit 40
  python scripts/scrape_all_jobs.py --no-shortlisted
  python scripts/scrape_all_jobs.py --no-github --no-hn
  python scripts/scrape_all_jobs.py --only-apis
"""

import argparse
import hashlib
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# ── Project root ──────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

DEFAULT_API_BASE = "http://localhost:8000"
DEFAULT_OUT      = "app/data/all_jobs.json"
REQUEST_TIMEOUT  = 25.0

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

JSON_HEADERS = {
    "User-Agent": "PlacementJobScraper/2.0",
    "Accept":     "application/json",
}

# ── Site homepage → jobs page remapping ───────────────────────────────────────
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

SKIP_PATH_FRAGMENTS = (
    "/login", "/signup", "/register",
    "/category/", "/tag/", "/author/",
    "/about", "/contact", "/privacy", "/terms",
    "/employer", "/hire", "/post-a-job",
    "/articles/", "/blog/", "/news/",
    "/auth/", "/oauth", "/careers/",
)

JOB_PATH_INDICATORS = (
    "/job/", "/jobs/", "/career/", "/careers/",
    "/opening/", "/openings/",
    "/position/", "/positions/",
    "/vacancy/", "/vacancies/",
    "/apply/", "/listing/", "/listings/",
    "/internship/", "/internships/",
)

JUNK_TITLES = {
    "remote", "careers", "all cities", "all designation",
    "we're hiring", "we're hiring", "hiring",
    "jobs in delhi", "jobs in mumbai", "jobs in chennai",
    "jobs in bangalore", "jobs in noida", "jobs in hyderabad",
    "jobs in gurgaon", "jobs in kolkata", "jobs in pune",
    "jobs in ahmedabad", "jobs in bengaluru",
    "view jobs", "view profile", "see all", "load more",
    "be part of our talent community", "apply now", "apply",
}

REMOTIVE_CATEGORIES = [
    "software-dev", "devops-sysadmin", "data", "product",
    "design", "qa", "backend", "frontend", "fullstack", "mobile",
]


# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_url(url: str) -> str:
    """Strip query/fragment, lowercase, remove trailing slash."""
    return url.split("?")[0].split("#")[0].rstrip("/").lower()


def url_hash(url: str) -> str:
    return hashlib.md5(normalize_url(url).encode()).hexdigest()


def get_jobs_url(source_url: str) -> str:
    """Remap bare homepages to their dedicated jobs page."""
    parsed = urlparse(source_url)
    netloc = parsed.netloc.lower()
    path   = parsed.path.rstrip("/")
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


def is_real_job(job: dict) -> bool:
    """Quality filter — reject nav links, junk, category pages."""
    title = job.get("title", "").strip()
    url   = job.get("url", "")

    if len(title.split()) <= 1:
        return False
    if title.lower() in JUNK_TITLES:
        return False
    if "techgig.com/jobs/city/" in url:
        parts = [p for p in urlparse(url).path.split("/") if p]
        if len(parts) >= 4 and parts[1] == "city":
            return False
    if re.search(r"techgig\.com/jobs/designation", url):
        parts = [p for p in urlparse(url).path.split("/") if p]
        if len(parts) <= 3:
            return False
    if re.search(r"techgig\.com/jobs/city/[^/]+-Jobs$", url):
        return False
    if "employers.builtin.com" in url:
        return False
    if not is_job_like_path(urlparse(url).path):
        # API jobs don't have job-like paths — allow them through
        if job.get("source_id", "").endswith("-api") or \
           job.get("source_id", "").startswith("github-") or \
           job.get("source_id", "").startswith("hn-"):
            return True
        return False
    return True


def make_job(
    title:       str,
    url:         str,
    source_name: str,
    source_id:   str   = "external",
    company:     str   = "",
    location:    str   = "",
    job_type:    str   = "",
    salary:      str   = "",
    tags:        Optional[List[str]] = None,
) -> dict:
    entry: dict = {
        "title":       title.strip()[:500],
        "url":         url,
        "source_name": source_name,
        "source_id":   source_id,
    }
    if company:  entry["company"]  = company.strip()
    if location: entry["location"] = location.strip()
    if job_type: entry["job_type"] = job_type.strip()
    if salary:   entry["salary"]   = salary.strip()
    if tags:     entry["tags"]     = tags
    return entry


def dedup(jobs: List[dict]) -> List[dict]:
    seen:   set        = set()
    result: List[dict] = []
    for j in jobs:
        h = url_hash(j.get("url", ""))
        if h not in seen:
            seen.add(h)
            result.append(j)
    return result


def section(title: str) -> None:
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 1 — Shortlisted sources from your API (Built In network, etc.)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_shortlisted_sources(api_base: str, page_size: int = 50) -> list:
    url = f"{api_base.rstrip('/')}/api/v1/discovery/sources/shortlisted"
    with httpx.Client(timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"}) as client:
        resp = client.get(url, params={"page": 1, "page_size": page_size})
        resp.raise_for_status()
    return resp.json().get("sources") or []


def extract_jobs_from_html(
    html: str, page_url: str, source_name: str, source_id: str
) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set        = set()
    jobs: List[dict] = []

    def add(title: str, href: str, location: str = "", company: str = "") -> None:
        if not href or not title or len(title.strip()) < 3:
            return
        full_url = urljoin(page_url, href)
        if not is_job_like_path(urlparse(full_url).path):
            return
        h = url_hash(full_url)
        if h in seen:
            return
        seen.add(h)
        jobs.append(make_job(title, full_url, source_name, source_id,
                             company=company, location=location))

    # Schema.org JobPosting microdata
    for node in soup.find_all(attrs={"itemtype": re.compile(r"JobPosting", re.I)}):
        link = node.find("a", href=True)
        t    = (node.find(attrs={"itemprop": "title"})
                or node.find(attrs={"itemprop": "name"})
                or node.find(["h2", "h3", "h4"]))
        if link and t:
            add(t.get_text(" ", strip=True), link["href"])

    # Generic job card selectors
    for sel in [
        "div[class*='job-card']", "li[class*='job-card']",
        "li.job_listing", "div.job-list-box",
        "[class*='job-item']", "[class*='job-listing']",
        "[class*='job-tile']", "[class*='job-result']",
        "[class*='listing-item']", "[data-job-id]", "[data-jk]",
        "article",
    ]:
        try:
            for card in soup.select(sel):
                link = card.find("a", href=True)
                t    = card.find(["h2", "h3", "h4"]) or link
                if link and t:
                    co  = card.find(class_=re.compile(r"company|employer|org", re.I))
                    loc = card.find(class_=re.compile(r"location|loc|city", re.I))
                    add(
                        t.get_text(" ", strip=True), link["href"],
                        location=loc.get_text(strip=True) if loc else "",
                        company =co.get_text(strip=True)  if co  else "",
                    )
        except Exception:
            continue

    # Direct href scan
    job_re = re.compile(
        r"/(job|jobs|career|careers|opening|openings|position|positions"
        r"|vacancy|vacancies|internship|listing)/", re.I)
    for a in soup.find_all("a", href=True):
        if job_re.search(a["href"]):
            t = a.get_text(" ", strip=True)
            if 5 <= len(t) <= 400:
                add(t, a["href"])

    return jobs


def scrape_html_source(
    client: httpx.Client, source: dict, limit: int = 100
) -> List[dict]:
    raw_url = source.get("url") or ""
    name    = source.get("name") or "Unknown"
    sid     = str(source.get("id") or "")
    if not raw_url:
        return []

    url      = get_jobs_url(raw_url)
    all_jobs: List[dict] = []
    visited:  set        = set()
    page_url = url

    for _ in range(1, 6):  # up to 5 pages
        if page_url in visited:
            break
        visited.add(page_url)
        try:
            resp = client.get(page_url, headers=BROWSER_HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            break

        new = [j for j in extract_jobs_from_html(resp.text, page_url, name, sid)
               if j["url"] not in {x["url"] for x in all_jobs}]
        all_jobs.extend(new)
        if len(all_jobs) >= limit:
            break

        # Follow next page
        soup = BeautifulSoup(resp.text, "html.parser")
        nxt  = (
            soup.find("a", string=re.compile(r"next|›|»|load more", re.I))
            or soup.find("a", attrs={"aria-label": re.compile(r"next", re.I)})
            or soup.find("a", rel=re.compile(r"next", re.I))
        )
        if nxt and nxt.get("href"):
            next_url = urljoin(page_url, nxt["href"])
            if next_url != page_url and next_url not in visited:
                page_url = next_url
                time.sleep(0.5)
                continue
        break

    return all_jobs[:limit]


def run_shortlisted(api_base: str, limit: int, per_source: int) -> List[dict]:
    section("SOURCE 1 — Your Shortlisted Sources (Built In network etc.)")
    try:
        sources = fetch_shortlisted_sources(api_base)
    except Exception as e:
        print(f"  ❌ Could not reach API: {e}")
        return []

    web_sources = [s for s in sources if (s.get("platform") or "").lower() == "web"]
    to_scrape   = web_sources[:limit]
    print(f"  Found {len(sources)} sources → {len(web_sources)} web → scraping {len(to_scrape)}")

    jobs: List[dict] = []
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        for i, src in enumerate(to_scrape, 1):
            name       = src.get("name") or src.get("url") or "?"
            remapped   = get_jobs_url(src.get("url") or "")
            remap_note = f" → {remapped}" if remapped != src.get("url") else ""
            print(f"  [{i:02}/{len(to_scrape)}] {name}{remap_note} ... ", end="", flush=True)
            result = scrape_html_source(client, src, limit=per_source)
            print(f"{len(result)} jobs")
            jobs.extend(result)

    print(f"  ✅ Shortlisted total: {len(jobs)} raw jobs")
    return jobs


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 2 — RemoteOK API
# ═══════════════════════════════════════════════════════════════════════════════

def run_remoteok(limit: int = 300) -> List[dict]:
    section("SOURCE 2 — RemoteOK API (free, no auth)")
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
            resp = client.get("https://remoteok.com/api", headers={
                "User-Agent": "PlacementJobScraper/2.0",
                "Accept": "application/json",
            })
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return []

    jobs: List[dict] = []
    seen: set = set()

    for j in data:
        if not isinstance(j, dict) or not j.get("position"):
            continue
        if len(jobs) >= limit:
            break
        url = j.get("url") or f"https://remoteok.com/l/{j.get('id','')}"
        h   = url_hash(url)
        if h in seen:
            continue
        seen.add(h)
        jobs.append(make_job(
            title       = j.get("position", ""),
            url         = url,
            source_name = "RemoteOK",
            source_id   = "remoteok-api",
            company     = j.get("company") or "",
            location    = j.get("location") or "Remote",
            tags        = j.get("tags") or [],
            salary      = j.get("salary") or "",
        ))

    print(f"  ✅ RemoteOK total: {len(jobs)} jobs")
    return jobs


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 3 — Remotive API
# ═══════════════════════════════════════════════════════════════════════════════

def run_remotive(limit: int = 300) -> List[dict]:
    section(f"SOURCE 3 — Remotive API ({len(REMOTIVE_CATEGORIES)} categories)")
    jobs: List[dict] = []
    seen: set = set()

    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        for cat in REMOTIVE_CATEGORIES:
            if len(jobs) >= limit:
                break
            try:
                resp = client.get(
                    "https://remotive.com/api/remote-jobs",
                    params={"category": cat, "limit": min(100, limit - len(jobs))},
                    headers=JSON_HEADERS,
                )
                resp.raise_for_status()
                raw = resp.json().get("jobs") or []
            except Exception as e:
                print(f"  [{cat}] FAILED: {e}")
                continue

            added = 0
            for j in raw:
                url   = j.get("url") or ""
                title = j.get("title") or ""
                if not url or not title:
                    continue
                h = url_hash(url)
                if h in seen:
                    continue
                seen.add(h)
                jobs.append(make_job(
                    title       = title,
                    url         = url,
                    source_name = "Remotive",
                    source_id   = "remotive-api",
                    company     = j.get("company_name") or "",
                    location    = j.get("candidate_required_location") or "Remote",
                    job_type    = j.get("job_type") or "",
                    salary      = j.get("salary") or "",
                    tags        = [cat],
                ))
                added += 1

            print(f"  [{cat}] {added} jobs")
            time.sleep(0.3)

    print(f"  ✅ Remotive total: {len(jobs)} jobs")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 4 — Arbeitnow API
# ═══════════════════════════════════════════════════════════════════════════════

def run_arbeitnow(limit: int = 200) -> List[dict]:
    section("SOURCE 4 — Arbeitnow API (paginated)")
    jobs: List[dict] = []
    seen: set = set()
    page = 1

    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        while len(jobs) < limit:
            try:
                resp = client.get(
                    "https://www.arbeitnow.com/api/job-board-api",
                    params={"page": page},
                    headers=JSON_HEADERS,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  page {page}: FAILED: {e}")
                break

            raw = data.get("data") or []
            if not raw:
                break

            added = 0
            for j in raw:
                url   = j.get("url") or ""
                title = j.get("title") or ""
                if not url or not title:
                    continue
                h = url_hash(url)
                if h in seen:
                    continue
                seen.add(h)
                jobs.append(make_job(
                    title       = title,
                    url         = url,
                    source_name = "Arbeitnow",
                    source_id   = "arbeitnow-api",
                    company     = j.get("company_name") or "",
                    location    = j.get("location") or "",
                    job_type    = (j.get("job_types") or [""])[0],
                    tags        = j.get("tags") or [],
                ))
                added += 1

            print(f"  page {page}: {added} jobs (running total: {len(jobs)})")
            page += 1

            if not data.get("links", {}).get("next"):
                break
            time.sleep(0.3)

    print(f"  ✅ Arbeitnow total: {len(jobs)} jobs")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 5 — GitHub: remoteintech/remote-jobs
# ═══════════════════════════════════════════════════════════════════════════════

def run_github_remoteintech(limit: int = 200) -> List[dict]:
    section("SOURCE 5 — GitHub: remoteintech/remote-jobs")
    jobs: List[dict] = []
    seen: set = set()

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(
                "https://raw.githubusercontent.com/remoteintech/remote-jobs/main/README.md",
                headers=JSON_HEADERS,
            )
            resp.raise_for_status()
            content = resp.text
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return []

    pattern = re.compile(r'\|\s*\[([^\]]+)\]\((https?://[^\)]+)\)\s*\|([^\|]*)\|')
    for company, url, region in pattern.findall(content):
        if len(jobs) >= limit:
            break
        careers_url = url.rstrip("/") + "/careers"
        h = url_hash(careers_url)
        if h in seen:
            continue
        seen.add(h)
        jobs.append(make_job(
            title       = f"Open Roles at {company.strip()}",
            url         = careers_url,
            source_name = "GitHub: remoteintech/remote-jobs",
            source_id   = "github-remoteintech",
            company     = company.strip(),
            location    = region.strip() or "Remote",
        ))

    print(f"  ✅ remoteintech total: {len(jobs)} company career links")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 6 — GitHub: awesome-remote-job
# ═══════════════════════════════════════════════════════════════════════════════

def run_github_awesome_remote(limit: int = 100) -> List[dict]:
    section("SOURCE 6 — GitHub: awesome-remote-job (job board links)")
    jobs: List[dict] = []
    seen: set = set()

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(
                "https://raw.githubusercontent.com/lukasz-madon/awesome-remote-job/master/README.md",
                headers=JSON_HEADERS,
            )
            resp.raise_for_status()
            content = resp.text
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return []

    in_jobboard_section = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("## Job board"):
            in_jobboard_section = True
            continue
        elif stripped.startswith("##"):
            in_jobboard_section = False
            continue
        if not in_jobboard_section:
            continue

        for name, url in re.findall(r'\[([^\]]+)\]\((https?://[^\)]+)\)', stripped):
            if len(jobs) >= limit:
                break
            h = url_hash(url)
            if h in seen:
                continue
            seen.add(h)
            jobs.append(make_job(
                title       = f"Jobs at {name}",
                url         = url,
                source_name = "GitHub: awesome-remote-job",
                source_id   = "github-awesome-remote",
                company     = name,
                location    = "Remote",
            ))

    print(f"  ✅ awesome-remote total: {len(jobs)} job board links")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 7 — HackerNews "Who is Hiring"
# ═══════════════════════════════════════════════════════════════════════════════

def run_hn_hiring(limit: int = 200) -> List[dict]:
    section("SOURCE 7 — HackerNews: Who is Hiring (latest thread)")
    jobs: List[dict] = []
    seen: set = set()

    # Find latest thread
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(
                "https://hn.algolia.com/api/v1/search",
                params={"query": "Ask HN: Who is hiring", "tags": "story,ask_hn", "hitsPerPage": 5},
                headers=JSON_HEADERS,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])
    except Exception as e:
        print(f"  ❌ Search FAILED: {e}")
        return []

    if not hits:
        print("  No thread found")
        return []

    thread_id = hits[0].get("objectID")
    print(f"  Thread: {hits[0].get('title')} (id={thread_id})")

    # Fetch comments
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(
                "https://hn.algolia.com/api/v1/search",
                params={"tags": f"comment,story_{thread_id}", "hitsPerPage": 1000},
                headers=JSON_HEADERS,
            )
            resp.raise_for_status()
            comments = resp.json().get("hits", [])
    except Exception as e:
        print(f"  ❌ Comments FAILED: {e}")
        return []

    url_re = re.compile(r'href=["\']?(https?://[^\s"\'<>]+)["\']?', re.I)

    for comment in comments:
        if len(jobs) >= limit:
            break
        if comment.get("parent_id") != int(thread_id):
            continue

        text = comment.get("comment_text") or ""
        if not text:
            continue

        plain      = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
        first_line = plain.split("\n")[0][:200].strip()
        if not first_line or len(first_line) < 5:
            continue

        urls    = url_re.findall(text)
        job_url = urls[0] if urls else f"https://news.ycombinator.com/item?id={comment.get('objectID')}"

        h = url_hash(job_url + first_line)
        if h in seen:
            continue
        seen.add(h)
        jobs.append(make_job(
            title       = first_line[:300],
            url         = job_url,
            source_name = "HackerNews: Who is Hiring",
            source_id   = f"hn-hiring-{thread_id}",
        ))

    print(f"  ✅ HackerNews total: {len(jobs)} job posts")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape jobs from ALL sources — Built In, APIs, GitHub, HackerNews"
    )
    parser.add_argument("--api-base",        default=DEFAULT_API_BASE)
    parser.add_argument("--limit",           type=int, default=20,  help="Max shortlisted sources to scrape")
    parser.add_argument("--out",             default=DEFAULT_OUT)
    parser.add_argument("--per-source",      type=int, default=100, help="Max jobs per HTML source")

    # Toggle individual sources ON/OFF
    parser.add_argument("--no-shortlisted",  action="store_true", help="Skip Built In / shortlisted sources")
    parser.add_argument("--no-remoteok",     action="store_true", help="Skip RemoteOK API")
    parser.add_argument("--no-remotive",     action="store_true", help="Skip Remotive API")
    parser.add_argument("--no-arbeitnow",    action="store_true", help="Skip Arbeitnow API")
    parser.add_argument("--no-github",       action="store_true", help="Skip GitHub sources")
    parser.add_argument("--no-hn",           action="store_true", help="Skip HackerNews")

    # Shortcut: only run the 3 free APIs
    parser.add_argument("--only-apis",       action="store_true", help="Only run RemoteOK + Remotive + Arbeitnow")

    args = parser.parse_args()

    # --only-apis shortcut
    if args.only_apis:
        args.no_shortlisted = True
        args.no_github      = True
        args.no_hn          = True

    out_path = JOBLEAD_ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("  🚀 scrape_all_jobs.py — Starting")
    print("=" * 55)

    all_raw: List[dict] = []

    if not args.no_shortlisted:
        all_raw.extend(run_shortlisted(args.api_base, args.limit, args.per_source))

    if not args.no_remoteok:
        all_raw.extend(run_remoteok(limit=300))

    if not args.no_remotive:
        all_raw.extend(run_remotive(limit=300))

    if not args.no_arbeitnow:
        all_raw.extend(run_arbeitnow(limit=200))

    if not args.no_github:
        all_raw.extend(run_github_remoteintech(limit=200))
        all_raw.extend(run_github_awesome_remote(limit=100))

    if not args.no_hn:
        all_raw.extend(run_hn_hiring(limit=200))

    # ── Filter + Dedup ────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print(f"  Raw collected : {len(all_raw)}")

    filtered = [j for j in all_raw if is_real_job(j)]
    print(f"  After filter  : {len(filtered)}  (removed {len(all_raw)-len(filtered)} junk)")

    clean = dedup(filtered)
    print(f"  After dedup   : {len(clean)} unique jobs")
    print("=" * 55)

    # ── Summary by source ─────────────────────────────────────────────────────
    by_source = Counter(j["source_name"] for j in clean)
    print("\n  Jobs by source:")
    for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"    {src:<48} {count:>5}")

    # ── Write output ──────────────────────────────────────────────────────────
    payload = {
        "total_jobs": len(clean),
        "by_source":  dict(by_source),
        "jobs":       clean,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"\n  ✅ Saved {len(clean)} jobs → {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())