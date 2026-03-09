"""
Scrape job data from multiple diverse sources:

  1. Shortlisted sources from your API  (Built In network, etc.)
  2. RemoteOK API          — free, no auth, 1000s of remote jobs
  3. Remotive API          — free, no auth, remote jobs by category
  4. Arbeitnow API         — free, no auth, international jobs
  5. GitHub remote-jobs    — github.com/remoteintech/remote-jobs (company list)
  6. GitHub awesome-remote — github.com/lukasz-madon/awesome-remote-job (links)
  7. HackerNews "Who's Hiring" — monthly thread scrape

Output: app/data/scraped_jobs_from_sources.json

Usage:
  cd jobLead && python scripts/scrape_jobs_from_shortlisted_sources.py
  python scripts/scrape_jobs_from_shortlisted_sources.py --per-source 300 --limit 40
"""

import argparse
import hashlib
import json
import re
import sys
import time
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
DEFAULT_OUT      = "app/data/scraped_jobs_from_sources.json"
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
    "Accept": "application/json",
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

# ── Remotive categories that are most relevant ────────────────────────────────
REMOTIVE_CATEGORIES = [
    "software-dev",
    "devops-sysadmin",
    "data",
    "product",
    "design",
    "qa",
    "backend",
    "frontend",
    "fullstack",
    "mobile",
]


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def get_jobs_url(source_url: str) -> str:
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


def normalize_url(url: str) -> str:
    return url.split("?")[0].split("#")[0].rstrip("/").lower()


def url_hash(url: str) -> str:
    return hashlib.md5(normalize_url(url).encode()).hexdigest()


def is_real_job(job: dict) -> bool:
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
        return False
    return True


def make_job(
    title: str,
    url: str,
    source_name: str,
    source_id: str = "external",
    company: str = "",
    location: str = "",
    job_type: str = "",
    tags: Optional[List[str]] = None,
    salary: str = "",
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


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 1 — Your existing shortlisted sources (Built In network etc.)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_shortlisted_sources(api_base: str, page_size: int = 50) -> list:
    url = f"{api_base.rstrip('/')}/api/v1/discovery/sources/shortlisted"
    with httpx.Client(timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"}) as client:
        resp = client.get(url, params={"page": 1, "page_size": page_size})
        resp.raise_for_status()
    return resp.json().get("sources") or []


def extract_jobs_from_html(html: str, page_url: str, source_name: str, source_id: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set = set()
    jobs: List[dict] = []

    def add(title: str, href: str, location: str = "", company: str = "") -> None:
        if not href or not title or len(title.strip()) < 3:
            return
        full_url    = urljoin(page_url, href)
        parsed_path = urlparse(full_url).path
        if not is_job_like_path(parsed_path):
            return
        h = url_hash(full_url)
        if h in seen:
            return
        seen.add(h)
        jobs.append(make_job(title, full_url, source_name, source_id, company=company, location=location))

    # Schema.org JobPosting
    for node in soup.find_all(attrs={"itemtype": re.compile(r"JobPosting", re.I)}):
        link = node.find("a", href=True)
        t    = node.find(attrs={"itemprop": "title"}) or node.find(["h2","h3","h4"])
        if link and t:
            add(t.get_text(" ", strip=True), link["href"])

    # Built In / generic job cards
    for sel in [
        "div[class*='job-card']", "li[class*='job-card']",
        "li.job_listing", "div.job-list-box",
        "[class*='job-item']", "[class*='job-listing']",
        "[class*='job-tile']",  "[class*='job-result']",
        "[class*='listing-item']", "[data-job-id]", "[data-jk]",
        "article",
    ]:
        try:
            for card in soup.select(sel):
                link = card.find("a", href=True)
                t    = card.find(["h2","h3","h4"]) or link
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


def scrape_html_source(client: httpx.Client, source: dict, limit: int = 100) -> List[dict]:
    raw_url = source.get("url") or ""
    name    = source.get("name") or "Unknown"
    sid     = str(source.get("id") or "")
    if not raw_url:
        return []

    url      = get_jobs_url(raw_url)
    all_jobs: List[dict] = []
    visited:  set        = set()
    page_url = url

    for _ in range(1, 6):
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


def scrape_shortlisted(api_base: str, limit: int, per_source: int) -> List[dict]:
    sources     = fetch_shortlisted_sources(api_base)
    web_sources = [s for s in sources if (s.get("platform") or "").lower() == "web"]
    to_scrape   = web_sources[:limit]
    print(f"\n[Shortlisted sources] {len(sources)} total, {len(web_sources)} web → scraping {len(to_scrape)}")

    jobs: List[dict] = []
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        for i, src in enumerate(to_scrape, 1):
            name       = src.get("name") or src.get("url") or "?"
            remapped   = get_jobs_url(src.get("url") or "")
            remap_note = f" (→ {remapped})" if remapped != src.get("url") else ""
            print(f"  [{i}/{len(to_scrape)}] {name}{remap_note} ... ", end="", flush=True)
            result = scrape_html_source(client, src, limit=per_source)
            print(f"{len(result)} raw")
            jobs.extend(result)
    return jobs


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 2 — RemoteOK API (free, no auth)
#  https://remoteok.com/api
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_remoteok(limit: int = 300) -> List[dict]:
    print(f"\n[RemoteOK API] Fetching up to {limit} jobs ...", end="", flush=True)
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
            resp = client.get("https://remoteok.com/api", headers={
                "User-Agent": "PlacementJobScraper/2.0",
                "Accept": "application/json",
            })
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f" FAILED ({e})")
        return []

    # First item is a legal notice dict, skip it
    raw_jobs = [j for j in data if isinstance(j, dict) and j.get("position")]
    jobs: List[dict] = []
    seen: set = set()

    for j in raw_jobs[:limit]:
        url   = j.get("url") or f"https://remoteok.com/l/{j.get('id','')}"
        title = j.get("position") or ""
        if not title or not url:
            continue
        h = url_hash(url)
        if h in seen:
            continue
        seen.add(h)
        jobs.append(make_job(
            title       = title,
            url         = url,
            source_name = "RemoteOK",
            source_id   = "remoteok-api",
            company     = j.get("company") or "",
            location    = j.get("location") or "Remote",
            tags        = j.get("tags") or [],
            salary      = j.get("salary") or "",
        ))

    print(f" {len(jobs)} jobs")
    return jobs


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 3 — Remotive API (free, no auth)
#  https://remotive.com/api/remote-jobs?category=...
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_remotive(limit: int = 300) -> List[dict]:
    print(f"\n[Remotive API] Fetching jobs across {len(REMOTIVE_CATEGORIES)} categories ...")
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
                data = resp.json()
            except Exception as e:
                print(f"  {cat}: FAILED ({e})")
                continue

            raw = data.get("jobs") or []
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
            print(f"  {cat}: {added} jobs")
            time.sleep(0.3)

    print(f"  Total Remotive: {len(jobs)} jobs")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 4 — Arbeitnow API (free, no auth)
#  https://www.arbeitnow.com/api/job-board-api
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_arbeitnow(limit: int = 200) -> List[dict]:
    print(f"\n[Arbeitnow API] Fetching up to {limit} jobs ...")
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
                print(f"  page {page}: FAILED ({e})")
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
                    job_type    = j.get("job_types", [""])[0] if j.get("job_types") else "",
                    tags        = j.get("tags") or [],
                ))
                added += 1

            print(f"  page {page}: {added} jobs (total {len(jobs)})")
            page += 1
            if len(jobs) >= limit or not data.get("links", {}).get("next"):
                break
            time.sleep(0.3)

    print(f"  Total Arbeitnow: {len(jobs)} jobs")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 5 — GitHub: remoteintech/remote-jobs
#  Scrapes the README for company career links
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_github_remote_jobs(limit: int = 200) -> List[dict]:
    print(f"\n[GitHub remoteintech/remote-jobs] Fetching company career links ...")
    jobs: List[dict] = []
    seen: set = set()

    # Fetch the raw README (markdown)
    readme_url = "https://raw.githubusercontent.com/remoteintech/remote-jobs/main/README.md"
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(readme_url, headers=JSON_HEADERS)
            resp.raise_for_status()
            content = resp.text
    except Exception as e:
        print(f"  FAILED ({e})")
        return []

    # Extract markdown table rows: | Company | Region | Info |
    # Pattern: | [Company Name](url) | ... |
    pattern = re.compile(r'\|\s*\[([^\]]+)\]\((https?://[^\)]+)\)\s*\|([^\|]*)\|', re.MULTILINE)
    matches = pattern.findall(content)

    for company, url, region in matches:
        if len(jobs) >= limit:
            break
        company = company.strip()
        region  = region.strip()
        # Convert company homepage to careers page
        careers_url = url.rstrip("/") + "/careers"
        h = url_hash(careers_url)
        if h in seen:
            continue
        seen.add(h)
        jobs.append(make_job(
            title       = f"Open Roles at {company}",
            url         = careers_url,
            source_name = "GitHub: remoteintech/remote-jobs",
            source_id   = "github-remoteintech",
            company     = company,
            location    = region or "Remote",
        ))

    print(f"  {len(jobs)} company career links")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 6 — GitHub: awesome-remote-job links
#  github.com/lukasz-madon/awesome-remote-job
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_awesome_remote_job(limit: int = 100) -> List[dict]:
    print(f"\n[GitHub awesome-remote-job] Fetching job board links ...")
    jobs: List[dict] = []
    seen: set = set()

    readme_url = "https://raw.githubusercontent.com/lukasz-madon/awesome-remote-job/master/README.md"
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            resp = client.get(readme_url, headers=JSON_HEADERS)
            resp.raise_for_status()
            content = resp.text
    except Exception as e:
        print(f"  FAILED ({e})")
        return []

    # Find job board links in sections like "## Job boards" and "## Job boards aggregators"
    # Extract: [Name](url) - description
    in_jobboard_section = False
    for line in content.splitlines():
        stripped = line.strip()

        # Track section headers
        if stripped.startswith("## Job board"):
            in_jobboard_section = True
            continue
        elif stripped.startswith("##"):
            in_jobboard_section = False
            continue

        if not in_jobboard_section:
            continue

        # Extract markdown links
        matches = re.findall(r'\[([^\]]+)\]\((https?://[^\)]+)\)', stripped)
        for name, url in matches:
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

    print(f"  {len(jobs)} job board links")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  SOURCE 7 — HackerNews "Who is Hiring" (latest monthly thread)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_hn_who_is_hiring(limit: int = 200) -> List[dict]:
    print(f"\n[HackerNews 'Who is Hiring'] Fetching latest thread ...")
    jobs: List[dict] = []
    seen: set = set()

    # Step 1: Find the latest "Who is Hiring" thread via Algolia HN search
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            search_resp = client.get(
                "https://hn.algolia.com/api/v1/search",
                params={
                    "query": "Ask HN: Who is hiring",
                    "tags": "story,ask_hn",
                    "hitsPerPage": 5,
                },
                headers=JSON_HEADERS,
            )
            search_resp.raise_for_status()
            hits = search_resp.json().get("hits", [])
    except Exception as e:
        print(f"  Search FAILED ({e})")
        return []

    if not hits:
        print("  No thread found")
        return []

    thread_id = hits[0].get("objectID")
    print(f"  Thread: {hits[0].get('title')} (id={thread_id})")

    # Step 2: Fetch all comments from the thread
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
            comments_resp = client.get(
                "https://hn.algolia.com/api/v1/search",
                params={
                    "tags": f"comment,story_{thread_id}",
                    "hitsPerPage": 1000,
                },
                headers=JSON_HEADERS,
            )
            comments_resp.raise_for_status()
            comments = comments_resp.json().get("hits", [])
    except Exception as e:
        print(f"  Comments FAILED ({e})")
        return []

    # Step 3: Parse top-level comments (job posts)
    url_re = re.compile(r'href=["\']?(https?://[^\s"\'<>]+)["\']?', re.I)

    for comment in comments[:limit * 2]:
        if len(jobs) >= limit:
            break

        # Only top-level comments (direct replies to the thread)
        if comment.get("parent_id") != int(thread_id):
            continue

        text = comment.get("comment_text") or ""
        if not text:
            continue

        # Extract company name and URL from comment
        soup  = BeautifulSoup(text, "html.parser")
        plain = soup.get_text(" ", strip=True)

        # First line is usually "Company | Location | Remote/Onsite | ..."
        first_line = plain.split("\n")[0][:200].strip()
        if not first_line or len(first_line) < 5:
            continue

        # Try to find a URL in the comment
        urls = url_re.findall(text)
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
            location    = "",
        ))

    print(f"  {len(jobs)} job posts")
    return jobs[:limit]


# ═══════════════════════════════════════════════════════════════════════════════
#  DEDUP across all sources
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_jobs(jobs: List[dict]) -> List[dict]:
    seen:   set        = set()
    result: List[dict] = []
    for j in jobs:
        h = url_hash(j.get("url", ""))
        if h not in seen:
            seen.add(h)
            result.append(j)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape jobs from multiple diverse sources")
    parser.add_argument("--api-base",   default=DEFAULT_API_BASE)
    parser.add_argument("--limit",      type=int, default=20,  help="Max shortlisted sources to scrape")
    parser.add_argument("--out",        default=DEFAULT_OUT)
    parser.add_argument("--per-source", type=int, default=100, help="Max jobs per HTML source")

    # Flags to enable/disable individual sources
    parser.add_argument("--no-shortlisted", action="store_true", help="Skip shortlisted API sources")
    parser.add_argument("--no-remoteok",    action="store_true", help="Skip RemoteOK API")
    parser.add_argument("--no-remotive",    action="store_true", help="Skip Remotive API")
    parser.add_argument("--no-arbeitnow",   action="store_true", help="Skip Arbeitnow API")
    parser.add_argument("--no-github",      action="store_true", help="Skip GitHub sources")
    parser.add_argument("--no-hn",          action="store_true", help="Skip HackerNews")

    args = parser.parse_args()

    out_path = JOBLEAD_ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_raw: List[dict] = []

    # ── 1. Shortlisted sources ────────────────────────────────────────────────
    if not args.no_shortlisted:
        try:
            all_raw.extend(scrape_shortlisted(args.api_base, args.limit, args.per_source))
        except Exception as e:
            print(f"  Shortlisted sources FAILED: {e}")

    # ── 2. RemoteOK ───────────────────────────────────────────────────────────
    if not args.no_remoteok:
        all_raw.extend(fetch_remoteok(limit=300))

    # ── 3. Remotive ───────────────────────────────────────────────────────────
    if not args.no_remotive:
        all_raw.extend(fetch_remotive(limit=300))

    # ── 4. Arbeitnow ─────────────────────────────────────────────────────────
    if not args.no_arbeitnow:
        all_raw.extend(fetch_arbeitnow(limit=200))

    # ── 5 & 6. GitHub sources ─────────────────────────────────────────────────
    if not args.no_github:
        all_raw.extend(fetch_github_remote_jobs(limit=200))
        all_raw.extend(fetch_awesome_remote_job(limit=100))

    # ── 7. HackerNews ─────────────────────────────────────────────────────────
    if not args.no_hn:
        all_raw.extend(fetch_hn_who_is_hiring(limit=200))

    # ── Filter + dedup ────────────────────────────────────────────────────────
    print(f"\n{'─'*50}")
    print(f"Raw total:  {len(all_raw)}")

    filtered = [j for j in all_raw if is_real_job(j)]
    print(f"After filter: {len(filtered)} (removed {len(all_raw)-len(filtered)} junk)")

    clean = dedup_jobs(filtered)
    print(f"After dedup:  {len(clean)} unique jobs")

    # ── Summary by source ─────────────────────────────────────────────────────
    from collections import Counter
    by_source = Counter(j["source_name"] for j in clean)
    print("\nJobs by source:")
    for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"  {src:<45} {count:>5}")

    # ── Write output ──────────────────────────────────────────────────────────
    payload = {
        "total_jobs":  len(clean),
        "by_source":   dict(by_source),
        "jobs":        clean,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Wrote {len(clean)} jobs to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())