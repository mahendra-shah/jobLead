# Phase 1: Discovery Engine

**Goal:** Discover **sources** that may contain jobs (job boards, GitHub repos, forums, company career pages, etc.). Not jobs yet. **Do not move to Phase 2 until Phase 1 is satisfied.**

**Testing:** All pipelines write to **JSON** (`app/data/discovery_sources_test.json`). When satisfied, run `scripts/discovery/import_discovery_json_to_db.py` to import to DB.

**Scope:** Telegram (joining groups, fetching data) is **out of scope** — other team. We only discover and store source URLs (e.g. t.me/group) in the JSON.

---

## Source JSON schema (testing)

All discovered sources are stored with:

| Field | Description |
|-------|-------------|
| `id` | Numeric id (auto) |
| `url` | Full URL |
| `domain` | Extracted domain (e.g. t.me, jobs.lever.co) |
| `type` | job_board \| telegram \| discord \| forum \| github_repo \| company_career \| website |
| `name` | Optional label |
| `city` | From query or null |
| `country` | From query or null |
| `confidence_score` | 0–10; **score > 5 = good source** |
| `first_seen` | ISO timestamp |
| `last_checked` | ISO timestamp |
| `status` | active \| pending \| failed |
| `metadata` | discovery_origin (search_engine \| github \| community \| forum), etc. |

---

## Source scoring

| Signal | Score |
|--------|-------|
| contains "jobs" | +3 |
| contains "hiring" | +2 |
| domain contains "jobs" | +3 |
| github repo | +1 |
| blog/article path | -1 |

**Good source:** `confidence_score > 5`.

---

## Four discovery pipelines

### Pipeline 1 — Search Engine Discovery

Finds most sources. Queries like:
- "python jobs bangalore", "software engineer jobs pune", "backend jobs india", "startup hiring india", "fresher developer jobs"
- site:t.me developer jobs, site:discord.gg jobs developer, site:github.com "job board", site:medium.com hiring engineer

**Script:** `scripts/discovery/pipeline_1_search_engine.py`  
**Usage:** `--simulation` (log only), `--delay 60`, `--max-queries 5`, `--out <path>`

---

### Pipeline 2 — GitHub Discovery

GitHub list READMEs (awesome-job-boards, remote jobs, india-dev-communities). Each repo has dozens of links.

**Script:** `scripts/discovery/pipeline_2_github.py` (or `github_discovery.py`)  
**Config:** `app/data/github_discovery_lists.json`  
**Usage:** `--delay 5`, `--dry-run`, `--out <path>`

---

### Pipeline 3 — Community Discovery

Telegram, Discord. Queries: site:t.me developer jobs, site:discord.gg developer jobs.  
We only **discover and store** URLs; joining/fetching is other team.

**Script:** `scripts/discovery/pipeline_3_community.py`  
**Usage:** `--simulation`, `--delay 60`, `--max-queries 4`

---

### Pipeline 4 — Forum Discovery

Reddit, HackerNews, Dev.to, Hashnode, IndieHackers.  
Queries: site:reddit.com "jobs india developer", site:news.ycombinator.com hiring.

**Script:** `scripts/discovery/pipeline_4_forum.py`  
**Usage:** `--simulation`, `--delay 60`

---

## Deduplication & URL normalization

- **Normalize before store:** All URLs are normalized (lowercase scheme+host, no fragment, no trailing slash). Duplicate URLs (different spelling) are not re-inserted.
- **Post-hoc dedup:** `base.dedup_sources(sources, keep="highest_score")` — by normalized URL, keep one per URL (optionally highest `confidence_score`). Run before or after discovery.
- **In analyzer:** `analyze_source.py --dedup-first` runs dedup on the JSON file before analyzing.

---

## Dynamic Website Analyzer (critical before Phase-2)

**Script:** `scripts/discovery/analyze_source.py`

For each discovered source the analyzer:

1. **robots.txt** — Fetches `{origin}/robots.txt`, parses `Disallow` and `Sitemap` → `metadata.robots_disallow_paths`, `metadata.robots_sitemap_urls`
2. **HTML** — Fetches page, extracts links
3. **Job links** — Detects `/jobs`, `/careers`, `/openings`, `/positions`, `/vacancies`, `/internships` → `metadata.job_page_urls`, `metadata.job_page_detected`
4. **Sitemap** — From HTML `<link rel="sitemap">` or common paths, or from robots.txt → `metadata.sitemap_url`
5. **Pagination** — Detects next-link, `?page=`, or `/page/N` → `metadata.pagination_type`, `metadata.pagination_sample_url`
6. **Crawl strategy** — Builds `metadata.crawl_strategy` for Phase-2:
   - `entry_urls`: job listing URLs to crawl first (or [])
   - `sitemap_url`: for sitemap-based discovery
   - `robots_disallow`: paths to avoid
   - `pagination_type`: `next_link` | `query` | `path`
   - `crawl_ready`: true if we have entry_urls or sitemap and no 403/429

Phase-2 crawler should read `metadata.crawl_strategy.entry_urls` (e.g. `company.com/careers`) instead of the homepage.

**Usage:**  
`python scripts/discovery/analyze_source.py --max 20 --delay 3`  
`python scripts/discovery/analyze_source.py --dedup-first`  
`python scripts/discovery/analyze_source.py --dedup-only`  
`python scripts/discovery/analyze_source.py --simulation`

---

## Scripts map

| Script | Pipeline | Role |
|--------|----------|------|
| `scripts/discovery/query_generator.py` | — | Query lists for all 4 pipelines |
| `scripts/discovery/base.py` | — | JSON load/save, schema, scoring, **normalize_url, dedup_sources** |
| `scripts/discovery/run_search.py` | 1,3,4 | DuckDuckGo search (shared) |
| `scripts/discovery/pipeline_1_search_engine.py` | 1 | Search engine discovery → JSON |
| `scripts/discovery/pipeline_2_github.py` | 2 | GitHub list READMEs → JSON |
| `scripts/discovery/pipeline_3_community.py` | 3 | Telegram/Discord discovery → JSON |
| `scripts/discovery/pipeline_4_forum.py` | 4 | Forum discovery → JSON |
| `scripts/discovery/analyze_source.py` | — | **Visit URL, detect job pages/sitemap, update JSON** |
| `scripts/discovery/import_discovery_json_to_db.py` | — | Import JSON → DB when ready |
| `scripts/job_board_flow/scrape_all_jobs.py` | Layer 3 | Crawl known sources (Phase 2) |

---

## How to run (test Phase 1)

1. **Simulation first** (no HTTP, only log queries):
   ```bash
   cd jobLead
   python scripts/discovery/pipeline_1_search_engine.py --simulation
   python scripts/discovery/pipeline_3_community.py --simulation
   python scripts/discovery/pipeline_4_forum.py --simulation
   ```

2. **Real runs** (rate-limited; same JSON file):
   ```bash
   python scripts/discovery/pipeline_2_github.py --delay 5
   python scripts/discovery/pipeline_1_search_engine.py --delay 60 --max-queries 5
   python scripts/discovery/pipeline_3_community.py --delay 60 --max-queries 4
   python scripts/discovery/pipeline_4_forum.py --delay 60
   ```

3. **Dedup (optional):** Run analyzer with `--dedup-first`, or call `dedup_sources()` on loaded list before save.

4. **Source analyzer** (before Phase-2):  
   `python scripts/discovery/analyze_source.py --max 50 --delay 3 --dedup-first`

5. **Inspect:** `app/data/discovery_sources_test.json` (sources with id, url, domain, type, confidence_score, metadata.job_page_urls, etc.).

6. **When ready for DB:**  
   `python scripts/discovery/import_discovery_json_to_db.py` (optionally `--dry-run` first).

---

## Data flow

```
Pilot cities + Fresher keywords
        ↓
  Query generator (4 pipelines)
        ↓
  Pipeline 1 (search) ──┐
  Pipeline 2 (GitHub) ──┼──→ discovery_sources_test.json  (normalize_url, dedup)
  Pipeline 3 (community) ─┤
  Pipeline 4 (forum) ───┘
        ↓
  analyze_source.py  (job pages, sitemap, status)
        ↓  (when tested, sources > 5k consider DB)
  import_discovery_json_to_db.py → discovery_sources (DB)
        ↓
  Phase 2: shortlist (score > 5), then scrape_all_jobs.py (Layer 3)
```

---

## Safeguards

- **JSON first:** Test everything in JSON; only then import to DB.
- **Simulation:** Use `--simulation` to log would-be requests and tune rate limits.
- **Delay:** Use `--delay` (e.g. 60s) between search requests to avoid bans.
- **Telegram:** Out of scope for this team; we only store discovered t.me URLs.

---

## What's missing for production Phase-1

| Gap | Purpose |
|-----|--------|
| **Domain classification** | Distinguish job board vs company site vs blog (beyond current type inference). |
| **robots.txt check** | Before Phase-2 crawl, detect Disallow rules. |
| **Domain rate tracking** | Per-domain request counts / last_request to avoid bans at scale. |
| **Source health check** | Periodic re-check: HTTP 200 vs 404/403; mark dead sources. |

Add these when scaling beyond pilot (e.g. approaching ~10k sources).

---

## When to move JSON → DB

Stay on JSON until **sources > ~5k** or you need query/ranking/joins. Then:

- **DB:** PostgreSQL, table `discovery_sources`.
- **Import:** `scripts/discovery/import_discovery_json_to_db.py`.

---

## Phase-1 success criteria

After running pipelines for 2–3 days (rate-limited):

- **Rough counts:** job boards ~3k, company career ~2k, forums ~1.5k, GitHub lists ~500, communities ~3k → **~10k sources**.
- **Shortlist:** `confidence_score > 5` → **~1.5k good sources** for Phase-2.
- **Then:** Run source analyzer, then Phase-2 crawler (`scrape_all_jobs.py`) on shortlisted sources.
