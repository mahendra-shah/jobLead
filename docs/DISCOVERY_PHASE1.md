# Phase 1: Discovery (Other Sources)

Discover and store **city-wise job boards and programming communities** in India (and global) so we can later shortlist and crawl them.

## What’s in place

- **`discovery_sources`** table: stores name, url, source_type (job_board, community, telegram_channel, discord, website, github_repo), platform, city, region, country_code, phase.
- **Seed data**: `app/data/discovery_sources_seed.json` — 80 sources in 4 phases (India city-wise job boards, Telegram/Discord communities, global boards, niche boards).
- **Runner**: `scripts/run_discovery_phases.py` — imports seed by phase, dedupes by URL.

## Run (phased until 1000)

1. **Apply migration** (once):
   ```bash
   alembic upgrade head
   ```
   (If the `discovery_sources` migration is not applied, run it.)

2. **Import seed in phases**:
   ```bash
   # From project root, with venv activated
   python scripts/run_discovery_phases.py              # all phases
   python scripts/run_discovery_phases.py --phase 1    # only phase 1
   python scripts/run_discovery_phases.py --dry-run    # no DB write
   python scripts/run_discovery_phases.py --target 1000
   ```

3. **Reach 1000**: The current seed has ~80 sources. To reach 1000:
   - Add more entries to `app/data/discovery_sources_seed.json` (e.g. from [Ultimate-Tech-Jobs](https://github.com/DhanushNehru/Ultimate-Tech-Jobs), more cities, more Telegram/Discord lists), or
   - Later: plug in search-based discovery (DuckDuckGo, GitHub API, etc.) with rate limiting and log requests to avoid bans.

## Seed sources (summary)

- **Phase 1**: Built In (Bangalore, Hyderabad, Mumbai, Pune), TechGig city pages (20+ Indian cities), TechNokri (Indore), VerifiedJobs, OpenFresher, GeekHub, Internshala, developersIndia job board, Hackerspace Mumbai.
- **Phase 2**: developersIndia Discord, CodeIN Community, 7 Telegram job channels (e.g. ENGINEER JOBS INDIA, OceanOfJobs, OFF CAMPUS JOBS INDIA).
- **Phase 3**: CutShort, Cuvette, Naukri, Unstop, Remote OK, We Work Remotely, WellFound, Himalayas, etc.
- **Phase 4**: GitHub/Stack Overflow jobs, Hasgeek, Python/React/Vue/Laravel boards, TechGig extra cities.

## Local run: see discovery + jobs in Swagger

1. **Apply migration** (once): `alembic upgrade head`
2. **Load seed**: `python scripts/run_discovery_phases.py` (from project root, venv active)
3. **Start API**: `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`
4. **Open Swagger**: http://localhost:8000/docs
5. **Discovery** (tag **Discovery**):
   - `GET /api/v1/discovery/pilot-cities` — 20 India + 20 outside cities
   - `GET /api/v1/discovery/fresher-keywords` — fresher pilot keywords
   - `GET /api/v1/discovery/sources` — list all discovery sources (optional: `shortlisted_only`, `phase`, `city`, `source_type`)
   - `GET /api/v1/discovery/sources/shortlisted` — only shortlisted
   - `GET /api/v1/discovery/summary` — counts: sources, shortlisted, telegram groups from discovery, jobs total
   - `POST /api/v1/discovery/shortlist` — run heuristics to mark up to 200 as shortlisted
   - `POST /api/v1/discovery/sync-telegram` — sync shortlisted Telegram sources into `telegram_groups` (so scraper can fetch jobs)
6. **Jobs** (tag **Jobs**): `GET /api/v1/jobs` — list fetched jobs (after running scraper on the synced Telegram groups).

To **fetch jobs** from shortlisted Telegram sources: run shortlist → sync-telegram, then run the Telegram scraper (scheduler or `POST /api/v1/admin/trigger-scrape`). New jobs will appear in `GET /api/v1/jobs`.

## Task list (from plan)

- [x] Discovery storage (DB + seed + runner)
- [x] Pilot cities (20 India + 20 outside); fresher keywords; shortlist 200 heuristics
- [x] Discovery API + sync shortlisted Telegram to telegram_groups (local fetch)
- [ ] Run phases until 1000 (add more seed or API discovery)
- [ ] S3 backup of discovery list (optional)
- [ ] Separate crawlers for Telegram / Discord / websites (Phase 2)
