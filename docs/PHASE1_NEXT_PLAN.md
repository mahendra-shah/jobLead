# Phase 1 “Not Done” – Plan & Requirements

How we go next on the remaining discovery/crawl items, in order, and what we need from you.

---

## 1. Overview: What’s left (grouped)

| Group | Items | Depends on |
|-------|--------|------------|
| **A. Reach 1k + backup** | Get discovery_sources to 1000; optional S3 backup | Nothing (can start) |
| **B. Safe search / no ban** | Proxies, slow multi-provider search, simulation mode, request logging | Your infra/API choices |
| **C. Task manager** | Keywords + sources lists; app logic vs network layer | After we have 1k+ sources |
| **D. Shortlist & pilot** | Shortlist 200 job-like from 1k; fresher-keyword pilot; don’t discard; S3 backup | A done |
| **E. 10k → 1k by type** | Shortlist 1k each: Telegram, Discord, websites | D done |
| **F. Crawlers** | Telegram (exists), Discord crawler, website/job-board crawler (captcha, sitemap, HTML) | E; your infra |
| **G. Infra & retention** | EC2 100GB, 24×7 slow run; 7d purge policy; S3 backup of “good” data | Your AWS/EC2 |

---

## 2. Requirements from your end

### 2.1 Decisions (no cost)

- **Pilot scope**: Confirm 20 cities in India + 20 outside India for the first pilot (we can use the cities already in the seed; add/remove as you like).
- **Fresher pilot**: Confirm we do a small “fresher jobs” keyword run to tune discovery (yes/no; which keywords).
- **Shortlist 200**: After 1k sources, we shortlist 200 that “sound like job groups” – confirm you’re okay with a mix of heuristics + optional manual review.

### 2.2 Data / access

- **More seed URLs**: To reach 1k without hitting search APIs yet: curated lists (e.g. more Telegram/Discord/Meetup links, city-wise boards). You or Asha can add to `app/data/discovery_sources_seed.json` or send a list we’ll ingest.
- **GitHub**: If we use GitHub Search API for discovery: GitHub token (fine-grained, read-only) for higher rate limits.
- **Search APIs (optional)**: If we use DuckDuckGo/Bing/Brave etc.: we need to know which you’re okay to use (and if you’ll use proxies). We can start with **no** paid APIs and only free + proxies.

### 2.3 Infra (when we need it)

- **AWS**: S3 bucket for backups (discovery list, “good data”, raw dumps). IAM key for the app/EC2.
- **EC2**: When we run 24×7 discovery/crawl: instance with 100GB disk (as per your plan); we’ll document exact size and 7d purge policy.
- **Proxies (optional)**: If we use paid/free search APIs and want to avoid bans: proxy provider and config (or “no proxies” to start).

### 2.4 Process (non-code)

- **WhatsApp group**: For tech day-to-day + students + Cursor prompts feedback (you mentioned creating it and adding Asha/students).
- **Cursor prompts**: Share prompts you use so we can align automation with your workflow.

---

## 3. Planning: order of work

### Phase 1a – Now (no dependency on you)

- **Reach 1k discovery sources**
  - Expand `app/data/discovery_sources_seed.json` (e.g. from Ultimate-Tech-Jobs, more cities, more Telegram/Discord).
  - Run `scripts/run_discovery_phases.py` in phases until count ≥ 1000.
- **Optional**: Script to export `discovery_sources` to JSON/CSV and upload to S3 (once you give bucket + IAM).

### Phase 1b – After 1k (needs your decisions 2.1)

- **Shortlist 200**
  - Heuristics: name/url keywords (job, hiring, careers, fresher, off-campus, etc.); source_type + platform.
  - Mark in DB (e.g. `is_shortlisted = true` or a `shortlist_phase` column).
  - Optional: small UI or CSV for you to review 200.
- **Fresher-keyword pilot**
  - Run discovery (or filter existing 1k) with fresher-focused keywords; store results; compare with rest.
- **Don’t discard**
  - All discovered URLs stay in DB; only “shortlisted” is used for next-step crawling. Optional S3 backup of full list.

### Phase 1c – Safe search (needs 2.2 and optionally 2.3)

- **Simulation mode**
  - Discovery runner: “simulation” mode that only logs what *would* be requested (provider, URL, keyword, timestamp). No real API calls. Run for 1 week, then review logs to set rate limits.
- **Request logging**
  - When we add real search (DuckDuckGo or GitHub etc.): log every request (provider, ts, keyword) to DB or file; review to avoid bans.
- **Proxies + slow loop**
  - Config for proxy (if you provide 2.3); per-provider delays; rotate providers (e.g. 100+ sources of truth, slow crawl).

### Phase 1d – Task manager (after 1k + shortlist)

- **Keywords and sources lists**
  - DB or config: list of keywords (e.g. “fresher jobs Bangalore”), list of “sources to search” (providers/APIs). Discovery job reads from these; no hardcoding in network layer.
- **Separation**
  - Application layer: what to search (keywords, source list). Network layer: how to call (one module per provider, rate limit, proxy). So we can swap providers without changing app logic.

### Phase 1e – 10k → 1k by type (after shortlist 200)

- **Classify and cap**
  - From 1k (or more) sources: classify by type (Telegram, Discord, website/job board). Shortlist up to 1k Telegram, 1k Discord, 1k websites (or your target numbers). Store in same table with tags or a `crawl_type` field.
- **Separate crawlers**
  - Telegram: already exists. Discord: new crawler (needs bot/token from you when we start). Website: new crawler (Phase 1f).

### Phase 1f – Website / job-board crawler (needs 2.3 when we run at scale)

- **First-pass checks**
  - Detect captcha/signup walls (e.g. headless fetch, check for common captcha/forms). Mark “crawlable” vs “blocked”.
- **Sitemap + structure**
  - Fetch sitemap; find job-related paths; sample pages; infer HTML structure (listing vs detail); store per-domain “crawl plan”.
- **Crawl and save**
  - Use crawl plan to extract job-like content; filter (regex then ML if needed); save to DB; preserve useful bits (e.g. poster contact). No deletion of useful data; 7d purge only on agreed datasets.

### Phase 1g – Infra & retention (your side + our config)

- **EC2**
  - 100GB disk; run discovery/crawlers 24×7; log rotation; 7d purge policy on *selected* data (not discovery list, not 30k groups list).
- **S3**
  - Back up: discovery_sources export, “good” crawl data, long-tail lists (e.g. 30k groups we can’t join yet). We provide scripts; you provide bucket + IAM.

---

## 4. What we need from you (checklist)

- [x] **Pilot cities**: Confirmed 20 India + 20 outside (see `app/data/pilot_cities.json`).
- [x] **Fresher pilot**: Yes; keywords in `app/data/fresher_keywords.json`.
- [x] **Shortlist 200**: Heuristics (name/url keywords) + optional manual review; `POST /api/v1/discovery/shortlist`.
- [ ] **More seed data**: Optional list of URLs/lists to add to discovery seed.
- [ ] **GitHub token**: Only if we use GitHub Search API (we can skip initially).
- [ ] **Search APIs**: Which providers you’re okay with (DuckDuckGo, Bing, etc.) and proxy yes/no.
- [ ] **AWS**: S3 bucket name + IAM for app/EC2 when we add backup.
- [ ] **EC2**: When you’re ready: 100GB instance; we’ll give runbook + 7d purge spec.
- [ ] **Proxies**: Provider + config if we use them (optional).
- [ ] **Discord**: Bot/token when we build Discord crawler.
- [ ] **WhatsApp group**: Create and add Asha/students; share Cursor prompts for feedback.

---

## 5. Suggested next step (this week)

1. **You**: Confirm pilot cities, fresher pilot (y/n + keywords), and shortlist-200 approach (Section 4 checklist).
2. **Us**: Expand seed to 1k and run `run_discovery_phases.py`; add optional S3 export script (ready for when you give bucket).
3. **Us**: Add “simulation mode” to discovery (log-only, no outbound calls) and document how we’ll use it for 1-week review.

After that we can do shortlist 200 + fresher pilot, then task manager and crawlers in order above.
