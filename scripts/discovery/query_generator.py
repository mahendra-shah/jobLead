"""
Phase 1 Discovery: query generation for all 4 pipelines.
Returns list of query strings (and optional metadata) for search engines / GitHub.
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.discovery.base import load_pilot_cities, load_fresher_keywords


def pipeline_1_search_engine_queries() -> list[tuple[str, str | None, str | None]]:
    """
    Pipeline 1 — Search Engine Discovery.
    Returns (query, city, country) for each. city/country used when adding to sources.
    Examples: "python jobs bangalore", "software engineer jobs pune", "backend jobs india",
    "startup hiring india", "fresher developer jobs"; then site: queries.
    """
    cities = load_pilot_cities()
    india = cities.get("india") or []
    outside = cities.get("outside_india") or []

    # (keyword_template, city, country)
    rows = []

    # Tech: stacks you care about (HTML/CSS/JS, React, MERN/PERN, Python)
    tech_roles = [
        "html css javascript jobs",
        "frontend developer jobs",
        "react developer jobs",
        "javascript developer jobs",
        "mern stack developer jobs",
        "mern developer jobs",
        "pern stack developer jobs",
        "nodejs react jobs",
        "python developer jobs",
        "python jobs",
        "full stack developer jobs",
        "software engineer jobs",
        "backend developer jobs",
        "web developer jobs",
        "startup hiring developers",
        "fresher react developer jobs",
        "entry level python developer jobs",
    ]
    # Non-tech: digital marketing, analytics, CRM, HR/recruiters
    non_tech_roles = [
        "digital marketing jobs",
        "performance marketing jobs",
        "seo specialist jobs",
        "social media marketing jobs",
        "data analytics jobs",
        "business analyst data jobs",
        "crm jobs",
        "salesforce jobs",
        "zoho crm jobs",
        "hr jobs",
        "hr executive jobs",
        "recruiter jobs",
        "talent acquisition jobs",
        "technical recruiter jobs",
        "marketing jobs",
        "sales jobs",
        "finance jobs",
        "accounting jobs",
        "operations jobs",
        "project manager jobs",
        "customer support jobs",
        "customer success jobs",
        "call center jobs",
        "bpo jobs",
        "content writer jobs",
        "graphic designer jobs",
        "admin jobs",
        "office assistant jobs",
        "data entry jobs",
        "fresher jobs",
        "entry level jobs",
        "graduate jobs",
    ]

    # Employment + location modifiers (paired in india_wide_queries below)
    # Bias discovery toward non-tech first (more non-tech board discovery).
    role_kw = non_tech_roles + tech_roles

    # India: non-tech first, then stack-specific tech
    for city in india[:15]:  # cap for pilot
        for kw in role_kw[:18]:
            rows.append((f"{kw} {city}", city, "India"))
    for city in outside[:10]:
        for kw in non_tech_roles[:8] + tech_roles[:5]:
            rows.append((f"{kw} {city}", city, None))

    # India-wide: full-time / part-time / internship / remote / on-site
    india_wide = [
        # Non-tech + employment / location
        ("digital marketing jobs india full time", None, "India"),
        ("digital marketing internship india", None, "India"),
        ("digital marketing jobs india remote", None, "India"),
        ("data analytics jobs india", None, "India"),
        ("data analytics internship india", None, "India"),
        ("data analyst jobs india work from home", None, "India"),
        ("crm jobs india", None, "India"),
        ("salesforce jobs india remote", None, "India"),
        ("hr jobs india full time", None, "India"),
        ("recruiter jobs india", None, "India"),
        ("technical recruiter jobs india remote", None, "India"),
        ("marketing jobs india remote", None, "India"),
        ("sales jobs india work from office", None, "India"),
        ("customer support jobs india", None, "India"),
        ("data entry jobs india part time", None, "India"),
        ("fresher jobs india", None, "India"),
        ("graduate trainee jobs india", None, "India"),
        ("internship jobs india", None, "India"),
        # Tech stacks + employment / location
        ("react developer jobs india remote", None, "India"),
        ("mern stack jobs india", None, "India"),
        ("pern stack jobs india", None, "India"),
        ("python developer jobs india full time", None, "India"),
        ("python internship india", None, "India"),
        ("javascript developer jobs india", None, "India"),
        ("html css jobs india", None, "India"),
        ("frontend developer jobs india hybrid", None, "India"),
        ("full stack developer jobs india remote", None, "India"),
        ("nodejs developer jobs india", None, "India"),
        ("web developer internship india", None, "India"),
        ("fresher developer jobs india", None, "India"),
        ("startup hiring india", None, "India"),
        ("tech jobs india", None, "India"),
    ]
    rows.extend(india_wide)

    # Site-specific (for Pipeline 3/4 we also use these; here we get generic results)
    site_queries = [
        # Tech communities / boards
        'site:t.me developer jobs',
        'site:t.me react jobs india',
        'site:t.me python jobs',
        'site:t.me internship developer',
        'site:t.me hiring engineers',
        'site:discord.gg jobs developer',
        'site:discord.gg developer jobs',
        'site:github.com "job board"',
        'site:github.com "remote jobs"',
        'site:github.com mern jobs',
        'site:medium.com hiring engineer',
        'site:reddit.com "jobs india developer"',
        'site:reddit.com react jobs india',
        'site:reddit.com python jobs india',
        'site:news.ycombinator.com hiring',
        # Non-tech communities / boards
        'site:t.me marketing jobs',
        'site:t.me digital marketing jobs',
        'site:t.me data analytics jobs',
        'site:t.me sales jobs',
        'site:t.me hr jobs',
        'site:t.me fresher jobs',
        'site:discord.gg marketing jobs',
        'site:reddit.com "marketing jobs india"',
        'site:reddit.com "digital marketing jobs india"',
        'site:reddit.com "data analytics jobs india"',
        'site:reddit.com "sales jobs india"',
        'site:reddit.com "customer support jobs india"',
        'site:reddit.com "hr jobs india"',
    ]
    for q in site_queries:
        rows.append((q, None, None))

    return rows


def pipeline_2_github_queries() -> list[str]:
    """
    Pipeline 2 — GitHub Discovery.
    Search queries for finding repos (e.g. awesome lists). Used with GitHub API or known list URLs.
    Examples: "job boards" language:markdown, "telegram jobs" language:markdown.
    """
    return [
        "job boards language:markdown",
        "telegram job groups language:markdown",
        "developer communities language:markdown",
        "remote jobs language:markdown",
        "awesome job boards language:markdown",
        "india dev communities language:markdown",
        "hiring lists language:markdown",
        "react developer jobs language:markdown",
        "internship india jobs language:markdown",
        "digital marketing jobs language:markdown",
        "data analytics careers language:markdown",
        "crm salesforce jobs language:markdown",
    ]


def pipeline_3_community_queries() -> list[tuple[str, str]]:
    """
    Pipeline 3 — Community Discovery (Telegram, Discord, Slack).
    Returns (query, type_hint). type_hint = telegram | discord.
    """
    base = [
        # Tech
        "developer jobs",
        "react jobs",
        "mern jobs",
        "python jobs",
        "javascript jobs",
        "internship developer",
        "hiring engineers",
        "tech jobs",
        "backend jobs",
        "startup jobs",
        # Non-tech
        "digital marketing jobs",
        "data analytics jobs",
        "crm jobs",
        "marketing jobs",
        "sales jobs",
        "hr jobs",
        "recruiter jobs",
        "customer support jobs",
        "fresher jobs",
    ]
    rows = []
    for kw in base:
        rows.append((f"site:t.me {kw}", "telegram"))
        rows.append((f"site:telegram.me {kw}", "telegram"))
        rows.append((f"site:discord.gg {kw}", "discord"))
    return rows


def pipeline_4_forum_queries() -> list[tuple[str, str]]:
    """
    Pipeline 4 — Forum Discovery.
    Returns (query, type_hint). type_hint = forum.
    """
    return [
        # Tech-focused
        ('site:reddit.com "jobs india developer"', "forum"),
        ('site:reddit.com "hiring" india', "forum"),
        ("site:news.ycombinator.com hiring", "forum"),
        ("site:dev.to jobs", "forum"),
        ("site:hashnode.dev hiring developer", "forum"),
        ("site:indiehackers.com jobs", "forum"),
        # Non-tech focused
        ('site:reddit.com "marketing jobs india"', "forum"),
        ('site:reddit.com "digital marketing jobs india"', "forum"),
        ('site:reddit.com "data analytics jobs india"', "forum"),
        ('site:reddit.com "sales jobs india"', "forum"),
        ('site:reddit.com \"customer support jobs\" india', "forum"),
        ('site:reddit.com internship india developer', "forum"),
    ]


def get_all_pipeline_1_queries(max_per_category: int | None = None) -> list[tuple[str, str | None, str | None]]:
    """All Pipeline 1 queries. Optional cap for testing."""
    q = pipeline_1_search_engine_queries()
    if max_per_category:
        q = q[:max_per_category]
    return q
