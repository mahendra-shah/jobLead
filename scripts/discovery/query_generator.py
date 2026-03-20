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

    # Tech roles + city
    tech_roles = [
        "python jobs",
        "software engineer jobs",
        "backend jobs",
        "frontend jobs",
        "developer jobs",
        "full stack jobs",
        "startup hiring",
        "fresher developer jobs",
        "entry level developer jobs",
    ]
    # Non-tech roles + city (Phase 1 includes tech + non-tech)
    non_tech_roles = [
        "marketing jobs",
        "sales jobs",
        "hr jobs",
        "finance jobs",
        "accounting jobs",
        "operations jobs",
        "business analyst jobs",
        "project manager jobs",
        "customer support jobs",
        "customer success jobs",
        "call center jobs",
        "bpo jobs",
        "content writer jobs",
        "graphic designer jobs",
        "digital marketing jobs",
        "social media jobs",
        "seo jobs",
        "admin jobs",
        "office assistant jobs",
        "data entry jobs",
        "recruiter jobs",
        "talent acquisition jobs",
        "fresher jobs",
        "entry level jobs",
        "graduate jobs",
    ]

    # Bias discovery toward non-tech first (so Phase-2 crawling
    # produces more of the non-tech roles you want).
    role_kw = non_tech_roles + tech_roles

    for city in india[:15]:  # cap for pilot
        for kw in role_kw[:8]:
            rows.append((f"{kw} {city}", city, "India"))
    for city in outside[:10]:
        for kw in tech_roles[:4] + non_tech_roles[:4]:
            rows.append((f"{kw} {city}", city, None))

    # India-wide
    rows.append(("backend jobs india", None, "India"))
    rows.append(("startup hiring india", None, "India"))
    rows.append(("fresher developer jobs india", None, "India"))
    rows.append(("tech jobs india", None, "India"))

    # Site-specific (for Pipeline 3/4 we also use these; here we get generic results)
    site_queries = [
        # Tech communities / boards
        'site:t.me developer jobs',
        'site:t.me hiring engineers',
        'site:discord.gg jobs developer',
        'site:discord.gg developer jobs',
        'site:github.com "job board"',
        'site:github.com "remote jobs"',
        'site:medium.com hiring engineer',
        'site:reddit.com "jobs india developer"',
        'site:news.ycombinator.com hiring',
        # Non-tech communities / boards
        'site:t.me marketing jobs',
        'site:t.me sales jobs',
        'site:t.me hr jobs',
        'site:t.me fresher jobs',
        'site:discord.gg marketing jobs',
        'site:reddit.com "marketing jobs india"',
        'site:reddit.com "sales jobs india"',
        'site:reddit.com "customer support jobs india"',
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
    ]


def pipeline_3_community_queries() -> list[tuple[str, str]]:
    """
    Pipeline 3 — Community Discovery (Telegram, Discord, Slack).
    Returns (query, type_hint). type_hint = telegram | discord.
    """
    base = [
        # Tech
        "developer jobs",
        "hiring engineers",
        "tech jobs",
        "python jobs",
        "backend jobs",
        "startup jobs",
        # Non-tech
        "marketing jobs",
        "sales jobs",
        "hr jobs",
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
        ('site:reddit.com "sales jobs india"', "forum"),
        ('site:reddit.com \"customer support jobs\" india', "forum"),
    ]


def get_all_pipeline_1_queries(max_per_category: int | None = None) -> list[tuple[str, str | None, str | None]]:
    """All Pipeline 1 queries. Optional cap for testing."""
    q = pipeline_1_search_engine_queries()
    if max_per_category:
        q = q[:max_per_category]
    return q
