"""
Phase 1 Discovery: query generation for all 4 pipelines.
Returns list of query strings (and optional metadata) for search engines / GitHub.
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.job_board_flow.discovery.base import load_pilot_cities, load_fresher_keywords


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

    tech_roles = [
        "web designer jobs",
        "front-end developer jobs",
    ]
    non_tech_roles = [
        "digital marketing executive jobs",
        "digital marketing specialist jobs",
        "crm executive jobs",
        "crm manager jobs",
        "performance marketing specialist jobs",
        "marketing automation specialist jobs",
        "seo specialist jobs",
        "business analyst entry level jobs",
        "ecommerce manager jobs",
        "freelancer jobs",
        "fresher jobs",
        "entry level jobs",
    ]

    # Employment + location modifiers (paired in india_wide_queries below)
    # Bias discovery toward non-tech first (more non-tech board discovery).
    role_kw = non_tech_roles + tech_roles

    # India: non-tech first, then stack-specific tech
    for city in india[:15]:  # cap for pilot
        for kw in role_kw[:18]:
            rows.append((f"{kw} {city}", city, "India"))
    for city in outside[:10]:
        for kw in non_tech_roles[:8] + tech_roles[:2]:
            rows.append((f"{kw} {city}", city, None))

    # India-wide: full-time / part-time / internship / remote / on-site
    india_wide = [
        ("digital marketing executive jobs india fresher", None, "India"),
        ("digital marketing specialist jobs india fresher", None, "India"),
        ("crm executive jobs india fresher", None, "India"),
        ("crm manager jobs india entry level", None, "India"),
        ("performance marketing specialist jobs india fresher", None, "India"),
        ("web designer jobs india fresher", None, "India"),
        ("front-end developer jobs india fresher", None, "India"),
        ("marketing automation specialist jobs india fresher", None, "India"),
        ("seo specialist jobs india fresher", None, "India"),
        ("business analyst entry level jobs india", None, "India"),
        ("ecommerce manager jobs india fresher", None, "India"),
        ("freelancer marketing jobs india", None, "India"),
        ("freelancer web designer jobs india", None, "India"),
        ("fresher jobs india digital marketing", None, "India"),
        ("entry level jobs india crm", None, "India"),
    ]
    rows.extend(india_wide)

    # Site-specific (for Pipeline 3/4 we also use these; here we get generic results)
    site_queries = [
        'site:t.me digital marketing jobs',
        'site:t.me seo specialist jobs',
        'site:t.me performance marketing jobs',
        'site:t.me crm jobs',
        'site:t.me front-end developer jobs',
        'site:t.me web designer jobs',
        'site:t.me business analyst fresher jobs',
        'site:t.me ecommerce jobs',
        'site:t.me freelancer jobs',
        'site:discord.gg jobs developer',
        'site:discord.gg developer jobs',
        'site:github.com "job board"',
        'site:github.com "remote jobs"',
        'site:medium.com hiring engineer',
        'site:reddit.com "digital marketing jobs india"',
        'site:reddit.com "crm jobs india"',
        'site:reddit.com "seo jobs india"',
        'site:reddit.com "front-end developer jobs india"',
        'site:reddit.com "business analyst fresher jobs india"',
        'site:news.ycombinator.com hiring',
        'site:discord.gg marketing jobs',
        'site:t.me fresher jobs',
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
        "marketing communities language:markdown",
        "remote jobs language:markdown",
        "awesome job boards language:markdown",
        "india marketing communities language:markdown",
        "hiring lists language:markdown",
        "digital marketing jobs language:markdown",
        "crm jobs language:markdown",
        "performance marketing jobs language:markdown",
        "seo jobs language:markdown",
        "front-end developer jobs language:markdown",
        "web designer jobs language:markdown",
        "business analyst fresher jobs language:markdown",
        "ecommerce manager jobs language:markdown",
        "freelancer jobs language:markdown",
    ]


def pipeline_3_community_queries() -> list[tuple[str, str]]:
    """
    Pipeline 3 — Community Discovery (Telegram, Discord, Slack).
    Returns (query, type_hint). type_hint = telegram | discord.
    """
    base = [
        "digital marketing executive jobs",
        "digital marketing specialist jobs",
        "crm executive jobs",
        "crm manager jobs",
        "performance marketing specialist jobs",
        "web designer jobs",
        "front-end developer jobs",
        "marketing automation specialist jobs",
        "seo specialist jobs",
        "business analyst entry level jobs",
        "ecommerce manager jobs",
        "freelancer jobs",
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
    local_city_forum_queries = [
        ('site:reddit.com "digital marketing" "bangalore" "fresher"', "forum"),
        ('site:reddit.com "seo specialist" "mumbai" "job"', "forum"),
        ('site:reddit.com "front-end developer" "delhi" "entry level"', "forum"),
        ('site:reddit.com "business analyst" "hyderabad" "fresher"', "forum"),
        ('site:reddit.com "ecommerce manager" "pune" "job"', "forum"),
        ('site:reddit.com "crm executive" "chennai" "job"', "forum"),
        ('site:quora.com "digital marketing jobs bangalore"', "forum"),
        ('site:quora.com "seo jobs mumbai fresher"', "forum"),
        ('site:quora.com "frontend developer jobs delhi"', "forum"),
        ('site:teamblind.com "digital marketing jobs india"', "forum"),
        ('site:internals forum "digital marketing jobs bangalore"', "forum"),
        ('site:discuss "seo jobs india" "fresher"', "forum"),
        ('inurl:forum "digital marketing jobs" "bangalore"', "forum"),
        ('inurl:community "crm jobs" "india"', "forum"),
        ('inurl:discussion "front-end developer jobs" "india"', "forum"),
        ('"digital marketing jobs" "HSR Layout" "Bangalore"', "forum"),
        ('"seo jobs" "Andheri" "Mumbai"', "forum"),
        ('"front-end developer jobs" "Saket" "Delhi"', "forum"),
        ('"business analyst fresher jobs" "Madhapur" "Hyderabad"', "forum"),
        ('"ecommerce jobs" "Kothrud" "Pune"', "forum"),
    ]

    return [
        ('site:reddit.com "hiring" india', "forum"),
        ("site:news.ycombinator.com hiring", "forum"),
        ("site:dev.to marketing jobs", "forum"),
        ("site:hashnode.dev front-end jobs", "forum"),
        ("site:indiehackers.com jobs", "forum"),
        ('site:reddit.com "marketing jobs india"', "forum"),
        ('site:reddit.com "digital marketing jobs india"', "forum"),
        ('site:reddit.com "crm jobs india"', "forum"),
        ('site:reddit.com "seo jobs india"', "forum"),
        ('site:reddit.com "front-end developer jobs india"', "forum"),
        ('site:reddit.com "business analyst fresher jobs india"', "forum"),
    ] + local_city_forum_queries


def get_all_pipeline_1_queries(max_per_category: int | None = None) -> list[tuple[str, str | None, str | None]]:
    """All Pipeline 1 queries. Optional cap for testing."""
    q = pipeline_1_search_engine_queries()
    if max_per_category:
        q = q[:max_per_category]
    return q
