"""
Discovery service: shortlist heuristics and sync shortlisted Telegram to telegram_groups.
"""
import json
import re
import os
from typing import List, Tuple

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.discovery_source import DiscoverySource
from app.models.telegram_group import TelegramGroup


# Heuristic keywords (name/url) that suggest job-related source
SHORTLIST_KEYWORDS = [
    "job", "jobs", "hiring", "career", "careers", "fresher", "freshers",
    "off campus", "offcampus", "internship", "placement", "recruitment",
    "developer", "tech", "engineer", "opening", "vacancy", "vacancies",
]


def _shortlist_score(name: str, url: str, source_type: str) -> int:
    """Score 0-100; higher = more likely job-related."""
    text = f"{(name or '').lower()} {(url or '').lower()}"
    score = 0
    for kw in SHORTLIST_KEYWORDS:
        if kw in text:
            score += 10
    if source_type in ("telegram_channel", "job_board"):
        score += 15
    if source_type == "community":
        score += 5
    return min(100, score)


def extract_telegram_username(url: str) -> str | None:
    """Extract username from telegram.me/xxx or t.me/xxx."""
    if not url:
        return None
    url = url.strip().lower()
    for prefix in ("https://telegram.me/", "https://t.me/", "http://telegram.me/", "http://t.me/", "telegram.me/", "t.me/"):
        if url.startswith(prefix) or url.startswith(prefix.replace("https://", "")):
            rest = url.replace(prefix, "").replace("http://", "").split("/")[0].split("?")[0]
            if rest:
                return rest
    if "t.me/" in url:
        rest = url.split("t.me/")[-1].split("/")[0].split("?")[0]
        if rest:
            return rest
    return None


async def run_shortlist_heuristics(db: AsyncSession, max_shortlist: int = 200) -> Tuple[int, int]:
    """
    Mark up to max_shortlist sources as shortlisted by heuristic score.
    Returns (shortlisted_count, total_sources).
    """
    result = await db.execute(select(DiscoverySource))
    all_sources = result.scalars().all()
    total = len(all_sources)

    scored = [(s, _shortlist_score(s.name, s.url, s.source_type)) for s in all_sources]
    scored.sort(key=lambda x: -x[1])

    # Reset all shortlisted first
    for s in all_sources:
        s.is_shortlisted = False

    count = 0
    for source, score in scored:
        if count >= max_shortlist or score < 5:
            break
        source.is_shortlisted = True
        count += 1

    return count, total


async def sync_shortlisted_telegram_to_groups(db: AsyncSession) -> Tuple[int, int, List[str]]:
    """
    Create/update telegram_groups from shortlisted discovery_sources with source_type=telegram_channel.
    Returns (synced, skipped, errors).
    """
    result = await db.execute(
        select(DiscoverySource).where(
            DiscoverySource.is_shortlisted.is_(True),
            DiscoverySource.source_type == "telegram_channel",
        )
    )
    sources = result.scalars().all()
    synced, skipped, errors = 0, 0, []

    for s in sources:
        username = extract_telegram_username(s.url)
        if not username:
            errors.append(f"Could not extract username from {s.url}")
            skipped += 1
            continue
        if not username.replace("_", "").isalnum():
            skipped += 1
            continue
        existing = await db.execute(select(TelegramGroup).where(TelegramGroup.username == username).limit(1))
        tg = existing.scalar_one_or_none()
        if tg:
            tg.title = tg.title or s.name
            tg.url = s.url
            tg.notes = (tg.notes or "") + "; from_discovery"
            skipped += 1
            continue
        try:
            tg = TelegramGroup(
                username=username,
                title=s.name,
                url=s.url,
                is_active=True,
                is_joined=False,
                notes="from_discovery",
            )
            db.add(tg)
            synced += 1
        except Exception as e:
            errors.append(f"{s.url}: {e}")
            skipped += 1

    return synced, skipped, errors


def load_pilot_cities() -> dict:
    path = os.path.join(os.path.dirname(__file__), "..", "data", "pilot_cities.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_fresher_keywords() -> dict:
    path = os.path.join(os.path.dirname(__file__), "..", "data", "fresher_keywords.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
