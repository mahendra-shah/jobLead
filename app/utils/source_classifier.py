from __future__ import annotations

from dataclasses import dataclass


POPULAR_SOURCE_TOKENS: tuple[str, ...] = (
    "internshala.com",
    "linkedin.com",
    "naukri.com",
    "foundit.in",
    "ambitionbox.com",
    "jobsora.com",
    "cutshort.io",
    "indeed.",
    "glassdoor.",
)


PROMO_PATH_TOKENS: tuple[str, ...] = (
    "/blog",
    "/blogs",
    "/category",
    "/categories",
    "/resources",
    "/events",
    "/about",
    "/contact",
    "/privacy",
    "/terms",
    "/pricing",
    "/login",
    "/signup",
    "/register",
)


@dataclass(frozen=True)
class SourceClass:
    label: str  # popular | niche
    is_popular: bool
    has_promo_pattern: bool


def classify_source(domain: str, url: str = "", name: str = "") -> SourceClass:
    d = (domain or "").strip().lower()
    u = (url or "").strip().lower()
    n = (name or "").strip().lower()
    hay = " ".join([d, u, n])

    is_popular = any(tok in hay for tok in POPULAR_SOURCE_TOKENS)
    has_promo_pattern = any(tok in u for tok in PROMO_PATH_TOKENS)
    return SourceClass(
        label="popular" if is_popular else "niche",
        is_popular=is_popular,
        has_promo_pattern=has_promo_pattern,
    )

