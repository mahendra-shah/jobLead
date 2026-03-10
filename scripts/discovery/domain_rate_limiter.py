import time
from urllib.parse import urlparse
from collections import defaultdict

# Per-domain minimum delay (seconds) between requests.
DOMAIN_LIMITS = {
    "linkedin.com": 30.0,
    "indeed.com": 60.0,
    "github.com": 10.0,
}

# Default delay for all other domains.
DEFAULT_DELAY = 3.0

_last_request_at = defaultdict(float)


def get_domain(url: str) -> str:
    """Extract netloc from URL in lowercase."""
    return (urlparse(url).netloc or "").lower()


def rate_limit_before_request(url: str) -> None:
    """
    Sleep just enough so that consecutive requests to the same domain
    respect the DOMAIN_LIMITS / DEFAULT_DELAY.
    """
    domain = get_domain(url)
    delay = DOMAIN_LIMITS.get(domain, DEFAULT_DELAY)

    now = time.time()
    last = _last_request_at.get(domain, 0.0)

    wait = last + delay - now
    if wait > 0:
        time.sleep(wait)

    _last_request_at[domain] = time.time()

