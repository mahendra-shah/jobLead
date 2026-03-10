"""
Shared search runner: run a query against DuckDuckGo HTML, return list of result URLs.
Used by Pipeline 1, 3, 4. Rate-limit externally.
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.discovery.domain_rate_limiter import rate_limit_before_request
from scripts.discovery.proxy_pool import get_next_proxy

REQUEST_TIMEOUT = 25.0
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (PlacementDiscovery/1.0)",
    "Accept": "text/html,application/xhtml+xml",
}


def duckduckgo_search(query: str, max_results: int = 30) -> list[str]:
    """Return result URLs from DuckDuckGo HTML, with per-domain rate limiting and optional proxy."""
    import httpx
    from bs4 import BeautifulSoup

    url = "https://html.duckduckgo.com/html/"
    rate_limit_before_request(url)
    proxies = get_next_proxy()

    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, proxies=proxies) as client:
        resp = client.post(
            url,
            data={"q": query},
            headers=BROWSER_HEADERS,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

    urls: list[str] = []
    for a in soup.select("a.result__a"):
        href = a.get("href")
        if href and href.startswith("http") and "duckduckgo.com" not in href:
            urls.append(href)
            if len(urls) >= max_results:
                break
    return urls

