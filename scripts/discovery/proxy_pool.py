"""
Simple proxy hook for discovery/crawling.

By default PROXY_POOL is empty, so no proxies are used.
Later you can add entries like "http://user:pass@ip:port".
"""

PROXY_POOL: list[str] = []
_index: int = 0


def get_next_proxy() -> dict | None:
    """
    Round-robin select next proxy from PROXY_POOL.
    Returns httpx proxies mapping, or None if pool is empty.
    """
    global _index
    if not PROXY_POOL:
        return None
    proxy = PROXY_POOL[_index % len(PROXY_POOL)]
    _index += 1
    return {
        "http://": proxy,
        "https://": proxy,
    }

