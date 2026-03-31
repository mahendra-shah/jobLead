"""
Discover job-board and website sources from GitHub list READMEs; write to JSON (testing).
Uses app/data/github_discovery_lists.json for list URLs. No GitHub API needed (raw content).
When satisfied, import app/data/discovery_sources_test.json to DB.

Usage:
  cd jobLead && python scripts/discovery/github_discovery.py
  python scripts/discovery/github_discovery.py --delay 5 --dry-run --out app/data/my_sources.json
"""
import argparse
import re
import sys
from pathlib import Path

import httpx

# Ensure jobLead on path
SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.discovery.base import (
    DATA_DIR,
    DISCOVERY_JSON_PATH,
    JOBLEAD_ROOT,
    load_discovery_sources_json,
    save_discovery_sources_json,
    rate_limit_sleep,
    upsert_discovery_source_into_list,
)
from scripts.discovery.domain_rate_limiter import rate_limit_before_request
from scripts.discovery.proxy_pool import get_next_proxy
REQUEST_TIMEOUT = 30.0
# Markdown link: [text](url)
LINK_RE = re.compile(r"\[([^\]]*)\]\((https?://[^)\s]+)\)")


def load_list_config() -> list:
    path = DATA_DIR / "github_discovery_lists.json"
    if not path.exists():
        return []
    import json
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("lists") or []


def is_job_like_url(url: str) -> bool:
    u = url.lower()
    if any(x in u for x in ["github.com", "twitter.com", "linkedin.com", "wikipedia.org", "raw.githubusercontent"]):
        return False
    if any(x in u for x in ["/job", "/jobs", "/career", "/careers", "/hire", "/hiring", "/position", "/opening"]):
        return True
    if "job" in u or "career" in u or "recruit" in u:
        return True
    return False


def extract_links_from_markdown(content: str, list_id: str) -> list[tuple[str, str]]:
    """Return (name, url) for links that look like job/career pages."""
    seen = set()
    out = []
    for name, url in LINK_RE.findall(content):
        url = url.split(")")[0].rstrip()
        if "?" in url:
            url = url.split("?")[0]
        if not url.startswith(("http://", "https://")):
            continue
        if url in seen:
            continue
        seen.add(url)
        if is_job_like_url(url) or list_id == "remoteintech":
            name = (name or "Link").strip()[:500]
            out.append((name, url))
    return out


def run(delay_seconds: float, dry_run: bool, out_path: Path) -> tuple[int, int]:
    lists = load_list_config()
    if not lists:
        print("No lists in app/data/github_discovery_lists.json")
        return 0, 0

    sources = load_discovery_sources_json(out_path)
    inserted, skipped = 0, 0

    proxies = get_next_proxy()
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, proxies=proxies) as client:
        for entry in lists:
            list_id = entry.get("id") or "unknown"
            url = entry.get("url", "").strip()
            if not url:
                continue
            print(f"Fetching {list_id} ... ", end="", flush=True)
            rate_limit_sleep(delay_seconds, simulation=False)
            try:
                rate_limit_before_request(url)
                resp = client.get(url)
                resp.raise_for_status()
                content = resp.text
            except Exception as e:
                print(f"FAILED: {e}")
                continue
            pairs = extract_links_from_markdown(content, list_id)
            print(f"{len(pairs)} links")
            for name, link_url in pairs:
                if dry_run:
                    inserted += 1
                    continue
                if upsert_discovery_source_into_list(
                    sources,
                    link_url,
                    name=name,
                    source_type="job_board",
                    metadata_extra={"github_list_id": list_id},
                    discovery_origin="github",
                ):
                    inserted += 1
                else:
                    skipped += 1

    if not dry_run:
        save_discovery_sources_json(sources, out_path)

    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description="Discover sources from GitHub list READMEs (output: JSON)")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between list fetches")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output")
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path (default: app/data/discovery_sources_test.json)")
    args = parser.parse_args()
    out_path = DISCOVERY_JSON_PATH if args.out is None else (args.out if args.out.is_absolute() else JOBLEAD_ROOT / args.out)

    ins, sk = run(args.delay, args.dry_run, out_path)
    print(f"Inserted: {ins}, Skipped (already present): {sk}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
