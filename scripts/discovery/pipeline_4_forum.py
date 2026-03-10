"""
Pipeline 4 — Forum Discovery (Reddit, HN, Dev.to, Hashnode, IndieHackers).
site:reddit.com, site:news.ycombinator.com, etc. Output: same JSON schema.

Usage:
  cd jobLead && python scripts/discovery/pipeline_4_forum.py --simulation
  python scripts/discovery/pipeline_4_forum.py --delay 60
"""
import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.discovery.base import (
    DISCOVERY_JSON_PATH,
    JOBLEAD_ROOT,
    load_discovery_sources_json,
    save_discovery_sources_json,
    rate_limit_sleep,
    upsert_discovery_source_into_list,
    extract_domain,
)
from scripts.discovery.query_generator import pipeline_4_forum_queries
from scripts.discovery.run_search import duckduckgo_search

FORUM_DOMAINS = {"reddit.com", "news.ycombinator.com", "dev.to", "hashnode.dev", "indiehackers.com", "medium.com"}


def run(simulation: bool, delay_sec: float, max_queries: int | None, out_path: Path) -> tuple[int, int]:
    queries = pipeline_4_forum_queries()
    if max_queries:
        queries = queries[:max_queries]
    sources = load_discovery_sources_json(out_path)
    inserted, skipped = 0, 0

    for i, (query, type_hint) in enumerate(queries, 1):
        print(f"[{i}/{len(queries)}] {query!r} ... ", end="", flush=True)
        if simulation:
            print("(simulation)")
            continue
        rate_limit_sleep(delay_sec, simulation=False)
        try:
            urls = duckduckgo_search(query, max_results=25)
        except Exception as e:
            print(f"FAILED: {e}")
            continue
        urls = [u for u in urls if extract_domain(u) in FORUM_DOMAINS]
        print(f"{len(urls)} forum links")
        for u in urls:
            if upsert_discovery_source_into_list(
                sources,
                u,
                name="",
                source_type=type_hint,
                discovery_origin="forum",
            ):
                inserted += 1
            else:
                skipped += 1

    if not simulation:
        save_discovery_sources_json(sources, out_path)
    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description="Pipeline 4: Forum Discovery")
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--delay", type=float, default=60.0)
    parser.add_argument("--max-queries", type=int, default=None)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    out_path = (JOBLEAD_ROOT / args.out) if args.out else DISCOVERY_JSON_PATH
    if args.out and not str(args.out).startswith("/"):
        out_path = JOBLEAD_ROOT / args.out

    ins, sk = run(args.simulation, args.delay, args.max_queries, out_path)
    print(f"Inserted: {ins}, Skipped: {sk}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
