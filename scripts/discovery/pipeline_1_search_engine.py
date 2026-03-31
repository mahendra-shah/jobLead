"""
Pipeline 1 — Search Engine Discovery.
Finds job boards, company pages, startup sites via search queries.
Output: discovery_sources_test.json (id, url, domain, type, city, country, confidence_score, ...).

Usage:
  cd jobLead && python scripts/discovery/pipeline_1_search_engine.py --simulation
  python scripts/discovery/pipeline_1_search_engine.py --delay 60 --max-queries 5
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
    infer_source_type,
)
from scripts.discovery.query_generator import get_all_pipeline_1_queries
from scripts.discovery.run_search import duckduckgo_search


def run(simulation: bool, delay_sec: float, max_queries: int | None, out_path: Path) -> tuple[int, int]:
    queries = get_all_pipeline_1_queries(max_per_category=max_queries)
    sources = load_discovery_sources_json(out_path)
    inserted, skipped = 0, 0

    for i, row in enumerate(queries, 1):
        query, city, country = row[0], row[1], row[2]
        print(f"[{i}/{len(queries)}] {query!r} ... ", end="", flush=True)
        if simulation:
            print("(simulation)")
            continue
        rate_limit_sleep(delay_sec, simulation=False)
        try:
            urls = duckduckgo_search(query, max_results=30)
        except Exception as e:
            print(f"FAILED: {e}")
            continue
        print(f"{len(urls)} results")
        for u in urls:
            st = infer_source_type(u)
            if upsert_discovery_source_into_list(
                sources,
                u,
                name="",
                source_type=st,
                city=city,
                country=country,
                discovery_origin="search_engine",
            ):
                inserted += 1
            else:
                skipped += 1

    if not simulation:
        save_discovery_sources_json(sources, out_path)
    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description="Pipeline 1: Search Engine Discovery")
    parser.add_argument("--simulation", action="store_true", help="Only log queries, no HTTP")
    parser.add_argument("--delay", type=float, default=60.0, help="Seconds between search requests")
    parser.add_argument("--max-queries", type=int, default=None, help="Cap number of queries (for testing)")
    parser.add_argument("--out", type=Path, default=None, help="Output JSON path")
    args = parser.parse_args()
    out_path = (JOBLEAD_ROOT / args.out) if args.out else DISCOVERY_JSON_PATH
    if args.out and not str(args.out).startswith("/"):
        out_path = JOBLEAD_ROOT / args.out

    ins, sk = run(args.simulation, args.delay, args.max_queries, out_path)
    print(f"Inserted: {ins}, Skipped (duplicate): {sk}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
