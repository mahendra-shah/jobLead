"""
Export crawl-ready sources from discovery_sources_test.json into a smaller JSON
that Phase-2 crawlers can consume.

Output: app/data/crawl_ready_sources.json
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.discovery.base import DISCOVERY_JSON_PATH, load_discovery_sources_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Export crawl-ready sources to a compact JSON file")
    parser.add_argument(
        "--in-file",
        type=Path,
        default=None,
        help="Input discovery JSON (default: discovery_sources_test.json)",
    )
    parser.add_argument(
        "--out-file",
        type=Path,
        default=Path("app/data/crawl_ready_sources.json"),
        help="Output JSON path",
    )
    args = parser.parse_args()

    in_path = args.in_file if args.in_file is not None else DISCOVERY_JSON_PATH
    if not in_path.is_absolute():
        in_path = JOBLEAD_ROOT / in_path

    out_path = args.out_file
    if not out_path.is_absolute():
        out_path = JOBLEAD_ROOT / out_path

    sources = load_discovery_sources_json(in_path)
    ready = []
    for s in sources:
        m = s.get("metadata") or {}
        cs = m.get("crawl_strategy") or {}
        if cs.get("crawl_ready"):
            ready.append(s)

    payload = {
        "meta": {
            "description": "Crawl-ready sources exported from discovery_sources_test.json",
            "exported_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total": len(ready),
        },
        "sources": ready,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Exported {len(ready)} crawl-ready sources -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

