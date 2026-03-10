"""
Pipeline 2 — GitHub Discovery.
Finds sources from GitHub list READMEs (awesome-job-boards, remote jobs, etc.).
Uses app/data/github_discovery_lists.json. Output: same JSON schema as Pipeline 1.

Usage:
  cd jobLead && python scripts/discovery/pipeline_2_github.py
  python scripts/discovery/pipeline_2_github.py --delay 5 --dry-run
"""
import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from scripts.discovery.base import DISCOVERY_JSON_PATH
from scripts.discovery.github_discovery import run

def main():
    parser = argparse.ArgumentParser(description="Pipeline 2: GitHub Discovery")
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    out_path = DISCOVERY_JSON_PATH if args.out is None else (args.out if args.out.is_absolute() else JOBLEAD_ROOT / args.out)
    ins, sk = run(args.delay, args.dry_run, out_path)
    print(f"Inserted: {ins}, Skipped: {sk}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
