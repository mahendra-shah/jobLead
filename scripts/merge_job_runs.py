"""
Merge multiple jobs_run_*.json files into a single jobs_master.json,
deduplicating by job URL and computing per-source statistics.
Also writes top_sources_by_jobs.json for quick inspection.
"""
import json
import sys
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))


def find_job_run_files(jobs_dir: Path) -> list[Path]:
    return sorted(jobs_dir.glob("jobs_run_*.json"))


def main() -> int:
    jobs_dir = JOBLEAD_ROOT / "app" / "data" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    run_files = find_job_run_files(jobs_dir)
    if not run_files:
        print("No jobs_run_*.json files found.")
        return 0

    print(f"Found {len(run_files)} run file(s).")

    all_jobs: list[dict] = []
    seen_urls: set[str] = set()

    for f in run_files:
        data = json.loads(f.read_text(encoding="utf-8"))
        jobs = data.get("jobs") or []
        print(f"  {f.name}: {len(jobs)} jobs")
        for j in jobs:
            url = (j.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            all_jobs.append(j)

    print(f"Total unique jobs after merge: {len(all_jobs)}")

    # Per-source stats
    per_source = Counter(j.get("source_domain") or "unknown" for j in all_jobs)

    master_path = jobs_dir / "jobs_master.json"
    master_payload = {
        "meta": {
            "runs_merged": [f.name for f in run_files],
            "total_jobs_unique": len(all_jobs),
        },
        "jobs": all_jobs,
    }
    master_path.write_text(json.dumps(master_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote merged jobs -> {master_path}")

    top_sources = sorted(per_source.items(), key=lambda x: x[1], reverse=True)
    top_payload = {
        "meta": {
            "total_sources": len(per_source),
            "description": "Sources ranked by number of unique jobs in jobs_master.json",
        },
        "sources": [
            {"source_domain": domain, "job_count": count} for domain, count in top_sources
        ],
    }
    top_path = jobs_dir / "top_sources_by_jobs.json"
    top_path.write_text(json.dumps(top_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote per-source stats -> {top_path}")

    # Print top 10 summary to console
    print("Top 10 sources by job count:")
    for domain, count in top_sources[:10]:
        print(f"  {domain:30} {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

