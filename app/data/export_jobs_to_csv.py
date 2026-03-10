#!/usr/bin/env python3
"""One-off script: Export scraped_jobs_from_sources.json to CSV for non-tech sharing."""
import csv
import json
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent
JSON_PATH = DATA_DIR / "scraped_jobs_from_sources.json"
CSV_PATH = DATA_DIR / "scraped_jobs_for_team.csv"

def main():
    with open(JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    jobs = data.get("jobs", [])
    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["#", "Job Title", "Job Link", "Source"])
        for i, job in enumerate(jobs, 1):
            writer.writerow([
                i,
                job.get("title", ""),
                job.get("url", ""),
                job.get("source_name", ""),
            ])
    print(f"Exported {len(jobs)} jobs to {CSV_PATH}")

if __name__ == "__main__":
    main()
