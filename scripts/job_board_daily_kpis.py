#!/usr/bin/env python3
"""Daily KPI snapshot for job-board pipeline (IST day window)."""

from __future__ import annotations

import sys
from datetime import timezone
from pathlib import Path

from sqlalchemy import inspect, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.job_board_ml.postgres_sync import build_sync_engine
from app.services.mongodb_job_ingest_service import MongoJobIngestService
from app.utils.timezone import ist_today_utc_window


def main() -> int:
    start_utc, end_utc, ist_date = ist_today_utc_window()
    start_aware = start_utc.replace(tzinfo=timezone.utc)
    end_aware = end_utc.replace(tzinfo=timezone.utc)

    # 1) Crawl coverage (Mongo job_ingest created today)
    svc = MongoJobIngestService()
    svc._ensure_indexes()
    col = svc._col
    assert col is not None
    q_today = {"created_at": {"$gte": start_aware, "$lt": end_aware}}
    jobs_today = int(col.count_documents(q_today))
    domains = set()
    for d in col.find(q_today, {"payload.source_domain": 1, "source_ref.source_domain": 1}):
        p = d.get("payload") or {}
        sr = d.get("source_ref") or {}
        dom = str(p.get("source_domain") or sr.get("source_domain") or "").strip().lower()
        if dom:
            domains.add(dom)
    sources_today = len(domains)

    # 2..4) Postgres sharing + matching + student coverage
    engine = build_sync_engine()
    insp = inspect(engine)
    tables = set(insp.get_table_names())
    out_shortlisted = {"app_shortlisted": 0, "jobs_shortlisted": 0, "students_shortlisted": 0}
    out_shared = {"notifications_sent": 0, "distinct_jobs_shared": 0, "students_reached": 0}
    out_match = {"recommendation_rows": 0, "students_matched": 0, "matched_jobs": 0}
    out_cov = {"active_students": 0, "students_reached_today": 0, "students_without_jobs_today": 0}

    with engine.connect() as conn:
        if {"applications", "jobs"}.issubset(tables):
            r = conn.execute(
                text(
                    """
                    SELECT
                      COUNT(*) AS app_shortlisted,
                      COUNT(DISTINCT a.job_id) AS jobs_shortlisted,
                      COUNT(DISTINCT a.student_id) AS students_shortlisted
                    FROM applications a
                    JOIN jobs j ON j.id = a.job_id
                    WHERE a.status = 'shortlisted'
                      AND j.source = 'job_board'
                      AND a.created_at >= :s AND a.created_at < :e
                    """
                ),
                {"s": start_utc, "e": end_utc},
            ).mappings().first()
            if r:
                out_shortlisted = {k: int(v or 0) for k, v in dict(r).items()}

        if {"student_notifications", "jobs"}.issubset(tables):
            r = conn.execute(
                text(
                    """
                    SELECT
                      COUNT(*) AS notifications_sent,
                      COUNT(DISTINCT n.job_id) AS distinct_jobs_shared,
                      COUNT(DISTINCT n.student_id) AS students_reached
                    FROM student_notifications n
                    JOIN jobs j ON j.id = n.job_id
                    WHERE n.type = 'new_job'
                      AND j.source = 'job_board'
                      AND n.created_at >= :s AND n.created_at < :e
                    """
                ),
                {"s": start_utc, "e": end_utc},
            ).mappings().first()
            if r:
                out_shared = {k: int(v or 0) for k, v in dict(r).items()}

        if {"job_recommendations", "jobs"}.issubset(tables):
            r = conn.execute(
                text(
                    """
                    SELECT
                      COUNT(*) AS recommendation_rows,
                      COUNT(DISTINCT r.student_id) AS students_matched,
                      COUNT(DISTINCT r.job_id) AS matched_jobs
                    FROM job_recommendations r
                    JOIN jobs j ON j.id = r.job_id
                    WHERE j.source = 'job_board'
                      AND r.generated_at >= :s AND r.generated_at < :e
                    """
                ),
                {"s": start_utc, "e": end_utc},
            ).mappings().first()
            if r:
                out_match = {k: int(v or 0) for k, v in dict(r).items()}

        if "students" in tables:
            active = int(conn.execute(text("SELECT COUNT(*) FROM students WHERE status='active'")).scalar() or 0)
            reached = int(out_match.get("students_matched") or out_shared.get("students_reached") or 0)
            out_cov = {
                "active_students": active,
                "students_reached_today": reached,
                "students_without_jobs_today": max(0, active - reached),
            }

    engine.dispose()

    print(f"IST_DATE={ist_date}")
    print("1) Crawl target gap (10k jobs/day, 200 sources/day)")
    print(f"   jobs_crawled_today={jobs_today} shortfall={max(0, 10000 - jobs_today)}")
    print(f"   unique_sources_today={sources_today} shortfall={max(0, 200 - sources_today)}")
    print("2) Shortlisted jobs shared today")
    print(f"   {out_shortlisted}")
    print("   (notification-based sharing)")
    print(f"   {out_shared}")
    print("3) Students matched with jobs today")
    print(f"   {out_match}")
    print("4) Students failed to receive jobs today")
    print(f"   {out_cov}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
