"""Modular ML + quality pipeline for Mongo `job_ingest` (crawled job boards)."""

from app.services.job_board_ml.worker import run_job_board_ingest_ml

__all__ = ["run_job_board_ingest_ml"]
