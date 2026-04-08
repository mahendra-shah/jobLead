"""
Machine Learning module for job posting classification and extraction.

Heavy imports (ensemble + spaCy) are lazy-loaded so scripts that only need
``sklearn_classifier`` or ``base_classifier`` start quickly (e.g. job_board ML worker).
"""

from __future__ import annotations

__all__ = ["job_classifier"]


def __getattr__(name: str):
    if name == "job_classifier":
        from app.ml.ensemble_classifier import job_classifier as _jc

        return _jc
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
