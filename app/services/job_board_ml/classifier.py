"""Job-board ingest: wrap shared sklearn job/not-job classifier."""

from __future__ import annotations

from app.ml.base_classifier import ClassificationResult
from app.ml.sklearn_classifier import SklearnClassifier


class JobBoardIngestClassifier:
    """
    Classifier dedicated to `job_ingest` crawled rows (separate artefact from Telegram).
    Loads `ML_JOB_BOARD_CLASSIFIER_BASENAME`, falling back to the legacy shared `.pkl` if missing.
    """

    def __init__(self) -> None:
        self._inner = SklearnClassifier(profile="job_board")

    def classify(self, text: str) -> ClassificationResult:
        return self._inner.classify(text)

    @property
    def is_loaded(self) -> bool:
        return bool(getattr(self._inner, "is_loaded", False))
