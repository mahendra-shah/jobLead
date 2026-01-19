"""
Machine Learning module for job posting classification and extraction
Uses spaCy + Scikit-learn for intelligent job detection
"""

from app.ml.ensemble_classifier import job_classifier

__all__ = ['job_classifier']
