"""
Job Quality Scorer Service

Calculates quality and relevance scores for individual jobs based on
configurable criteria. Integrates with ML processor pipeline.

Author: Backend Team
Date: 2026-02-13
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class JobQualityScore:
    """Result of job quality scoring"""
    quality_score: float  # 0-100
    relevance_score: float  # 0-100
    meets_criteria: bool
    breakdown: Dict[str, float]
    reasons: List[str]


class JobQualityScorer:
    """
    Calculates comprehensive quality and relevance scores for jobs.
    
    Scoring Factors:
    - Experience match (30%) - Alignment with target experience range
    - Field completeness (25%) - How many required fields are filled
    - Skill relevance (20%) - Match with preferred skills
    - Salary range (10%) - Alignment with expected salary
    - ML confidence (10%) - Classifier confidence
    - Engagement potential (5%) - Predicted user engagement
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the scorer with relevance criteria.
        
        Args:
            config_path: Path to job_relevance_criteria.json (uses settings default if None)
        """
        if config_path is None:
            config_path = settings.JOB_RELEVANCE_CONFIG_PATH
        
        self.config = self._load_relevance_config(config_path)
        self.weights = self.config['scoring_weights']
        self.experience_criteria = self.config['experience_criteria']
        self.skill_criteria = self.config['preferred_skills']
        self.field_criteria = self.config['required_fields']
        self.excluded_keywords = self.config['excluded_keywords']
        self.location_filters = self.config.get('location_filters', {})
        
        logger.info(f"JobQualityScorer initialized with config from {config_path}")
    
    def _load_relevance_config(self, config_path: str) -> Dict:
        """Load relevance criteria from JSON file"""
        try:
            path = Path(config_path)
            if not path.is_absolute():
                # Relative to project root
                path = Path(__file__).parent.parent.parent / config_path
            
            with open(path, 'r') as f:
                config = json.load(f)
            
            logger.info(f"Loaded relevance config version {config.get('version', 'unknown')}")
            return config
        
        except FileNotFoundError:
            logger.error(f"Relevance config not found: {config_path}")
            # Return default minimal config
            return self._get_default_config()
        
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Fallback default configuration"""
        return {
            "version": "default",
            "experience_criteria": {"max_years": 2, "scoring": {}},
            "preferred_skills": {"tech": [], "scoring": {}},
            "required_fields": {"must_have": {"fields": ["title"], "weight": 1.0}},
            "scoring_weights": {
                "experience_match": 0.28,
                "field_completeness": 0.23,
                "skill_relevance": 0.18,
                "location_compatibility": 0.12,
                "salary_range": 0.09,
                "ml_confidence": 0.07,
                "engagement_potential": 0.03
            },
            "excluded_keywords": {},
            "location_filters": {
                "exclude_international_onsite": True,
                "allow_international_remote": True,
                "scoring": {
                    "international_remote": 90,
                    "international_onsite": 0,
                    "india_remote": 100,
                    "india_onsite": 70,
                    "unspecified": 60
                }
            }
        }
    
    def score_job(self, job_data: Dict[str, Any], ml_confidence: float = 0.7) -> JobQualityScore:
        """
        Calculate comprehensive quality score for a job.
        
        Args:
            job_data: Job fields dict with keys like title, company, skills_required, location_data, etc.
            ml_confidence: ML classifier confidence (0.0-1.0)
        
        Returns:
            JobQualityScore with overall score, relevance, and breakdown
        """
        scores = {}
        reasons = []
        
        # 1. Experience Match
        scores['experience_match'], exp_reasons = self._score_experience(
            job_data.get('experience_min'),
            job_data.get('experience_max'),
            job_data.get('is_fresher', False)
        )
        reasons.extend(exp_reasons)
        
        # 2. Field Completeness
        scores['field_completeness'], field_reasons = self._score_completeness(job_data)
        reasons.extend(field_reasons)
        
        # 3. Skill Relevance
        scores['skill_relevance'], skill_reasons = self._score_skills(
            job_data.get('skills_required', [])
        )
        reasons.extend(skill_reasons)
        
        # 4. Location Compatibility (NEW)
        scores['location_compatibility'], location_reasons = self._score_location(
            job_data.get('location_data') or {}
        )
        reasons.extend(location_reasons)
        
        # 5. Salary Range
        scores['salary_score'], salary_reasons = self._score_salary(
            job_data.get('salary_min'),
            job_data.get('salary_max')
        )
        reasons.extend(salary_reasons)
        
        # 6. ML Confidence
        scores['ml_confidence'] = ml_confidence * 100
        
        # 7. Engagement Potential
        scores['engagement_potential'] = self._predict_engagement(job_data)
        
        # Calculate weighted average
        quality_score = sum(
            scores.get(key, 50) * self.weights.get(key, 0)
            for key in self.weights.keys()
        )
        
        # Relevance check (must pass all critical criteria)
        meets_criteria, fail_reasons = self._check_relevance_criteria(job_data, scores)
        reasons.extend(fail_reasons)
        
        return JobQualityScore(
            quality_score=round(quality_score, 2),
            relevance_score=round(scores['experience_match'], 2),
            meets_criteria=meets_criteria,
            breakdown=scores,
            reasons=reasons
        )
    
    def _score_experience(self, min_exp: Optional[int], max_exp: Optional[int], 
                          is_fresher: bool) -> tuple[float, List[str]]:
        """Score based on experience match with criteria"""
        criteria = self.experience_criteria
        scoring = criteria.get('scoring', {})
        reasons = []
        
        # Fresher jobs get top score
        if is_fresher:
            reasons.append("✓ Fresher-friendly job")
            return scoring.get('fresher_jobs', 100.0), reasons
        
        # No experience specified or None
        if max_exp is None or max_exp == 0:
            if criteria.get('include_unspecified', True):
                reasons.append("○ Experience not specified (assumed fresher-friendly)")
                return scoring.get('unspecified', 70.0), reasons
            else:
                reasons.append("✗ Experience not specified")
                return 40.0, reasons
        
        # Ensure max_exp is an integer
        try:
            max_exp = int(max_exp)
        except (ValueError, TypeError):
            reasons.append("○ Experience not specified (assumed fresher-friendly)")
            return scoring.get('unspecified', 70.0), reasons
        
        # Check experience range
        if max_exp <= 2:
            reasons.append(f"✓ Experience {min_exp or 0}-{max_exp} years (excellent match)")
            return scoring.get('0_2_years', 100.0), reasons
        elif max_exp <= 3:
            reasons.append(f"○ Experience {min_exp or 0}-{max_exp} years (good match)")
            return scoring.get('2_3_years', 70.0), reasons
        elif max_exp <= 5:
            reasons.append(f"△ Experience {min_exp or 0}-{max_exp} years (acceptable)")
            return scoring.get('3_5_years', 40.0), reasons
        else:
            reasons.append(f"✗ Experience {min_exp or 0}-{max_exp} years (too senior)")
            return scoring.get('5_plus_years', 10.0), reasons
    
    def _score_completeness(self, job_data: Dict) -> tuple[float, List[str]]:
        """Score based on extracted field completeness"""
        criteria = self.field_criteria
        reasons = []
        score = 0.0
        
        # Must-have fields
        must_have = criteria.get('must_have', {})
        must_have_fields = must_have.get('fields', ['title', 'company'])
        must_have_weight = must_have.get('weight', 0.60)
        
        filled_must = sum(1 for field in must_have_fields if job_data.get(field))
        must_score = (filled_must / len(must_have_fields)) * 100 * must_have_weight
        score += must_score
        
        missing_must = [f for f in must_have_fields if not job_data.get(f)]
        if missing_must:
            reasons.append(f"✗ Missing critical fields: {', '.join(missing_must)}")
        else:
            reasons.append("✓ All critical fields present")
        
        # Should-have fields
        should_have = criteria.get('should_have', {})
        should_have_fields = should_have.get('fields', ['location', 'skills_required'])
        should_have_weight = should_have.get('weight', 0.30)
        
        filled_should = sum(1 for field in should_have_fields if job_data.get(field))
        should_score = (filled_should / len(should_have_fields)) * 100 * should_have_weight
        score += should_score
        
        # Nice-to-have fields
        nice_to_have = criteria.get('nice_to_have', {})
        nice_to_have_fields = nice_to_have.get('fields', ['salary_range', 'apply_link'])
        nice_to_have_weight = nice_to_have.get('weight', 0.10)
        
        filled_nice = sum(1 for field in nice_to_have_fields if job_data.get(field))
        if nice_to_have_fields:
            nice_score = (filled_nice / len(nice_to_have_fields)) * 100 * nice_to_have_weight
            score += nice_score
        
        total_fields = len(must_have_fields) + len(should_have_fields) + len(nice_to_have_fields)
        total_filled = filled_must + filled_should + filled_nice
        reasons.append(f"○ Completeness: {total_filled}/{total_fields} fields filled")
        
        return min(score, 100.0), reasons
    
    def _score_skills(self, skills: List[str]) -> tuple[float, List[str]]:
        """Score based on skill relevance"""
        if not skills:
            return 50.0, ["○ No skills specified"]
        
        # Normalize skills to lowercase
        skills_lower = [s.lower() for s in skills if s]
        reasons = []
        
        # Check against preferred skills categories
        tech_skills = self.skill_criteria.get('tech', [])
        design_skills = self.skill_criteria.get('design', [])
        marketing_skills = self.skill_criteria.get('marketing', [])
        data_skills = self.skill_criteria.get('data', [])
        
        # Count matches
        tech_matches = sum(1 for s in skills_lower if s in tech_skills)
        design_matches = sum(1 for s in skills_lower if s in design_skills)
        marketing_matches = sum(1 for s in skills_lower if s in marketing_skills)
        data_matches = sum(1 for s in skills_lower if s in data_skills)
        
        scoring = self.skill_criteria.get('scoring', {})
        
        if tech_matches > 0:
            reasons.append(f"✓ Tech skills match: {tech_matches} skills")
            return scoring.get('tech_match', 100.0), reasons
        elif design_matches > 0:
            reasons.append(f"✓ Design skills match: {design_matches} skills")
            return scoring.get('design_match', 100.0), reasons
        elif marketing_matches > 0:
            reasons.append(f"✓ Marketing skills match: {marketing_matches} skills")
            return scoring.get('marketing_match', 100.0), reasons
        elif data_matches > 0:
            reasons.append(f"✓ Data skills match: {data_matches} skills")
            return scoring.get('data_match', 100.0), reasons
        else:
            reasons.append(f"△ Generic skills: {', '.join(skills[:3])}")
            return scoring.get('no_match', 50.0), reasons
    
    def _score_location(self, location_data: Dict) -> tuple[float, List[str]]:
        """
        Score based on location compatibility.
        
        Args:
            location_data: Dict with keys:
                - geographic_scope: 'india' | 'international' | 'unspecified'
                - is_remote: bool
                - is_hybrid: bool
                - is_onsite_only: bool
        
        Returns:
            (score, reasons)
        """
        if not location_data:
            return 60.0, ["○ Location not analyzed"]
        
        reasons = []
        scoring = self.location_filters.get('scoring', {})
        geo_scope = location_data.get('geographic_scope', 'unspecified')
        is_remote = location_data.get('is_remote', False)
        is_hybrid = location_data.get('is_hybrid', False)
        is_onsite = location_data.get('is_onsite_only', False)
        
        # International jobs
        if geo_scope == 'international':
            if is_remote:
                reasons.append("✓ International remote job (acceptable)")
                return scoring.get('international_remote', 90.0), reasons
            elif is_hybrid:
                reasons.append("○ International hybrid job (limited accessibility)")
                return scoring.get('international_hybrid', 85.0), reasons
            else:
                reasons.append("✗ International onsite-only job (excluded)")
                return scoring.get('international_onsite', 0.0), reasons
        
        # India-based jobs
        elif geo_scope == 'india':
            if is_remote:
                reasons.append("✓ India remote job (excellent)")
                return scoring.get('india_remote', 100.0), reasons
            elif is_hybrid:
                reasons.append("✓ India hybrid job (very good)")
                return scoring.get('india_hybrid', 95.0), reasons
            elif is_onsite:
                reasons.append("△ India onsite with strict requirements")
                return scoring.get('onsite_with_restrictions', 30.0), reasons
            else:
                reasons.append("○ India office-based job (acceptable)")
                return scoring.get('india_onsite', 70.0), reasons
        
        # Unspecified location
        else:
            if is_remote or is_hybrid:
                reasons.append("○ Remote/hybrid (location unspecified)")
                return 80.0, reasons
            else:
                reasons.append("○ Location unspecified")
                return scoring.get('unspecified', 60.0), reasons
    
    def _score_salary(self, min_salary: Optional[float], 
                      max_salary: Optional[float]) -> tuple[float, List[str]]:
        """Score based on salary range alignment"""
        criteria = self.config['salary_criteria']
        min_expected = criteria.get('min_expected_inr', 15000)
        max_expected = criteria.get('max_expected_inr', 100000)
        modifiers = criteria.get('score_modifier', {})
        reasons = []
        
        if not min_salary and not max_salary:
            reasons.append("○ Salary not specified")
            return 50.0 + modifiers.get('unspecified', -5), reasons
        
        # Use whichever is available
        salary = max_salary or min_salary
        
        if salary < min_expected:
            reasons.append(f"△ Salary ₹{salary:,.0f} below expectations")
            return 50.0 + modifiers.get('below_min', -20), reasons
        elif salary <= max_expected:
            reasons.append(f"✓ Salary ₹{salary:,.0f} within range")
            return 50.0 + modifiers.get('within_range', 0), reasons
        else:
            reasons.append(f"✓ Salary ₹{salary:,.0f} above expectations")
            return 50.0 + modifiers.get('above_max', 10), reasons
    
    def _predict_engagement(self, job_data: Dict) -> float:
        """Predict engagement potential (0-100)"""
        engagement_config = self.config.get('engagement_prediction', {})
        factors = engagement_config.get('factors', {})
        
        score = 0.0
        
        # Has apply link
        if job_data.get('source_url') or job_data.get('apply_link'):
            score += factors.get('has_apply_link', 20)
        
        # Has salary
        if job_data.get('salary_min') or job_data.get('salary_max'):
            score += factors.get('has_salary', 15)
        
        # Has skills
        if job_data.get('skills_required'):
            score += factors.get('has_skills', 10)
        
        # Fresher-friendly
        if job_data.get('is_fresher') or ((job_data.get('experience_max') or 99) <= 2):
            score += factors.get('freshers_welcome', 20)
        
        # Remote work
        if job_data.get('work_type') in ['remote', 'hybrid']:
            score += factors.get('remote_work', 15)
        
        # Complete details
        if all([
            job_data.get('title'),
            job_data.get('company'),
            job_data.get('location'),
            job_data.get('skills_required')
        ]):
            score += factors.get('complete_details', 20)
        
        return min(score, 100.0)
    
    def _check_relevance_criteria(self, job_data: Dict, 
                                   scores: Dict[str, float]) -> tuple[bool, List[str]]:
        """Check if job meets minimum relevance criteria"""
        fail_reasons = []
        
        # Check for excluded keywords in title or description
        text_to_check = (
            f"{job_data.get('title', '')} {job_data.get('description', '')} "
            f"{job_data.get('raw_text', '')}"
        ).lower()
        
        for category, keywords in self.excluded_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_to_check:
                    fail_reasons.append(f"✗ Contains excluded keyword: {keyword} ({category})")
                    return False, fail_reasons
        
        # Check location compatibility (REJECT international onsite jobs)
        location_data = job_data.get('location_data') or {}
        if location_data:
            geo_scope = location_data.get('geographic_scope', 'unspecified')
            is_remote = location_data.get('is_remote', False)
            is_onsite = location_data.get('is_onsite_only', False)
            
            # Reject international non-remote jobs
            if geo_scope == 'international' and not is_remote:
                fail_reasons.append("✗ International non-remote position (excluded)")
                return False, fail_reasons
            
            # Reject strict onsite-only with relocation requirements
            if is_onsite and 'relocation' in text_to_check:
                fail_reasons.append("✗ Strict onsite with relocation requirement")
                return False, fail_reasons
        
        # Minimum location score threshold
        if scores.get('location_compatibility', 100) < 10:
            fail_reasons.append("✗ Location not suitable for target audience")
            return False, fail_reasons
        
        # Minimum experience threshold
        if scores.get('experience_match', 0) < 30:
            fail_reasons.append("✗ Experience requirements too high for freshers")
            return False, fail_reasons
        
        # Minimum field completeness
        if scores.get('field_completeness', 0) < 40:
            fail_reasons.append("✗ Too many missing fields")
            return False, fail_reasons
        
        # Overall quality threshold
        if scores.get('ml_confidence', 0) < 30:
            fail_reasons.append("✗ Low ML confidence in job classification")
            return False, fail_reasons
        
        return True, []


# Global instance (singleton pattern)
_scorer_instance: Optional[JobQualityScorer] = None


def get_quality_scorer() -> JobQualityScorer:
    """Get or create global job quality scorer instance"""
    global _scorer_instance
    
    if _scorer_instance is None:
        _scorer_instance = JobQualityScorer()
    
    return _scorer_instance
