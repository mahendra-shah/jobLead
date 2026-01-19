"""
Job Recommendation Service
Matches jobs to students based on preferences with visibility tracking
"""

from typing import List, Dict, Optional
from datetime import datetime, timedelta
from uuid import UUID
import logging

from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from app.models.job import Job
from app.models.student import Student
from app.models.company import Company

logger = logging.getLogger(__name__)


class RecommendationService:
    """
    Service for generating personalized job recommendations for students.
    
    Features:
    - Skills-based matching (TF-IDF similarity)
    - Location preference matching
    - Salary range filtering
    - Experience level matching
    - Visibility tracking (prevents showing same job multiple times)
    - Respects vacancy limits
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.vectorizer = TfidfVectorizer(
            max_features=100,
            stop_words='english',
            ngram_range=(1, 2)
        )
    
    async def get_recommendations_for_student(
        self,
        student_id: UUID,
        limit: int = 20,
        min_match_score: float = 0.3
    ) -> List[Dict]:
        """
        Get personalized job recommendations for a student.
        
        Args:
            student_id: UUID of the student
            limit: Maximum number of recommendations
            min_match_score: Minimum match score (0.0 to 1.0)
        
        Returns:
            List of job recommendations with match scores
        """
        try:
            # Get student with preferences
            student_result = await self.db.execute(
                select(Student).where(Student.id == student_id)
            )
            student = student_result.scalar_one_or_none()
            
            if not student:
                logger.error(f"Student {student_id} not found")
                return []
            
            # Get eligible jobs (not shown to this student yet)
            eligible_jobs = await self._get_eligible_jobs(student_id)
            
            if not eligible_jobs:
                logger.info(f"No eligible jobs for student {student_id}")
                return []
            
            # Calculate match scores
            recommendations = []
            for job in eligible_jobs:
                match_score = await self._calculate_match_score(student, job)
                
                if match_score >= min_match_score:
                    recommendations.append({
                        'job_id': str(job.id),
                        'job': job,
                        'match_score': match_score,
                        'match_reasons': await self._get_match_reasons(student, job, match_score)
                    })
            
            # Sort by match score
            recommendations.sort(key=lambda x: x['match_score'], reverse=True)
            
            # Limit results
            recommendations = recommendations[:limit]
            
            logger.info(
                f"Generated {len(recommendations)} recommendations for student {student_id}"
            )
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            raise
    
    async def _get_eligible_jobs(self, student_id: UUID) -> List[Job]:
        """
        Get jobs that are eligible to be shown to the student.
        
        Filters:
        - Active jobs only
        - Not expired (posted within last 30 days)
        - Not already shown to this student
        - Still has visibility slots available
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=30)
            
            # Query for eligible jobs
            result = await self.db.execute(
                select(Job).where(
                    and_(
                        Job.is_active == True,
                        Job.created_at >= cutoff_date,
                        # Not already shown to this student
                        ~Job.students_shown_to.contains([str(student_id)]),
                        # Check visibility mode constraints
                        or_(
                            # Mode 'all': check against max_students_to_show
                            and_(
                                Job.visibility_mode == 'all',
                                func.jsonb_array_length(Job.students_shown_to) < Job.max_students_to_show
                            ),
                            # Mode 'random_one': only if not shown to anyone yet
                            and_(
                                Job.visibility_mode == 'random_one',
                                func.jsonb_array_length(Job.students_shown_to) < 1
                            ),
                            # Mode 'vacancy_based': check against vacancy_count
                            and_(
                                Job.visibility_mode == 'vacancy_based',
                                func.jsonb_array_length(Job.students_shown_to) < Job.vacancy_count
                            )
                        )
                    )
                ).order_by(Job.created_at.desc()).limit(100)  # Limit for performance
            )
            
            jobs = result.scalars().all()
            logger.info(f"Found {len(jobs)} eligible jobs")
            return jobs
            
        except Exception as e:
            logger.error(f"Error fetching eligible jobs: {e}")
            raise
    
    async def _calculate_match_score(self, student: Student, job: Job) -> float:
        """
        Calculate match score between student and job (0.0 to 1.0).
        
        Scoring weights:
        - Skills match: 50%
        - Location match: 20%
        - Salary match: 15%
        - Experience match: 15%
        """
        try:
            scores = {
                'skills': 0.0,
                'location': 0.0,
                'salary': 0.0,
                'experience': 0.0
            }
            
            # Get student preferences from JSONB
            prefs = student.preferences or {}
            
            # Skills matching (TF-IDF similarity)
            student_skills = student.skills or []
            job_skills = job.skills_required or []
            
            if student_skills and job_skills:
                student_skills_text = ' '.join(student_skills)
                job_skills_text = ' '.join(job_skills)
                
                try:
                    tfidf_matrix = self.vectorizer.fit_transform([
                        student_skills_text,
                        job_skills_text
                    ])
                    similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
                    scores['skills'] = similarity
                except Exception as e:
                    logger.warning(f"TF-IDF error: {e}, using basic matching")
                    # Fallback to simple overlap
                    common_skills = set(student_skills) & set(job_skills)
                    scores['skills'] = len(common_skills) / max(len(job_skills), 1)
            
            # Location matching
            student_locations = prefs.get('preferred_locations', [])
            job_location = job.location or ""
            
            if student_locations and job_location:
                # Exact match
                if any(loc.lower() in job_location.lower() for loc in student_locations):
                    scores['location'] = 1.0
                # Remote jobs always match
                elif 'remote' in job_location.lower() or 'anywhere' in job_location.lower():
                    scores['location'] = 1.0
                else:
                    scores['location'] = 0.3  # Partial credit
            else:
                scores['location'] = 0.5  # Neutral if no preference
            
            # Salary matching
            student_min_salary = prefs.get('expected_salary_min', 0)
            student_max_salary = prefs.get('expected_salary_max', 0)
            job_salary_range = job.salary_range or {}
            
            if student_min_salary and job_salary_range:
                job_min = job_salary_range.get('min', 0)
                job_max = job_salary_range.get('max', 0)
                
                if job_min and job_min >= student_min_salary * 0.8:  # Within 80%
                    scores['salary'] = 1.0
                elif job_max and job_max >= student_min_salary:
                    scores['salary'] = 0.7
                else:
                    scores['salary'] = 0.3
            else:
                scores['salary'] = 0.5  # Neutral if no data
            
            # Experience matching
            student_experience = prefs.get('experience_years', 0) or 0
            job_experience = job.experience_required or ""
            
            if job_experience:
                # Parse experience (e.g., "2-3 years", "3+ years")
                import re
                exp_match = re.search(r'(\d+)', job_experience)
                if exp_match:
                    required_exp = int(exp_match.group(1))
                    if student_experience >= required_exp:
                        scores['experience'] = 1.0
                    elif student_experience >= required_exp - 1:
                        scores['experience'] = 0.8
                    else:
                        scores['experience'] = 0.5
                else:
                    scores['experience'] = 0.7  # No clear requirement
            else:
                scores['experience'] = 0.7  # No requirement specified
            
            # Weighted final score
            final_score = (
                scores['skills'] * 0.50 +
                scores['location'] * 0.20 +
                scores['salary'] * 0.15 +
                scores['experience'] * 0.15
            )
            
            return round(final_score, 3)
            
        except Exception as e:
            logger.error(f"Error calculating match score: {e}")
            return 0.0
    
    async def _get_match_reasons(
        self,
        student: Student,
        job: Job,
        match_score: float
    ) -> List[str]:
        """Get human-readable reasons for the match."""
        reasons = []
        
        # Skills
        student_skills = set(student.skills or [])
        job_skills = set(job.skills_required or [])
        common_skills = student_skills & job_skills
        
        if common_skills:
            skill_list = ', '.join(list(common_skills)[:3])
            reasons.append(f"Skills match: {skill_list}")
        
        # Location
        prefs = student.preferences or {}
        student_locations = prefs.get('preferred_locations', [])
        if any(loc.lower() in (job.location or "").lower() for loc in student_locations):
            reasons.append(f"Location: {job.location}")
        
        # Salary
        student_min = prefs.get('expected_salary_min', 0)
        if student_min and job.salary_range:
            job_min = job.salary_range.get('min', 0)
            if job_min and job_min >= student_min:
                reasons.append(f"Salary meets expectations")
        
        # High confidence job
        if job.ml_confidence and float(job.ml_confidence) >= 0.90:
            reasons.append("Verified job posting")
        
        # Recent posting
        if job.created_at and (datetime.utcnow() - job.created_at).days <= 3:
            reasons.append("Recently posted")
        
        return reasons
    
    async def mark_job_shown_to_student(
        self,
        job_id: UUID,
        student_id: UUID
    ) -> bool:
        """
        Mark a job as shown to a student.
        Updates the students_shown_to array.
        
        Returns:
            True if successfully marked, False otherwise
        """
        try:
            result = await self.db.execute(
                select(Job).where(Job.id == job_id)
            )
            job = result.scalar_one_or_none()
            
            if not job:
                logger.error(f"Job {job_id} not found")
                return False
            
            # Add student to shown list if not already there
            students_shown = job.students_shown_to or []
            student_id_str = str(student_id)
            
            if student_id_str not in students_shown:
                students_shown.append(student_id_str)
                job.students_shown_to = students_shown
                job.view_count = (job.view_count or 0) + 1
                
                await self.db.commit()
                logger.info(f"Marked job {job_id} as shown to student {student_id}")
                return True
            
            return True  # Already marked
            
        except Exception as e:
            logger.error(f"Error marking job as shown: {e}")
            await self.db.rollback()
            return False
    
    async def get_recommendation_stats(self, student_id: UUID) -> Dict:
        """Get recommendation statistics for a student."""
        try:
            # Count jobs shown
            result = await self.db.execute(
                select(func.count(Job.id)).where(
                    Job.students_shown_to.contains([str(student_id)])
                )
            )
            jobs_shown = result.scalar() or 0
            
            # Count eligible jobs
            eligible = await self._get_eligible_jobs(student_id)
            jobs_available = len(eligible)
            
            return {
                'jobs_shown': jobs_shown,
                'jobs_available': jobs_available,
                'student_id': str(student_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting recommendation stats: {e}")
            return {}
