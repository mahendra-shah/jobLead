"""
Job Recommendation Service
Core algorithm for personalized job recommendations
Scoring based on: Skills, Location, Experience, Job Type, Company, Freshness
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import selectinload

from app.models.student import Student
from app.models.job import Job
from app.models.student_interactions import SavedJob, JobView


class JobRecommendationService:
    """Service for calculating personalized job recommendations"""
    
    # Scoring weights
    SKILL_WEIGHT = 0.40  # 40%
    LOCATION_WEIGHT = 0.20  # 20%
    EXPERIENCE_WEIGHT = 0.15  # 15%
    JOB_TYPE_WEIGHT = 0.10  # 10%
    COMPANY_WEIGHT = 0.10  # 10%
    FRESHNESS_WEIGHT = 0.05  # 5%
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_recommendations(
        self,
        student: Student,
        limit: int = 20,
        offset: int = 0,
        min_score: float = 50.0,
        exclude_saved: bool = False,
        exclude_viewed: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get personalized job recommendations for student
        
        This function:
        1. Queries job table for active jobs from last 7 days only
        2. Matches student's technical_skills and soft_skills with job's skills_required
        3. Returns jobs sorted by match score
        
        Args:
            student: Student object (logged-in student)
            limit: Max number of recommendations
            offset: Pagination offset
            min_score: Minimum recommendation score (0-100)
            exclude_saved: Skip saved jobs
            exclude_viewed: Skip viewed jobs
        
        Returns:
            List of recommendations with scores and reasons
        """
        # Query job table for active jobs from last 7 days only
        # Eagerly load company relationship to avoid async issues
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        
        query = (
            select(Job)
            .options(selectinload(Job.company))  # Eagerly load company
            .where(
                and_(
                    Job.is_active.is_(True),
                    Job.created_at >= seven_days_ago
                )
            )
            .order_by(Job.created_at.desc())  # Order by newest first
        )
        
        result = await self.db.execute(query)
        jobs = result.scalars().all()
        
        # Get student's saved and viewed jobs if needed
        saved_job_ids = set()
        viewed_job_ids = set()
        
        if exclude_saved:
            saved_result = await self.db.execute(
                select(SavedJob.job_id).where(SavedJob.user_id == student.user_id)
            )
            saved_job_ids = {row[0] for row in saved_result.all()}
        
        if exclude_viewed:
            viewed_result = await self.db.execute(
                select(JobView.job_id).where(JobView.student_id == student.id)
            )
            viewed_job_ids = {row[0] for row in viewed_result.all()}
        
        # Score each job
        recommendations = []
        for job in jobs:
            # Skip if excluded
            if exclude_saved and job.id in saved_job_ids:
                continue
            if exclude_viewed and job.id in viewed_job_ids:
                continue
            
            # Calculate score
            score_data = self.calculate_score(student, job)
            
            # Filter by minimum score
            if score_data["total_score"] >= min_score:
                recommendations.append({
                    "job": job,
                    "score_data": score_data,
                    "is_saved": job.id in saved_job_ids if not exclude_saved else False
                })
        
        # Sort by score (descending)
        recommendations.sort(key=lambda x: x["score_data"]["total_score"], reverse=True)
        
        # Apply pagination
        paginated = recommendations[offset:offset + limit]
        
        # Format response
        formatted_recommendations = []
        for rec in paginated:
            job = rec["job"]
            score_data = rec["score_data"]
            
            formatted_recommendations.append({
                "job": {
                    "id": str(job.id),
                    "title": job.title,
                    "company_name": job.company.name if job.company else "Unknown",
                    "location": job.location,
                    "job_type": job.job_type,
                    "employment_type": job.employment_type,
                    "experience_required": job.experience_required or "Not specified",
                    "salary_range": job.salary_range or {},
                    "skills_required": job.skills_required or [],
                    "description": job.description,
                    "source_url": job.source_url,
                    "created_at": job.created_at.isoformat() if hasattr(job, 'created_at') and job.created_at else None,
                },
                "recommendation_score": round(score_data["total_score"], 2),
                "match_reasons": score_data["match_reasons"],
                "missing_skills": score_data["missing_skills"],
                "is_saved": rec["is_saved"],
                "score_breakdown": score_data["breakdown"],
                "view_count": job.view_count if hasattr(job, 'view_count') else 0,
                "similar_jobs_count": 0  # Placeholder for now
            })
        
        return formatted_recommendations
    
    def calculate_score(self, student: Student, job: Job) -> Dict[str, Any]:
        """
        Calculate recommendation score for a job
        
        Returns:
            {
                "total_score": float,
                "breakdown": {...},
                "match_reasons": [...],
                "missing_skills": [...]
            }
        """
        breakdown = {}
        match_reasons = []
        missing_skills = []
        
        # 1. Skill Match (40%)
        skill_score = self._calculate_skill_score(student, job, match_reasons, missing_skills)
        breakdown["skill_score"] = skill_score
        
        # 2. Location Match (20%)
        location_score = self._calculate_location_score(student, job, match_reasons)
        breakdown["location_score"] = location_score
        
        # 3. Experience Match (15%)
        experience_score = self._calculate_experience_score(student, job, match_reasons)
        breakdown["experience_score"] = experience_score
        
        # 4. Job Type Match (10%)
        job_type_score = self._calculate_job_type_score(student, job, match_reasons)
        breakdown["job_type_score"] = job_type_score
        
        # 5. Company Preference (10%)
        company_score = self._calculate_company_score(student, job, match_reasons)
        breakdown["company_score"] = company_score
        
        # 6. Freshness (5%)
        freshness_score = self._calculate_freshness_score(job, match_reasons)
        breakdown["freshness_score"] = freshness_score
        
        # Total score
        total_score = (
            skill_score +
            location_score +
            experience_score +
            job_type_score +
            company_score +
            freshness_score
        )
        
        return {
            "total_score": total_score,
            "breakdown": breakdown,
            "match_reasons": match_reasons,
            "missing_skills": missing_skills
        }
    
    def _calculate_skill_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str],
        missing_skills: List[str]
    ) -> float:
        """Calculate skill match score (0-40) based on technical_skills and soft_skills"""
        job_skills_list = job.skills_required or []
        if len(job_skills_list) == 0:
            return 20.0  # Neutral score if no skills specified
        
        # Combine student's technical_skills and soft_skills
        student_technical_skills = student.technical_skills or []
        student_soft_skills = student.soft_skills or []
        
        # Combine all student skills into one set
        all_student_skills = []
        all_student_skills.extend(student_technical_skills)
        all_student_skills.extend(student_soft_skills)
        
        student_skills = set(s.lower().strip() for s in all_student_skills if s)
        job_skills = set(s.lower().strip() for s in job_skills_list if s)
        
        if len(job_skills) == 0:
            return 20.0
        
        # Calculate match percentage
        matched_skills = student_skills & job_skills
        match_percentage = len(matched_skills) / len(job_skills) if len(job_skills) > 0 else 0
        
        # Score: 40% * match_percentage
        score = self.SKILL_WEIGHT * 100 * match_percentage
        
        # Add reason
        if match_percentage >= 0.8:
            match_reasons.append(f"🎯 Excellent skill match ({len(matched_skills)}/{len(job_skills)} skills)")
        elif match_percentage >= 0.5:
            match_reasons.append(f"✅ Good skill match ({len(matched_skills)}/{len(job_skills)} skills)")
        elif match_percentage > 0:
            match_reasons.append(f"📚 Partial skill match ({len(matched_skills)}/{len(job_skills)} skills)")
        else:
            match_reasons.append(f"⚠️ No skill match - consider adding required skills")
        
        # Missing skills
        missing = job_skills - student_skills
        missing_skills.extend(list(missing))
        
        return score
    
    def _calculate_location_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str]
    ) -> float:
        """
        Calculate location match score (0-20)
        
        Supports:
        - Specific cities: "Bangalore", "Mumbai", "Pune"
        - Remote work: "Remote", "Work from Home", "WFH"
        - Pan India: "Pan India", "Anywhere in India", "India"
        - International: "International", "Global", "Worldwide"
        - Specific countries: "USA", "UK", "Canada", "Singapore"
        """
        # Get preferred locations from preference JSONB field
        preferences = student.preference or {}
        preferred_locations = preferences.get('preferred_location', [])
        
        if not preferred_locations or len(preferred_locations) == 0:
            return 10.0  # Neutral score if no preferences
        
        if not job.location:
            return 10.0  # Neutral if job location not specified
        
        # Normalize locations for comparison
        job_location_lower = job.location.lower()
        preferred_locations_lower = [loc.lower() for loc in preferred_locations]
        
        # Define location categories
        pan_india_keywords = ["pan india", "anywhere in india", "india", "all india", "across india"]
        international_keywords = ["international", "global", "worldwide", "any location", "anywhere"]
        remote_keywords = ["remote", "work from home", "wfh", "work remotely", "anywhere"]
        
        # Check for Pan India preference
        has_pan_india_pref = any(
            any(keyword in pref_loc for keyword in pan_india_keywords)
            for pref_loc in preferred_locations_lower
        )
        
        # Check for International preference
        has_international_pref = any(
            any(keyword in pref_loc for keyword in international_keywords)
            for pref_loc in preferred_locations_lower
        )
        
        # Check for Remote preference
        has_remote_pref = any(
            any(keyword in pref_loc for keyword in remote_keywords)
            for pref_loc in preferred_locations_lower
        )
        
        # 1. Check if job is Remote
        is_remote_job = any(keyword in job_location_lower for keyword in remote_keywords)
        if is_remote_job:
            if has_remote_pref or has_pan_india_pref or has_international_pref:
                match_reasons.append("🏠 Remote work option available")
                return self.LOCATION_WEIGHT * 100
        
        # 2. Check for Pan India jobs
        is_pan_india_job = any(keyword in job_location_lower for keyword in pan_india_keywords)
        if is_pan_india_job:
            if has_pan_india_pref or has_remote_pref:
                match_reasons.append("🇮🇳 Pan India opportunity")
                return self.LOCATION_WEIGHT * 100
        
        # 3. Check for International jobs
        is_international_job = any(keyword in job_location_lower for keyword in international_keywords)
        if is_international_job:
            if has_international_pref:
                match_reasons.append("🌍 International opportunity")
                return self.LOCATION_WEIGHT * 100
        
        # 4. If student wants Pan India, match any Indian city
        if has_pan_india_pref:
            indian_cities = [
                "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad", "chennai",
                "pune", "kolkata", "ahmedabad", "jaipur", "surat", "lucknow",
                "kanpur", "nagpur", "indore", "thane", "bhopal", "visakhapatnam",
                "pimpri-chinchwad", "patna", "vadodara", "ghaziabad", "ludhiana",
                "agra", "nashik", "faridabad", "meerut", "rajkot", "noida", "gurgaon"
            ]
            if any(city in job_location_lower for city in indian_cities):
                match_reasons.append(f"📍 Location match: {job.location} (Pan India preference)")
                return self.LOCATION_WEIGHT * 100
        
        # 5. If student wants International, match any country
        if has_international_pref:
            countries = [
                "usa", "united states", "uk", "united kingdom", "canada", "australia",
                "singapore", "germany", "france", "netherlands", "ireland", "dubai",
                "uae", "switzerland", "sweden", "norway", "denmark", "japan", "china"
            ]
            if any(country in job_location_lower for country in countries):
                match_reasons.append(f"🌍 Location match: {job.location} (International preference)")
                return self.LOCATION_WEIGHT * 100
        
        # 6. Check for exact or partial city/country match
        for pref_loc in preferred_locations_lower:
            if pref_loc in job_location_lower or job_location_lower in pref_loc:
                match_reasons.append(f"📍 Location match: {job.location}")
                return self.LOCATION_WEIGHT * 100
        
        # 7. Check for country match when student specifies a country
        # Example: Student wants "USA" and job is in "New York, USA"
        for pref_loc in preferred_locations_lower:
            if len(pref_loc) > 2:  # Avoid matching short strings
                if pref_loc in job_location_lower:
                    match_reasons.append(f"📍 Location match: {job.location}")
                    return self.LOCATION_WEIGHT * 100
        
        return 0.0
    
    def _calculate_experience_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str]
    ) -> float:
        """Calculate experience match score (0-15)"""
        if not job.experience_required:
            return 7.5  # Neutral if experience not specified
        
        # Parse experience_required string (e.g., "0-2 years", "2-5 years", "Fresher")
        exp_str = job.experience_required.lower()
        
        # Check if it's a fresher role
        if any(keyword in exp_str for keyword in ["fresher", "entry", "0-1", "0-2"]):
            match_reasons.append("🎓 Freshers welcomed")
            return self.EXPERIENCE_WEIGHT * 100
        
        # Try to extract minimum years from string
        import re
        numbers = re.findall(r'\d+', exp_str)
        if numbers:
            min_exp = int(numbers[0])
            
            # If experience required but student is typically fresher
            if min_exp > 0:
                # Give partial score if requirement is low (1-2 years)
                if min_exp <= 2:
                    match_reasons.append(f"💼 {min_exp}+ years experience preferred")
                    return self.EXPERIENCE_WEIGHT * 50  # 50% score
                else:
                    return 0.0
        
        return 7.5  # Neutral
    
    def _calculate_job_type_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str]
    ) -> float:
        """Calculate job type match score (0-10)"""
        # Get preferred job types from preference JSONB field
        preferences = student.preference or {}
        preferred_job_types = preferences.get('job_type', [])
        
        if not preferred_job_types or len(preferred_job_types) == 0:
            return 5.0  # Neutral if no preferences
        
        if not job.job_type:
            return 5.0  # Neutral if job type not specified
        
        # Normalize for comparison
        job_type_lower = job.job_type.lower()
        preferred_types_lower = [jt.lower() for jt in preferred_job_types]
        
        # Check match
        for pref_type in preferred_types_lower:
            if pref_type in job_type_lower or job_type_lower in pref_type:
                match_reasons.append(f"💼 {job.job_type.title()} position")
                return self.JOB_TYPE_WEIGHT * 100
        
        return 0.0
    
    def _calculate_company_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str]
    ) -> float:
        """Calculate company preference score (0-10)"""
        preferences = student.preference or {}
        excluded_companies = preferences.get('excluded_companies', [])
        
        if not excluded_companies or len(excluded_companies) == 0:
            return 5.0  # Neutral if no exclusions
        
        if not job.company_id:
            return 5.0  # Neutral if company not specified
        
        # Note: job.company is a relationship, need to check company name
        # For now, give neutral score if we can't determine company name
        # In a full implementation, we'd join Company model
        return 5.0
    
    def _calculate_freshness_score(
        self,
        job: Job,
        match_reasons: List[str]
    ) -> float:
        """Calculate freshness score based on posting date (0-5)"""
        if not job.created_at:
            return 2.5  # Neutral if date not specified
        
        days_old = (datetime.utcnow() - job.created_at).days
        
        if days_old <= 1:
            match_reasons.append("🔥 Posted today!")
            return self.FRESHNESS_WEIGHT * 100
        elif days_old <= 3:
            match_reasons.append("✨ Posted recently")
            return self.FRESHNESS_WEIGHT * 80
        elif days_old <= 7:
            match_reasons.append("📅 Posted this week")
            return self.FRESHNESS_WEIGHT * 60
        elif days_old <= 14:
            return self.FRESHNESS_WEIGHT * 40
        else:
            return self.FRESHNESS_WEIGHT * 20
    
    async def get_similar_jobs(
        self,
        job_id,
        limit: int = 5
    ) -> List[Job]:
        """
        Get similar jobs based on skills and company.
        Accepts a UUID job_id (or any DB-compatible primary key type).
        """
        # Get the reference job with company loaded
        result = await self.db.execute(
            select(Job)
            .options(selectinload(Job.company))
            .where(Job.id == job_id)
        )
        reference_job = result.scalar_one_or_none()
        
        if not reference_job:
            return []
        
        # Get jobs with similar skills or same company (with company loaded)
        query = (
            select(Job)
            .options(selectinload(Job.company))
            .where(
                and_(
                    Job.id != job_id,
                    Job.is_active.is_(True),
                    or_(
                        Job.company_id == reference_job.company_id,
                        # Jobs with overlapping skills (handled in Python)
                        Job.skills_required.isnot(None)
                    )
                )
            )
            .limit(limit * 2)  # Get extra to filter by skills
        )
        
        result = await self.db.execute(query)
        similar_jobs = result.scalars().all()
        
        return similar_jobs
