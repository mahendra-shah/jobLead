"""
Job Recommendation Service

Personalized job feed with per-student Redis caching.

Scoring weights (total 100 pts):
  Skill match      45 %   (primary signal)
  Location match   20 %
  Experience       15 %
  Job type         10 %
  Freshness        10 %

Performance design:
  - Cache key: per-student / per-filter-set (NOT per-page).
    The FULL scored+sorted list is cached once; pagination is a free
    Python slice on subsequent calls.
  - Cache miss fires exactly ONE SQL query (no company relationship
    load — company_name is a denormalized column on jobs).
  - Automatic date-window widening (7 → 14 → 30 days) prevents empty
    feeds during quiet posting periods.

Expected latency:
  Cache hit  (any page):       < 20 ms
  Cache miss (first load):   200-400 ms   (was 600-1200 ms)
"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.models.job import Job
from app.models.student import Student
from app.models.student_interactions import JobView, SavedJob

logger = logging.getLogger(__name__)


class JobRecommendationService:
    """Personalized job recommendation service with Redis caching."""

    # --- Scoring weights (must sum to 1.0) ---
    SKILL_WEIGHT = 0.45        # 45% — primary signal (was 40%)
    LOCATION_WEIGHT = 0.20     # 20%
    EXPERIENCE_WEIGHT = 0.15   # 15%
    JOB_TYPE_WEIGHT = 0.10     # 10%
    FRESHNESS_WEIGHT = 0.10    # 10% (was 5%; absorbed removed company weight)

    # --- Query tuning ---
    MAX_JOBS_TO_QUERY = 500    # Hard cap on rows loaded from DB
    MIN_QUALITY_SCORE = 50     # Pre-filter: skip low-quality jobs

    # --- Cache ---
    CACHE_RECOMMENDATIONS_TTL = 1800  # 30 minutes

    # --- Fallback windows: widen date range when too few jobs found ---
    DATE_WINDOWS = [7, 14, 30]    # days
    MIN_JOBS_THRESHOLD = 10       # retry with wider window if below this

    def __init__(self, db: AsyncSession, cache_manager=None):
        self.db = db
        self.cache_manager = cache_manager
    
    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def get_recommendations(
        self,
        student: Student,
        limit: int = 20,
        offset: int = 0,
        min_score: float = 50.0,
        exclude_saved: bool = False,
        exclude_viewed: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Return paginated job recommendations for *student*.

        The FULL scored+sorted list is cached per student/filter-set so
        subsequent pages (any offset) are served instantly from a Python
        slice — no re-scoring, no extra DB queries.

        Args:
            student: Authenticated student object.
            limit: Page size (1-100).
            offset: Pagination offset.
            min_score: Minimum recommendation score (0-100).
            exclude_saved: Exclude already-saved jobs.
            exclude_viewed: Exclude already-viewed jobs.

        Returns:
            Slice of the full scored list: full_list[offset : offset+limit].
        """
        cache_key = self._build_cache_key(
            student.id, min_score, exclude_saved, exclude_viewed
        )

        # --- Cache read: hit returns an O(1) slice ---
        if self.cache_manager and self.cache_manager.enabled:
            cached = self.cache_manager.get(cache_key)
            if cached is not None:
                logger.debug(
                    "recommendation_cache_hit student=%s key=%s",
                    student.id,
                    cache_key,
                )
                return cached[offset: offset + limit]

        # --- Cache miss: full computation ---
        full_list = await self._compute_recommendations(
            student, min_score, exclude_saved, exclude_viewed
        )

        # Cache the FULL list once; pagination is a free slice thereafter.
        if self.cache_manager and self.cache_manager.enabled:
            self.cache_manager.set(
                cache_key, full_list, ttl=self.CACHE_RECOMMENDATIONS_TTL
            )

        return full_list[offset: offset + limit]

    async def get_recommendation_counts(
        self, student: Student
    ) -> Dict[str, Any]:
        """
        Lightweight recommendation stats using cheap DB count queries.

        Uses the already-warmed main-feed cache (min_score=50) when
        available for score distribution.  Never triggers a full re-score.

        Returns:
            Dict with total_jobs_available, match_distribution,
            criteria_matches, and top_recommendations (≤ 5).
        """
        cutoff = datetime.utcnow() - timedelta(days=7)

        # Total eligible active jobs (uses partial covering index)
        total_result = await self.db.execute(
            select(func.count())
            .select_from(Job)
            .where(
                Job.is_active.is_(True),
                Job.created_at >= cutoff,
                Job.quality_score >= self.MIN_QUALITY_SCORE,
            )
        )
        total_count = total_result.scalar() or 0

        # Fresher-friendly jobs (partial index ix_jobs_is_active_is_fresher)
        fresher_result = await self.db.execute(
            select(func.count())
            .select_from(Job)
            .where(
                Job.is_active.is_(True),
                Job.created_at >= cutoff,
                Job.is_fresher.is_(True),
            )
        )
        fresher_count = fresher_result.scalar() or 0

        # Jobs with any skills listed (approximation for "skill matches")
        has_skills = bool(
            (student.technical_skills or []) + (student.soft_skills or [])
        )
        skill_count = 0
        if has_skills:
            skill_result = await self.db.execute(
                select(func.count())
                .select_from(Job)
                .where(
                    Job.is_active.is_(True),
                    Job.created_at >= cutoff,
                    Job.skills_required.isnot(None),
                    func.jsonb_array_length(Job.skills_required) > 0,
                )
            )
            skill_count = skill_result.scalar() or 0

        # Score distribution from the warmed main-feed cache (if present)
        cache_key = self._build_cache_key(student.id, 50.0, False, False)
        score_dist: Dict[str, int] = {
            "high_match": 0,
            "medium_match": 0,
            "low_match": 0,
        }
        top_recs: List[Dict] = []
        if self.cache_manager and self.cache_manager.enabled:
            cached = self.cache_manager.get(cache_key)
            if cached:
                score_dist["high_match"] = sum(
                    1 for r in cached if r["recommendation_score"] >= 80
                )
                score_dist["medium_match"] = sum(
                    1 for r in cached if 60 <= r["recommendation_score"] < 80
                )
                score_dist["low_match"] = sum(
                    1 for r in cached if 50 <= r["recommendation_score"] < 60
                )
                top_recs = cached[:5]

        return {
            "total_jobs_available": total_count,
            "match_distribution": score_dist,
            "criteria_matches": {
                "skill_matches": skill_count,
                "location_matches": 0,   # Requires full scoring
                "fresher_friendly": fresher_count,
            },
            "top_recommendations": top_recs,
        }

    def invalidate_student_cache(self, student_id: int) -> int:
        """
        Invalidate all cached recommendations for *student_id*.

        Call whenever student profile (skills, prefs) or interaction
        state (save, view, apply) changes so next request recomputes.

        Returns:
            Number of cache keys deleted.
        """
        if not self.cache_manager or not self.cache_manager.enabled:
            return 0
        pattern = f"rec:student_{student_id}:*"
        deleted = self.cache_manager.delete_pattern(pattern)
        logger.info(
            "recommendation_cache_invalidated student=%s keys=%s",
            student_id,
            deleted,
        )
        return deleted

    # ------------------------------------------------------------------ #
    # Cache key                                                            #
    # ------------------------------------------------------------------ #

    def _build_cache_key(
        self,
        student_id: int,
        min_score: float,
        exclude_saved: bool,
        exclude_viewed: bool,
    ) -> str:
        """
        Build a cache key that is stable across paginated requests.

        *limit* and *offset* are intentionally excluded so the same
        cached full list is reused for every page.
        """
        return (
            f"rec:student_{student_id}:"
            f"min_{min_score}:"
            f"saved_{exclude_saved}:"
            f"viewed_{exclude_viewed}"
        )

    # ------------------------------------------------------------------ #
    # Core computation                                                     #
    # ------------------------------------------------------------------ #

    async def _compute_recommendations(
        self,
        student: Student,
        min_score: float,
        exclude_saved: bool,
        exclude_viewed: bool,
    ) -> List[Dict[str, Any]]:
        """
        Compute the FULL scored + sorted recommendation list (no pagination).

        Caller caches this result; individual pages are sliced from it
        without re-scoring.

        Returns:
            Formatted list of all recommendations with score >= min_score,
            sorted descending by recommendation_score.
        """
        jobs = await self._fetch_eligible_jobs(
            student=student,
            exclude_saved=exclude_saved,
            exclude_viewed=exclude_viewed,
        )

        if not jobs:
            logger.warning(
                "no_eligible_jobs student=%s quality_threshold=%s",
                student.id,
                self.MIN_QUALITY_SCORE,
            )
            return []

        logger.info("scoring_jobs student=%s count=%s", student.id, len(jobs))

        # Saved-job IDs for the is_saved flag in the response.
        saved_job_ids: set = set()
        if not exclude_saved:
            saved_result = await self.db.execute(
                select(SavedJob.job_id).where(
                    SavedJob.user_id == student.user_id
                )
            )
            saved_job_ids = {row[0] for row in saved_result.all()}

        # Score each job (typically 50-150 iterations)
        scored: List[Dict] = []
        for job in jobs:
            try:
                score_data = self.calculate_score(student, job)
                if score_data["total_score"] >= min_score:
                    scored.append(
                        {
                            "job": job,
                            "score_data": score_data,
                            "is_saved": job.id in saved_job_ids,
                        }
                    )
            except Exception as exc:
                logger.error(
                    "job_scoring_error job=%s error=%s", str(job.id), str(exc)
                )
                continue

        # Sort descending by total score
        scored.sort(
            key=lambda x: x["score_data"]["total_score"], reverse=True
        )

        return self._format_recommendations(scored)

    async def _fetch_eligible_jobs(
        self,
        student: Student,
        exclude_saved: bool,
        exclude_viewed: bool,
    ) -> List[Job]:
        """
        Query eligible jobs from the DB with automatic window widening.

        Tries DATE_WINDOWS (7 → 14 → 30 days) until at least
        MIN_JOBS_THRESHOLD rows are found, so students in quiet periods
        always see recommendations.

        No selectinload — company_name is a denormalized column;
        zero extra queries needed.
        """
        jobs: List[Job] = []
        for days in self.DATE_WINDOWS:
            cutoff = datetime.utcnow() - timedelta(days=days)

            # Uses partial covering index:
            # idx_jobs_recommendation_query_optimized
            # (quality_score DESC, created_at DESC)
            # WHERE is_active = TRUE AND quality_score >= 50
            query = (
                select(Job)
                .where(
                    Job.is_active.is_(True),
                    Job.created_at >= cutoff,
                    Job.quality_score >= self.MIN_QUALITY_SCORE,
                )
                .order_by(
                    Job.quality_score.desc(), Job.created_at.desc()
                )
                .limit(self.MAX_JOBS_TO_QUERY)
            )

            # SQL-level exclusions via NOT EXISTS
            if exclude_saved:
                query = query.where(
                    ~exists(
                        select(1).where(
                            and_(
                                SavedJob.job_id == Job.id,
                                SavedJob.user_id == student.user_id,
                            )
                        )
                    )
                )

            if exclude_viewed:
                query = query.where(
                    ~exists(
                        select(1).where(
                            and_(
                                JobView.job_id == Job.id,
                                JobView.student_id == student.id,
                            )
                        )
                    )
                )

            result = await self.db.execute(query)
            jobs = result.scalars().all()

            if len(jobs) >= self.MIN_JOBS_THRESHOLD:
                if days > 7:
                    logger.info(
                        "recommendation_fallback_window student=%s days=%s",
                        student.id,
                        days,
                    )
                return jobs

            logger.debug(
                "widening_recommendation_window student=%s days=%s found=%s",
                student.id,
                days,
                len(jobs),
            )

        # Return whatever was found in the widest window (may be empty)
        return jobs

    # ------------------------------------------------------------------ #
    # Scoring                                                              #
    # ------------------------------------------------------------------ #

    def calculate_score(self, student: Student, job: Job) -> Dict[str, Any]:
        """
        Calculate the total recommendation score (0-100) for one job.

        Weights:
            skill_score        45 %
            location_score     20 %
            experience_score   15 %
            job_type_score     10 %
            freshness_score    10 %

        Returns:
            Dict with keys: total_score, breakdown, match_reasons,
            missing_skills.
        """
        match_reasons: List[str] = []
        missing_skills: List[str] = []
        breakdown: Dict[str, float] = {}

        breakdown["skill_score"] = self._calculate_skill_score(
            student, job, match_reasons, missing_skills
        )
        breakdown["location_score"] = self._calculate_location_score(
            student, job, match_reasons
        )
        breakdown["experience_score"] = self._calculate_experience_score(
            student, job, match_reasons
        )
        breakdown["job_type_score"] = self._calculate_job_type_score(
            student, job, match_reasons
        )
        breakdown["freshness_score"] = self._calculate_freshness_score(
            job, match_reasons
        )

        return {
            "total_score": sum(breakdown.values()),
            "breakdown": breakdown,
            "match_reasons": match_reasons,
            "missing_skills": missing_skills,
        }

    def _calculate_skill_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str],
        missing_skills: List[str],
    ) -> float:
        """
        Skill match score (0-45).

        Combines student.technical_skills and student.soft_skills into
        one set and computes the intersection ratio with
        job.skills_required.  Returns 22.5 (neutral) when either side
        has no skills listed.
        """
        job_skills_list = job.skills_required or []
        if not job_skills_list:
            return 22.5  # Neutral — no requirement specified

        all_student = (student.technical_skills or []) + (
            student.soft_skills or []
        )
        student_skills = {s.lower().strip() for s in all_student if s}
        job_skills = {s.lower().strip() for s in job_skills_list if s}

        if not job_skills:
            return 22.5

        matched = student_skills & job_skills
        ratio = len(matched) / len(job_skills)
        score = self.SKILL_WEIGHT * 100 * ratio

        if ratio >= 0.8:
            match_reasons.append(
                f"🎯 Excellent skill match ({len(matched)}/{len(job_skills)} skills)"
            )
        elif ratio >= 0.5:
            match_reasons.append(
                f"✅ Good skill match ({len(matched)}/{len(job_skills)} skills)"
            )
        elif ratio > 0:
            match_reasons.append(
                f"📚 Partial skill match ({len(matched)}/{len(job_skills)} skills)"
            )
        else:
            match_reasons.append(
                "⚠️ No skill match — consider adding required skills"
            )

        missing_skills.extend(job_skills - student_skills)
        return score

    def _calculate_location_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str],
    ) -> float:
        """
        Location match score (0-20).

        Handles specific cities, Pan India, Remote, and International
        preferences.  Returns 10 (neutral) when either side has no
        location data.
        """
        preferences = student.preference or {}
        preferred_locations = preferences.get("preferred_location", [])
        if not isinstance(preferred_locations, list):
            preferred_locations = []

        if not preferred_locations or not job.location:
            return 10.0  # Neutral

        job_loc = job.location.lower()
        pref_locs = [loc.lower() for loc in preferred_locations]

        pan_india_kw = [
            "pan india", "anywhere in india", "india",
            "all india", "across india",
        ]
        international_kw = [
            "international", "global", "worldwide",
            "any location", "anywhere",
        ]
        remote_kw = [
            "remote", "work from home", "wfh",
            "work remotely", "anywhere",
        ]

        has_pan = any(
            any(kw in p for kw in pan_india_kw) for p in pref_locs
        )
        has_intl = any(
            any(kw in p for kw in international_kw) for p in pref_locs
        )
        has_remote = any(
            any(kw in p for kw in remote_kw) for p in pref_locs
        )

        is_remote = any(kw in job_loc for kw in remote_kw)
        is_pan = any(kw in job_loc for kw in pan_india_kw)
        is_intl = any(kw in job_loc for kw in international_kw)

        if is_remote and (has_remote or has_pan or has_intl):
            match_reasons.append("🏠 Remote work option available")
            return self.LOCATION_WEIGHT * 100

        if is_pan and (has_pan or has_remote):
            match_reasons.append("🇮🇳 Pan India opportunity")
            return self.LOCATION_WEIGHT * 100

        if is_intl and has_intl:
            match_reasons.append("🌍 International opportunity")
            return self.LOCATION_WEIGHT * 100

        indian_cities = {
            "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
            "chennai", "pune", "kolkata", "ahmedabad", "jaipur", "surat",
            "lucknow", "kanpur", "nagpur", "indore", "thane", "bhopal",
            "visakhapatnam", "patna", "vadodara", "ghaziabad", "ludhiana",
            "agra", "nashik", "faridabad", "meerut", "rajkot", "noida",
            "gurgaon",
        }
        if has_pan and any(city in job_loc for city in indian_cities):
            match_reasons.append(
                f"📍 Location match: {job.location} (Pan India preference)"
            )
            return self.LOCATION_WEIGHT * 100

        intl_places = {
            "usa", "united states", "uk", "united kingdom", "canada",
            "australia", "singapore", "germany", "france", "netherlands",
            "ireland", "dubai", "uae", "switzerland", "sweden", "norway",
            "denmark", "japan", "china",
        }
        if has_intl and any(c in job_loc for c in intl_places):
            match_reasons.append(
                f"🌍 Location match: {job.location} (International preference)"
            )
            return self.LOCATION_WEIGHT * 100

        for pref in pref_locs:
            if pref in job_loc or job_loc in pref:
                match_reasons.append(f"📍 Location match: {job.location}")
                return self.LOCATION_WEIGHT * 100

        return 0.0

    def _calculate_experience_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str],
    ) -> float:
        """
        Experience match score (0-15).

        Returns 7.5 (neutral) when experience is unspecified.
        Fresher roles score full points; roles requiring > 2 years score 0.
        """
        if not job.experience_required:
            return 7.5  # Neutral

        exp_str = job.experience_required.lower()
        if any(kw in exp_str for kw in ("fresher", "entry", "0-1", "0-2")):
            match_reasons.append("🎓 Freshers welcomed")
            return self.EXPERIENCE_WEIGHT * 100

        numbers = re.findall(r"\d+", exp_str)
        if numbers:
            min_exp = int(numbers[0])
            if min_exp <= 2:
                match_reasons.append(
                    f"💼 {min_exp}+ years experience preferred"
                )
                return self.EXPERIENCE_WEIGHT * 50
            return 0.0

        return 7.5  # Neutral fallback

    def _calculate_job_type_score(
        self,
        student: Student,
        job: Job,
        match_reasons: List[str],
    ) -> float:
        """
        Job type match score (0-10).

        Returns 5 (neutral) when either side has no job-type data.
        """
        preferences = student.preference or {}
        preferred_types = preferences.get("job_type", [])
        if not isinstance(preferred_types, list):
            preferred_types = []

        if not preferred_types or not job.job_type:
            return 5.0  # Neutral

        job_type = job.job_type.lower()
        for pref in (pt.lower() for pt in preferred_types):
            if pref in job_type or job_type in pref:
                match_reasons.append(f"💼 {job.job_type.title()} position")
                return self.JOB_TYPE_WEIGHT * 100

        return 0.0

    def _calculate_freshness_score(
        self,
        job: Job,
        match_reasons: List[str],
    ) -> float:
        """
        Freshness score (0-10).

        jobs.created_at is TIMESTAMP WITHOUT TIME ZONE (naive UTC);
        datetime.utcnow() is used to avoid aware/naive subtraction errors.
        """
        if not job.created_at:
            return 5.0  # Neutral

        days_old = (datetime.utcnow() - job.created_at).days

        if days_old <= 1:
            match_reasons.append("🔥 Posted today!")
            return self.FRESHNESS_WEIGHT * 100
        if days_old <= 3:
            match_reasons.append("✨ Posted recently")
            return self.FRESHNESS_WEIGHT * 80
        if days_old <= 7:
            match_reasons.append("📅 Posted this week")
            return self.FRESHNESS_WEIGHT * 60
        if days_old <= 14:
            return self.FRESHNESS_WEIGHT * 40
        return self.FRESHNESS_WEIGHT * 20

    # ------------------------------------------------------------------ #
    # Formatting                                                           #
    # ------------------------------------------------------------------ #

    def _format_recommendations(
        self, recommendations: List[Dict]
    ) -> List[Dict[str, Any]]:
        """
        Serialize a list of scored job dicts into the wire-format response.

        Uses job.company_name (denormalized column) so no relationship
        load is required.
        """
        formatted = []
        for rec in recommendations:
            job: Job = rec["job"]
            score_data: Dict = rec["score_data"]
            formatted.append(
                {
                    "job": {
                        "id": str(job.id),
                        "title": job.title,
                        "company_name": job.company_name or "Unknown",
                        "location": job.location,
                        "job_type": job.job_type,
                        "employment_type": job.employment_type,
                        "experience_required": (
                            job.experience_required or "Not specified"
                        ),
                        "salary_range": job.salary_range or {},
                        "skills_required": job.skills_required or [],
                        "description": job.description,
                        "source_url": job.source_url,
                        "created_at": (
                            job.created_at.isoformat()
                            if job.created_at
                            else None
                        ),
                    },
                    "recommendation_score": round(
                        score_data["total_score"], 2
                    ),
                    "match_reasons": score_data["match_reasons"],
                    "missing_skills": score_data["missing_skills"],
                    "is_saved": rec["is_saved"],
                    "score_breakdown": score_data["breakdown"],
                    "view_count": getattr(job, "view_count", 0),
                    "similar_jobs_count": 0,
                }
            )
        return formatted

    # ------------------------------------------------------------------ #
    # Similar jobs                                                         #
    # ------------------------------------------------------------------ #

    async def get_similar_jobs(
        self,
        job_id,
        limit: int = 5,
    ) -> List[Job]:
        """
        Get similar jobs based on company.

        Args:
            job_id: UUID of the reference job.
            limit: Maximum similar jobs to return.

        Returns:
            List of Job ORM objects (not formatted).
        """
        ref_result = await self.db.execute(
            select(Job).where(Job.id == job_id)
        )
        reference_job = ref_result.scalar_one_or_none()
        if not reference_job:
            return []

        query = (
            select(Job)
            .where(
                and_(
                    Job.id != job_id,
                    Job.is_active.is_(True),
                    Job.company_id == reference_job.company_id,
                )
            )
            .limit(limit)
        )
        result = await self.db.execute(query)
        return result.scalars().all()

