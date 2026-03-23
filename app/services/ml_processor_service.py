"""
ML Processor Service
Processes unprocessed messages from MongoDB, classifies them, and stores jobs to PostgreSQL
"""

import logging
import json
import re as _re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
from pymongo import MongoClient
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session

from app.config import Settings
from app.db.session import get_sync_db
from app.db.base import Base  # Import Base to initialize all models
# Import all models at once to ensure proper relationship initialization
from app.models import Job, Company, Channel, Application, Student, User, TelegramGroup
from app.models.job_scraping_preferences import JobScrapingPreferences
from app.ml.sklearn_classifier import SklearnClassifier
from app.ml.spacy_extractor import SpacyExtractor
from app.ml.enhanced_extractor import get_enhanced_extractor
from app.services.job_quality_scorer import get_quality_scorer
from app.services.deduplication_service import deduplication_service

# Load settings
settings = Settings()

logger = logging.getLogger(__name__)

# ── Pre-filter: non-job content patterns ──────────────────────────────────────────
# These patterns catch spam / service-ads that fool the ML classifier because
# they use job-like language. Compiled once at import time.
#
# To ADD a new pattern: append a tuple ("label", compiled_regex) to the list.
# To REMOVE a pattern: comment it out or delete the tuple.
# ─────────────────────────────────────────────────────────────────
_NON_JOB_PATTERNS = [
    # Crypto / USDT payment scams
    (
        "crypto_scam",
        _re.compile(
            r'\b(?:USDT|bitcoin|BTC|ETH|ethereum|crypto\s+earn|earn\s+USDT'  # noqa: ISC001
            r'|\d+\s*USDT\s*=|buy\s+USDT|sell\s+USDT'  # noqa: ISC001
            r'|IMPS.*UPI.*(?:rupee|INR|RS)'  # noqa: ISC001
            r')\b',
            _re.IGNORECASE,
        ),
    ),
    # Job coaching / interview support services
    (
        "interview_coaching",
        _re.compile(
            r'\b(?:job\s+support|interview\s+support|interview\s+preparation\s+service'
            r'|mock\s+interview|interview\s+coaching|interview\s+assist'
            r'|we\s+provide\s+structured\s+interview'
            r'|training\s+support\s+for\s+IT\s+professionals'
            r')\b',
            _re.IGNORECASE,
        ),
    ),
    # Social-media / YouTube channel promotions disguised as WFH jobs
    (
        "social_media_promo",
        _re.compile(
            r'(?:youtube|instagram|telegram)\s+chann?el.*task'
            r'|promote.*chann?el'
            r'|online.*youtube.*task'
            r'|earn.*(?:like|subscribe|view|share)',
            _re.IGNORECASE,
        ),
    ),
    # Forwarded supplier / trading / referral spam
    (
        "supplier_spam",
        _re.compile(
            r'24\s*\*\s*365.*(?:all.weather|supplier|work)'
            r'|reliable.*supplier.*(?:earn|income)'
            r'|IMPS|UPI.*bank\s+card',
            _re.IGNORECASE,
        ),
    ),
    # Course / bootcamp / training promotions that look like jobs
    (
        "course_promo",
        _re.compile(
            r'\b(?:full\s*stack\s*development\s*program|bootcamp|register\s+now|'
            r'learn\s+everything\s+from\s+scratch|perfect\s+for\s+beginners|'
            r'career\s+switchers|course\s+details|admission\s+open)\b',
            _re.IGNORECASE,
        ),
    ),
    # Job-seeker / resume-help chatter (not job postings)
    (
        "job_seeker_post",
        _re.compile(
            r'\b(?:looking\s+for\s+job|need\s+a\s+job|job\s+seeker|job\s+search|'
            r'anyone\s+hiring|please\s+refer|resume\s+tips|interview\s+questions)\b',
            _re.IGNORECASE,
        ),
    ),
]

_LEARNING_PROMO_CONTEXT = _re.compile(
    r'\b(?:want\s+to\s+become|become\s+a|learn\s+from\s+scratch|'
    r'program\s+helps\s+you\s+learn|perfect\s+for\s+beginners|'
    r'students|career\s+switchers|register\s+now|bootcamp|course)\b',
    _re.IGNORECASE,
)

_ENTRY_LEVEL_CONTEXT = _re.compile(
    r'\b(?:fresher|freshers|intern|internship|entry\s*level|trainee|graduate\s+program|'
    r'0\s*[-–]\s*1\s*(?:years?|yrs?)|0\s*[-–]\s*2\s*(?:years?|yrs?)|'
    r'up\s*to\s*2\s*(?:years?|yrs?))\b',
    _re.IGNORECASE,
)

_SOFT_NEGATIVE_KEYWORDS = {
    "job alert",
}


def _is_non_job_spam(text: str) -> Optional[str]:
    """Return the matched label string if text matches a non-job spam pattern, else None."""
    for label, pattern in _NON_JOB_PATTERNS:
        if pattern.search(text):
            return label
    return None


def _has_obfuscated_email(text: str) -> bool:
    """
    Detect very simple obfuscated email patterns like:
    - name [at] gmail [dot] com
    - name at gmail dot com
    This is only used to decide if a job has *some* way to apply.
    """
    patterns = [
        r"[A-Za-z0-9._%+-]+\s*(?:\[at\]|\(at\)|at)\s*[A-Za-z0-9.-]+\s*(?:\[dot\]|\(dot\)|dot|\.)\s*[A-Za-z]{2,}",
    ]
    for p in patterns:
        if _re.search(p, text, flags=_re.IGNORECASE):
            return True
    return False


def _normalize_text(text: str) -> str:
    """Normalize text for keyword matching."""
    return _re.sub(r"\s+", " ", (text or "").lower()).strip()


def _keyword_present(text: str, keyword: str) -> bool:
    """Word-boundary-aware keyword matching with phrase support."""
    text_norm = _normalize_text(text)
    keyword_norm = _normalize_text(keyword)
    if not text_norm or not keyword_norm:
        return False

    pattern = r'(?<!\w)' + _re.escape(keyword_norm).replace(r'\ ', r'\s+') + r'(?!\w)'
    return bool(_re.search(pattern, text_norm, flags=_re.IGNORECASE))


def _find_first_keyword_match(text: str, keywords: List[str]) -> Optional[str]:
    """Return first matching keyword (case-insensitive), else None."""
    for keyword in keywords or []:
        if _keyword_present(text, keyword):
            return keyword
    return None


def _find_first_skill_overlap(extracted_skills: List[str], excluded_skills: List[str]) -> Optional[str]:
    """Return first overlapping skill between extracted and excluded lists."""
    if not extracted_skills or not excluded_skills:
        return None

    extracted_norm = {_normalize_text(skill) for skill in extracted_skills if skill}
    for blocked in excluded_skills:
        blocked_norm = _normalize_text(blocked)
        if blocked_norm and blocked_norm in extracted_norm:
            return blocked
    return None


def _has_standard_email(text: str) -> bool:
    """Detect standard (non-obfuscated) email addresses."""
    return bool(
        _re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    )


class MLProcessorService:
    """
    Service to process Telegram messages using ML classification
    """
    
    def __init__(self):
        """Initialize ML processor with classifier and MongoDB connection"""
        self.classifier = SklearnClassifier()
        self.extractor = SpacyExtractor()
        self.enhanced_extractor = get_enhanced_extractor()  # NEW: Enhanced extraction
        self.quality_scorer = get_quality_scorer()  # NEW: Quality scoring
        self.senior_role_keywords = self._load_senior_role_keywords()
        
        # MongoDB connection
        self.mongo_client = MongoClient(settings.MONGODB_URI)
        self.mongo_db = self.mongo_client[settings.MONGODB_DB_NAME]
        self.messages_collection = self.mongo_db["raw_messages"]  # Changed from telegram_messages
        
        logger.info("✅ ML Processor Service initialized")
        logger.info(f"   Classifier loaded: {self.classifier.is_loaded}")
        logger.info(f"   Classifier version: {self.classifier.model_version}")
        logger.info(f"   spaCy extractor: {self.extractor.is_loaded}")
        logger.info("   Enhanced extractor: True")
        logger.info("   Quality scorer: True")
        logger.info(f"   Senior keywords loaded: {len(self.senior_role_keywords)}")

    def _load_senior_role_keywords(self) -> List[str]:
        """Load senior role keywords from relevance config with safe fallback."""
        try:
            config_path = settings.JOB_RELEVANCE_CONFIG_PATH
            path = Path(config_path)
            if not path.is_absolute():
                path = Path(__file__).resolve().parents[2] / config_path

            with open(path, "r", encoding="utf-8") as file_obj:
                cfg = json.load(file_obj)

            senior_keywords = (cfg.get("excluded_keywords", {}) or {}).get("senior_roles", []) or []
            return [item.strip() for item in senior_keywords if isinstance(item, str) and item.strip()]
        except Exception as exc:
            logger.warning(f"Could not load senior role keywords from config: {exc}")
            return [
                "10+ years", "15+ years", "senior architect", "vp of", "director of",
                "chief", "cto", "ceo", "head of", "senior manager", "principal engineer",
            ]

    def _get_active_preferences(self, db: Session) -> Optional[JobScrapingPreferences]:
        """Get active global scraping preferences if available."""
        return db.execute(
            select(JobScrapingPreferences)
            .where(JobScrapingPreferences.is_active.is_(True))
            .order_by(JobScrapingPreferences.updated_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    def _is_senior_keyword_match(self, text: str) -> Optional[str]:
        """Detect senior keyword while avoiding common learning-promo context false matches."""
        matched_keyword = _find_first_keyword_match(text, self.senior_role_keywords)
        if not matched_keyword:
            return None

        # Edge-case guard: content like "Want to become a ..." should be treated as promo/non-job.
        # Do not use this as a senior-job signal in that context.
        if _LEARNING_PROMO_CONTEXT.search(text):
            return None

        return matched_keyword

    @staticmethod
    def _experience_threshold(preferences: Optional[JobScrapingPreferences]) -> int:
        """Resolve experience threshold from active preferences with settings fallback."""
        if preferences and preferences.max_experience_years is not None:
            return int(preferences.max_experience_years)
        return int(settings.MAX_FRESHER_EXPERIENCE_YEARS)

    @staticmethod
    def _is_experience_over_threshold(extraction, threshold: int) -> bool:
        """Return True if parsed min/max experience exceeds threshold."""
        min_exp = extraction.experience_min
        max_exp = extraction.experience_max
        if min_exp is not None and min_exp > threshold:
            return True
        if max_exp is not None and max_exp > threshold:
            return True
        return False

    @staticmethod
    def _should_allow_soft_negative_keyword(
        matched_keyword: str,
        text: str,
        links: List[str],
    ) -> bool:
        """
        Allow specific soft negative keywords when strong entry-level job signals exist.

        Example: "job alert" post that is a real fresher/intern opening with apply path.
        """
        keyword_norm = _normalize_text(matched_keyword)
        if keyword_norm not in _SOFT_NEGATIVE_KEYWORDS:
            return False

        has_entry_level_signal = bool(_ENTRY_LEVEL_CONTEXT.search(text or ""))
        has_apply_path = bool(links) or _has_standard_email(text) or _has_obfuscated_email(text)
        has_non_job_promo = bool(_NON_JOB_PATTERNS[4][1].search(text or ""))  # course_promo pattern

        return has_entry_level_signal and has_apply_path and not has_non_job_promo
    
    def process_unprocessed_messages(
        self,
        limit: Optional[int] = None,
        min_confidence: float = 0.6
    ) -> Dict:
        """
        Process all unprocessed messages from MongoDB
        
        Args:
            limit: Maximum number of messages to process (None = all)
            min_confidence: Minimum confidence threshold for job classification
            
        Returns:
            Processing statistics
        """
        logger.info("=" * 80)
        logger.info("🚀 Starting ML Message Processing")
        logger.info("=" * 80)
        logger.info(f"   Min confidence threshold: {min_confidence}")
        if limit:
            logger.info(f"   Processing limit: {limit} messages")
        
        # Get unprocessed messages
        query = {"is_processed": False}  # Changed from processed to is_processed
        messages_cursor = self.messages_collection.find(query)
        
        if limit:
            messages_cursor = messages_cursor.limit(limit)
        
        messages = list(messages_cursor)
        total_messages = len(messages)
        
        logger.info(f"📊 Found {total_messages} unprocessed messages")
        
        if total_messages == 0:
            logger.info("✅ No messages to process")
            return {
                "success": True,
                "total_messages": 0,
                "job_messages": 0,
                "individual_jobs_created": 0,
                "companies_created": 0,
                "non_jobs": 0,
                "low_confidence": 0,
                "stored_to_postgres": 0,
                "errors": 0,
                "processing_time_ms": 0
            }
        
        # Process messages
        stats = {
            "total_messages": total_messages,
            "job_messages": 0,  # Messages classified as jobs
            "individual_jobs_created": 0,  # Actual job entries (can be > job_messages if splitting)
            "non_jobs": 0,
            "low_confidence": 0,
            "stored_to_postgres": 0,
            "companies_created": 0,  # New companies added
            "quality_filtered": 0,  # Jobs rejected for low quality score
            "spam_rejected": 0,    # Messages blocked by pre-filter (crypto/coaching/promo spam)
            "relevant_jobs": 0,  # Jobs meeting relevance criteria
            "errors": 0,
            "processing_time_ms": 0
        }
        
        start_time = datetime.now()
        
        # Get DB session for PostgreSQL (synchronous)
        db = next(get_sync_db())
        active_preferences = self._get_active_preferences(db)
        excluded_keywords = (active_preferences.excluded_keywords or []) if active_preferences else []
        if excluded_keywords:
            logger.info(f"Loaded {len(excluded_keywords)} active excluded keywords from preferences")
        
        try:
            for idx, message in enumerate(messages, 1):
                try:
                    logger.info(f"\n📝 Processing message {idx}/{total_messages}")
                    logger.info(f"   Channel: {message.get('channel_username', 'Unknown')}")  # Changed from channel_name
                    logger.info(f"   Message ID: {message.get('message_id')}")
                    
                    result = self._process_single_message(
                        message,
                        db,
                        min_confidence,
                        active_preferences=active_preferences,
                    )
                    
                    # Update stats with enhanced tracking
                    if result['is_job']:
                        stats['job_messages'] += 1
                        stats['individual_jobs_created'] += result['jobs_created']
                        stats['companies_created'] += result['companies_created']
                        stats['quality_filtered'] += result['quality_filtered']
                        stats['relevant_jobs'] += result['relevant_jobs']
                        stats['spam_rejected'] += result.get('spam_rejected', 0)
                        
                        if result['stored_to_postgres']:
                            stats['stored_to_postgres'] += result['jobs_created']
                        if result['low_confidence']:
                            stats['low_confidence'] += 1
                    else:
                        stats['non_jobs'] += 1
                        stats['spam_rejected'] += result.get('spam_rejected', 0)
                    
                    # Mark as processed in MongoDB
                    self.messages_collection.update_one(
                        {"_id": message["_id"]},
                        {
                            "$set": {
                                "is_processed": True,  # Changed from processed to is_processed
                                "processed_at": datetime.now(timezone.utc),
                                "ml_classification": {
                                    "is_job": result['is_job'],
                                    "confidence": result['confidence'],
                                    "reason": result['reason']
                                }
                            }
                        }
                    )

                    # Commit per message so one bad insert doesn't abort the full batch.
                    db.commit()
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message {message.get('message_id')}: {e}")
                    stats['errors'] += 1
                    db.rollback()
                    
                    # Still mark as processed to avoid reprocessing
                    self.messages_collection.update_one(
                        {"_id": message["_id"]},
                        {
                            "$set": {
                                "is_processed": True,  # Changed from processed to is_processed
                                "processed_at": datetime.now(timezone.utc),
                                "processing_error": str(e)
                            }
                        }
                    )
            
        except Exception as e:
            logger.error(f"❌ Fatal error during processing: {e}")
            db.rollback()
            stats['errors'] += 1
        
        finally:
            db.close()
        
        end_time = datetime.now()
        stats['processing_time_ms'] = (end_time - start_time).total_seconds() * 1000
        
        # Log summary
        logger.info("\n" + "=" * 80)
        logger.info("✅ ML Processing Complete!")
        logger.info("=" * 80)
        logger.info("📊 Summary:")
        logger.info(f"   Total messages processed: {stats['total_messages']}")
        logger.info(f"   Job messages found: {stats['job_messages']}")
        logger.info(f"   Individual jobs created: {stats['individual_jobs_created']}")
        logger.info(f"   Relevant jobs: {stats['relevant_jobs']}")
        logger.info(f"   Quality filtered: {stats['quality_filtered']}")
        logger.info(f"   Spam pre-filtered: {stats['spam_rejected']}")
        logger.info(f"   Companies auto-created: {stats['companies_created']}")
        logger.info(f"   Non-jobs: {stats['non_jobs']}")
        logger.info(f"   Low confidence jobs: {stats['low_confidence']}")
        logger.info(f"   Errors: {stats['errors']}")
        logger.info(f"   Processing time: {stats['processing_time_ms']:.2f}ms")
        logger.info(f"   Avg per message: {stats['processing_time_ms']/stats['total_messages']:.2f}ms")
        
        return stats
    
    def _process_single_message(
        self,
        message: Dict,
        db: Session,
        min_confidence: float,
        active_preferences: Optional[JobScrapingPreferences] = None,
    ) -> Dict:
        """
        Process a single message: classify, extract, store
        
        Returns:
            Processing result dict with jobs_created and companies_created counts
        """
        text = message.get("text", "")
        links = message.get("links", [])

        # 0. Pre-filter: reject known non-job spam patterns before hitting the ML model.
        #    This is faster and more reliable than relying on ML for these edge cases.
        spam_label = _is_non_job_spam(text)
        if spam_label:
            logger.info(f"   🚫 Pre-filtered as non-job spam [{spam_label}]: {text[:60]!r}")
            return {
                "is_job": False, "confidence": 0.0,
                "reason": f"pre-filter:{spam_label}",
                "low_confidence": False, "stored_to_postgres": False,
                "jobs_created": 0, "companies_created": 0,
                "quality_filtered": 0, "spam_rejected": 1, "relevant_jobs": 0,
                "job_ids": [], "channel_id": None,
            }

        # 0b. Preference-based excluded keyword gate (global active preferences)
        excluded_keywords = (active_preferences.excluded_keywords or []) if active_preferences else []
        excluded_keyword_match = _find_first_keyword_match(text, excluded_keywords)
        if excluded_keyword_match:
            if self._should_allow_soft_negative_keyword(excluded_keyword_match, text, links):
                logger.info(
                    f"   ✅ Soft negative keyword bypassed [{excluded_keyword_match}] due to entry-level signals"
                )
            else:
                logger.info(
                    f"   🚫 Rejected by preferences excluded keyword [{excluded_keyword_match}]: {text[:60]!r}"
                )
                return {
                    "is_job": False, "confidence": 0.0,
                    "reason": f"pre-filter:preferences_excluded_keyword:{excluded_keyword_match}",
                    "low_confidence": False, "stored_to_postgres": False,
                    "jobs_created": 0, "companies_created": 0,
                    "quality_filtered": 0, "spam_rejected": 1, "relevant_jobs": 0,
                    "job_ids": [], "channel_id": None,
                }

        # 1. Classify the message
        classification = self.classifier.classify(text)
        
        logger.info(f"   🤖 Classification: is_job={classification.is_job}, "
                   f"confidence={classification.confidence:.2f}")
        logger.info(f"      Reason: {classification.reason}")
        
        result = {
            "is_job": classification.is_job,
            "confidence": classification.confidence,
            "reason": classification.reason,
            "low_confidence": classification.confidence < min_confidence,
            "stored_to_postgres": False,
            "jobs_created": 0,
            "companies_created": 0,
            "quality_filtered": 0,
            "relevant_jobs": 0,
            "job_ids": [],
            "channel_id": None  # Track channel for aggregation
        }
        
        # 2. If it's a job, extract details and store
        if classification.is_job:
            # Message-level deduplication guard:
            # If this Telegram message_id was already processed earlier,
            # skip the whole message. This still allows multiple extracted jobs
            # from the same *current* message in this run.
            _msg_id = str(message.get("message_id", ""))
            if _msg_id:
                existing_by_msg = db.execute(
                    select(Job.id).where(Job.source_message_id == _msg_id)
                ).first()
                if existing_by_msg:
                    logger.info(
                        f"   ⏭️  Duplicate message_id={_msg_id} already stored, skipping message"
                    )
                    return result

            # Exact message text deduplication guard:
            # Normalize message text, hash it, and check if exact same text already exists.
            # This catches cases where the same message is posted with different message_id.
            if text and len(text.strip()) > 20:  # Only check meaningful messages
                # Use deduplication service to compute hash (matches how content_hash is stored)
                text_hash = deduplication_service.compute_content_hash(text)
                
                # Check if this hash already exists
                existing_by_hash = db.execute(
                    select(Job.id).where(Job.content_hash == text_hash)
                ).first()
                
                if existing_by_hash:
                    logger.info(
                        f"   ⏭️  Duplicate exact message text (hash={text_hash[:8]}...) already stored, skipping message"
                    )
                    return result
            
            # Check confidence threshold
            if classification.confidence < min_confidence:
                logger.warning(f"   ⚠️  Low confidence ({classification.confidence:.2f}), "
                             f"still storing but flagging for review")
            
            # Extract job details using enhanced extractor (returns LIST of jobs)
            extractions = self.enhanced_extractor.extract_jobs_from_message(text, links)
            
            logger.info(f"   📦 Found {len(extractions)} job(s) in message")
            
            # 3. Process each extracted job
            for idx, extraction in enumerate(extractions, 1):
                try:
                    logger.info(f"   📋 Processing job {idx}/{len(extractions)}:")
                    logger.info(f"      Company: {extraction.company_name or 'Not found'}")
                    logger.info(f"      Job Title: {extraction.job_title or 'Not found'}")
                    logger.info(f"      Category: {extraction.job_category or 'Not classified'}")
                    logger.info(f"      Location: {extraction.location or 'Not found'}")
                    logger.info(f"      Salary: {extraction.salary_raw or 'Not found'}")
                    logger.info(f"      Experience: {extraction.experience_raw or 'Not found'}")
                    logger.info(f"      Skills: {', '.join(extraction.skills[:5]) if extraction.skills else 'None'}")
                    logger.info(f"      Apply Link: {extraction.apply_link or 'Not found'}")
                    logger.info(f"      Confidence: {extraction.confidence:.2f}")
                    
                    # Skip jobs with 0 confidence (filtered by international onsite check)
                    if extraction.confidence == 0.0:
                        logger.warning(f"      ⚠️  Job rejected by international filtering")
                        result['quality_filtered'] += 1
                        continue

                    # Require at least one way to apply: link or email in text.
                    # If there is no apply link and no email address, drop this job.
                    has_apply_link = bool(extraction.apply_link)
                    has_email_in_text = bool(
                        _re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
                    ) or _has_obfuscated_email(text)
                    if not has_apply_link and not has_email_in_text:
                        logger.warning(
                            "      ⚠️  Job rejected: no apply link or email found in message"
                        )
                        result["quality_filtered"] += 1
                        continue

                    # Hard senior gates (both):
                    # 1) parsed experience exceeds threshold
                    # 2) senior-role keyword appears in title/description/raw text
                    threshold = self._experience_threshold(active_preferences)
                    if self._is_experience_over_threshold(extraction, threshold):
                        logger.warning(
                            f"      ⚠️  Job rejected [senior_experience_gate]: "
                            f"experience_min={extraction.experience_min}, experience_max={extraction.experience_max} "
                            f"(gate: max {threshold} yrs)"
                        )
                        result['quality_filtered'] += 1
                        continue

                    senior_keyword_match = self._is_senior_keyword_match(
                        f"{extraction.job_title or ''} {extraction.description or text}"
                    )
                    if senior_keyword_match:
                        logger.warning(
                            f"      ⚠️  Job rejected [senior_keyword_gate]: matched '{senior_keyword_match}'"
                        )
                        result['quality_filtered'] += 1
                        continue

                    # Reject jobs with no extractable title (avoids 'Job Opening' garbage)
                    if not extraction.job_title and not extraction.company_name:
                        logger.warning(f"      ⚠️  Job rejected: no title or company extractable")
                        result['quality_filtered'] += 1
                        continue

                    # Preference gates: excluded companies and excluded skills
                    if active_preferences:
                        excluded_company_match = _find_first_keyword_match(
                            extraction.company_name or "",
                            active_preferences.excluded_companies or [],
                        )
                        if excluded_company_match:
                            logger.warning(
                                f"      ⚠️  Job rejected [preferences_excluded_company]: matched '{excluded_company_match}'"
                            )
                            result['quality_filtered'] += 1
                            continue

                        excluded_skill_match = _find_first_skill_overlap(
                            extraction.skills or [],
                            active_preferences.excluded_skills or [],
                        )
                        if excluded_skill_match:
                            logger.warning(
                                f"      ⚠️  Job rejected [preferences_excluded_skill]: matched '{excluded_skill_match}'"
                            )
                            result['quality_filtered'] += 1
                            continue

                    # ── Deduplication guard ──────────────────────────────────────────
                    # Heuristic guard on (title, company, apply_link):
                    #    Handles cases where effectively the same job is posted again
                    #    with a different message_id but identical core fields.
                    # Normalise title and company a bit for dedup:
                    # - strip spaces
                    # - lowercase
                    # - drop common suffixes like " (remote)" on title and " india", "pvt ltd" on company.
                    raw_title = (extraction.job_title or extraction.company_name or "").strip()
                    raw_company = (extraction.company_name or "").strip()

                    base_title = raw_title.lower()
                    # Remove common remote markers at the end
                    for suffix in [" (remote)", " - remote", " – remote"]:
                        if base_title.endswith(suffix):
                            base_title = base_title[: -len(suffix)].strip()
                            break

                    base_company = raw_company.lower()
                    for suffix in [" pvt ltd", " pvt. ltd", " private limited", " india"]:
                        if base_company.endswith(suffix):
                            base_company = base_company[: -len(suffix)].strip()
                            break

                    candidate_title = base_title
                    candidate_company = base_company
                    candidate_apply_link = extraction.apply_link or ""

                    if candidate_title and candidate_company and candidate_apply_link:
                        # Normalise apply link slightly: ignore query params/tracking
                        # when matching against existing jobs, and treat http/https as same.
                        base_apply_link = candidate_apply_link.split("?", 1)[0].rstrip("/")

                        existing_by_fields = db.execute(
                            select(Job.id).where(
                                func.lower(Job.title) == candidate_title,
                                func.lower(Job.company_name) == candidate_company,
                                or_(
                                    Job.source_url == candidate_apply_link,
                                    Job.source_url.ilike(f"{base_apply_link}%"),
                                ),
                            )
                        ).first()

                        if existing_by_fields:
                            logger.info(
                                "      ⏭️  Duplicate — same title/company/apply_link "
                                "already stored, skipping"
                            )
                            continue
                    # ── End deduplication guard ───────────────────────────────────────

                    # Get or create company
                    company = None
                    if extraction.company_name:
                        company = self.enhanced_extractor.get_or_create_company(
                            db, 
                            extraction.company_name
                        )
                        
                        if company:
                            # Check if this is a newly created company
                            if not hasattr(company, '_is_new'):
                                # Company was fetched from DB
                                extraction.company_id = company.id
                                logger.info(f"      🏢 Using existing company: {company.name} (ID: {company.id})")
                            else:
                                # Company was just created
                                extraction.company_id = company.id
                                result['companies_created'] += 1
                                logger.info(f"      🏢 Created new company: {company.name} (ID: {company.id})")
                    
                    # Extract telegram metadata from message (NEW)
                    telegram_group_id = None  # Always None - telegram_groups table is empty
                    scraped_by_account_id = None  # Always None - telegram_accounts table is empty
                    fetched_by_account_value = None
                    sender_id_value = None
                    channel_id_value = None
                    
                    # Get sender_id from message
                    sender_id = message.get("sender_id")
                    if sender_id:
                        sender_id_value = int(sender_id)
                    
                    # Get channel_id from message (actual Telegram channel ID)
                    channel_id = message.get("channel_id")
                    channel_username = message.get("channel_username")
                    if channel_id:
                        channel_id_value = str(channel_id)
                        # Store for channel aggregation
                        result['channel_id'] = channel_id_value
                        result['channel_username'] = channel_username
                        # NOTE: We don't link to telegram_groups table because it's empty
                        # We store the channel_id directly in source_telegram_channel_id
                    
                    # Get account ID from message (store the integer value)
                    fetched_by_account = message.get("fetched_by_account")
                    if fetched_by_account:
                        fetched_by_account_value = int(fetched_by_account)
                        # NOTE: We don't link to telegram_accounts table because it's empty
                        # We store the account ID directly in fetched_by_account field
                    
                    # Create Job entry with all enhanced fields
                    job = Job(
                        title=extraction.job_title or extraction.company_name,
                        company_id=extraction.company_id,
                        # Denormalized so recommendations never need to load
                        # the companies relationship on display.
                        company_name=company.name if company else extraction.company_name,
                        description=extraction.description[:5000] if extraction.description else text[:5000],
                        location=extraction.location,
                        
                        # Job details with NEW simplified extraction
                        skills_required=extraction.skills if extraction.skills else [],
                        experience_required=extraction.experience_raw,  # NEW: String format "2-4 years"
                        
                        # Numeric experience fields (for filtering)
                        experience_min=extraction.experience_min,
                        experience_max=extraction.experience_max,
                        is_fresher=extraction.is_fresher_friendly,
                        
                        # Salary fields (NEW: simplified monthly value)
                        salary_min=extraction.salary_min,  # Monthly INR from new method
                        salary_max=extraction.salary_max,
                        salary_range={  # Legacy JSONB field
                            "min": extraction.salary_min,
                            "max": extraction.salary_max,
                            "currency": extraction.salary_currency or "INR",
                            "raw": extraction.salary_raw
                        } if extraction.salary_raw else {},
                        
                        job_type="fulltime",  # Default
                        employment_type="fulltime",  # Default
                        
                        # Source information
                        source="telegram",
                        source_url=extraction.apply_link,
                        raw_text=text,  # Full original message text
                        source_message_id=str(message.get("message_id")),
                        source_telegram_channel_id=channel_id_value,  # NEW - Actual Telegram channel ID
                        sender_id=sender_id_value,  # NEW - Sender user ID
                        telegram_group_id=telegram_group_id,  # NEW
                        scraped_by_account_id=scraped_by_account_id,  # NEW
                        fetched_by_account=fetched_by_account_value,  # NEW - Store MongoDB account ID
                        
                        # ML metadata
                        ml_confidence=str(round(extraction.confidence, 2)),
                        
                        # Status
                        is_active=classification.confidence >= min_confidence,
                        is_verified=False,
                        
                        # Stats
                        view_count=0,
                        application_count=0
                    )
                    
                    # Generate content hash for deduplication
                    job.content_hash = deduplication_service.compute_content_hash(text)
                    
                    # Derive work_type from location intelligence
                    _loc = extraction.location_data or {}
                    if _loc.get('is_remote'):
                        _work_type = 'remote'
                    elif _loc.get('is_hybrid'):
                        _work_type = 'hybrid'
                    elif _loc.get('is_onsite_only'):
                        _work_type = 'onsite'
                    else:
                        _work_type = None
                    job.work_type = _work_type

                    # Score job quality before storing
                    job_data = {
                        "title": job.title,
                        "description": job.description,
                        "skills": job.skills_required or [],
                        "skills_required": job.skills_required or [],
                        "experience": job.experience_required,
                        "experience_min": job.experience_min,
                        "experience_max": job.experience_max,
                        "is_fresher": job.is_fresher,
                        "work_type": _work_type,
                        "salary_min": job.salary_min,
                        "salary_max": job.salary_max,
                        "salary": job.salary_range.get("raw") if job.salary_range else None,
                        "location": job.location,
                        "source_url": job.source_url,
                        "apply_link": job.source_url,
                        "company": company.name if company else None
                    }
                    
                    quality_result = self.quality_scorer.score_job(job_data, extraction.confidence)
                    
                    # Set quality scoring fields
                    job.quality_score = quality_result.quality_score
                    job.relevance_score = quality_result.relevance_score
                    job.meets_relevance_criteria = quality_result.meets_criteria
                    job.quality_breakdown = quality_result.breakdown
                    job.relevance_reasons = quality_result.reasons
                    job.extraction_completeness_score = quality_result.breakdown.get("completeness", 0)
                    job.quality_factors = {
                        "experience_match": quality_result.breakdown.get("experience_match", 0),
                        "completeness": quality_result.breakdown.get("completeness", 0),
                        "skills_value": quality_result.breakdown.get("skills_value", 0)
                    }
                    
                    logger.info(f"      🎯 Quality Score: {quality_result.quality_score:.2f}/100")
                    logger.info(f"      📊 Relevance: {quality_result.relevance_score:.2f}/100")
                    logger.info(f"      ✓ Meets Criteria: {quality_result.meets_criteria}")
                    
                    # Filter jobs below minimum quality threshold
                    if quality_result.quality_score < settings.JOB_QUALITY_MIN_SCORE:
                        logger.warning(f"      ⚠️  Job rejected: Quality score {quality_result.quality_score:.2f} < {settings.JOB_QUALITY_MIN_SCORE}")
                        result['quality_filtered'] += 1
                        continue  # Skip this job
                    
                    # Track relevant jobs
                    if quality_result.meets_criteria:
                        result['relevant_jobs'] += 1
                    
                    db.add(job)
                    db.flush()  # Get the ID
                    
                    result['jobs_created'] += 1
                    result['job_ids'].append(str(job.id))
                    logger.info(f"      ✅ Stored to PostgreSQL (Job ID: {job.id})")
                    
                except Exception as e:
                    logger.error(f"      ❌ Error storing job {idx} to PostgreSQL: {e}")
                    db.rollback()
                    # Continue processing other jobs in the message
                    continue
            
            # Mark as stored if at least one job was created
            if result['jobs_created'] > 0:
                result['stored_to_postgres'] = True
                
                # NEW: Update channel aggregation metrics
                if result.get('channel_username'):
                    self._update_channel_metrics(
                        db, 
                        result['channel_username'],
                        jobs_created=result['jobs_created'],
                        relevant_jobs=result['relevant_jobs']
                    )
        
        return result
    
    def _update_channel_metrics(
        self,
        db: Session,
        channel_username: str,
        jobs_created: int,
        relevant_jobs: int
    ):
        """
        Update channel aggregation metrics and recalculate health score
        
        Args:
            db: Database session
            channel_username: Channel username (e.g., @channelname or channelname)
            jobs_created: Number of jobs created from this batch
            relevant_jobs: Number of relevant jobs in this batch
        """
        try:
            # Normalize username (ensure @ prefix)
            if not channel_username.startswith('@'):
                channel_username = f'@{channel_username}'
            
            # Find or create channel
            channel = db.query(TelegramGroup).filter(
                TelegramGroup.username == channel_username
            ).first()
            
            if not channel:
                logger.warning(f"   ⚠️  Channel {channel_username} not found in telegram_groups table, skipping metrics update")
                return
            
            # Update job counts
            channel.total_jobs_posted = (channel.total_jobs_posted or 0) + jobs_created
            channel.relevant_jobs_count = (channel.relevant_jobs_count or 0) + relevant_jobs
            
            # Calculate relevance ratio
            if channel.total_jobs_posted > 0:
                channel.relevance_ratio = channel.relevant_jobs_count / channel.total_jobs_posted
            
            # Calculate average job quality score — scalar aggregation, not full table load.
            # (Old approach loaded every job ORM object to compute the average in Python,
            # which becomes O(n) per processed message as the jobs table grows.)
            avg_quality = db.query(
                func.avg(Job.quality_score)
            ).filter(
                Job.source_telegram_channel_id == str(channel.username).replace('@', ''),
                Job.quality_score.isnot(None)
            ).scalar()

            if avg_quality is not None:
                channel.avg_job_quality_score = float(avg_quality)
            
            # Update last job posted timestamp
            from datetime import datetime, timezone
            channel.last_job_posted_at = datetime.now(timezone.utc)
            
            # Recalculate health score
            new_score = channel.calculate_health_score()
            
            logger.info(f"   📊 Updated channel {channel_username}:")
            logger.info(f"      Total jobs: {channel.total_jobs_posted}")
            logger.info(f"      Relevant jobs: {channel.relevant_jobs_count}")
            logger.info(f"      Relevance ratio: {channel.relevance_ratio:.2%}")
            logger.info(f"      Avg quality: {channel.avg_job_quality_score:.2f}")
            logger.info(f"      Health score: {new_score:.2f}")
            logger.info(f"      Status: {channel.status_label}")
            
            db.flush()
            
        except Exception as e:
            logger.error(f"   ❌ Error updating channel metrics for {channel_username}: {e}")
    
    def get_processing_stats(self) -> Dict:
        """Get statistics about processed/unprocessed messages"""
        total = self.messages_collection.count_documents({})
        processed = self.messages_collection.count_documents({"is_processed": True})  # Changed
        unprocessed = self.messages_collection.count_documents({"is_processed": False})  # Changed
        
        # Get job classification stats
        jobs = self.messages_collection.count_documents({
            "is_processed": True,  # Changed
            "ml_classification.is_job": True
        })
        
        return {
            "total_messages": total,
            "processed": processed,
            "unprocessed": unprocessed,
            "jobs_classified": jobs,
            "processing_rate": (processed / total * 100) if total > 0 else 0
        }
    
    def close(self):
        """Close MongoDB connection"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("✅ MongoDB connection closed")


# Singleton instance
_ml_processor = None


def get_ml_processor() -> MLProcessorService:
    """Get or create ML processor singleton"""
    global _ml_processor
    if _ml_processor is None:
        _ml_processor = MLProcessorService()
    return _ml_processor
