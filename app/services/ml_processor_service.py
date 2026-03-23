"""
ML Processor Service
Processes unprocessed messages from MongoDB, classifies them, and stores jobs to PostgreSQL
"""

import logging
import re as _re
import unicodedata as _ud
from datetime import datetime, timezone
from typing import List, Dict, Optional
from pymongo import MongoClient
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session

from app.config import Settings
from app.db.session import get_sync_db
from app.db.base import Base  # Import Base to initialize all models
# Import all models at once to ensure proper relationship initialization
from app.models import Job, Company, Channel, Application, Student, User, TelegramGroup
from app.ml.sklearn_classifier import SklearnClassifier
from app.ml.spacy_extractor import SpacyExtractor
from app.ml.enhanced_extractor import get_enhanced_extractor
from app.services.job_quality_scorer import get_quality_scorer
from app.services.deduplication_service import deduplication_service

# Load settings
settings = Settings()

logger = logging.getLogger(__name__)

_LEARNING_CONTEXT_TERMS = [
    "bootcamp", "course", "training", "class", "cohort", "learn", "become",
    "tutorial", "workshop", "masterclass", "academy", "upskill", "mentorship",
]

_ENTRY_LEVEL_SIGNALS = [
    "intern", "internship", "fresher", "freshers", "entry level", "entry-level",
    "0-1", "0-2", "0 to 2", "1-2", "1 to 2", "junior",
]

_HARD_NEGATIVE_KEYWORDS = {
    "urgent requirement for client",
}

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
    # Course / class / training promotions (even if they mention internship outcomes)
    (
        "training_promo",
        _re.compile(
            r'\b(?:admission\s+open|enroll\s+now|join\s+our\s+(?:course|batch|class)'
            r'|placement\s+assistance\s+course|internship\s+training\s+program'
            r'|placement\s+assistance\s+program'
            r'|job\s+ready\s+course|learn\s+\w+\s+in\s+\d+\s+days'
            r'|institute\s+of\s+\w+|coaching\s+center|course\s+fee'
            r'|training\s+fee|certificate\s+course|paid\s+training\s+program'
            r'|skill\s+development\s+course|live\s+classes?\s+starting'
            r'|top\s+certifications?\s+offered\s+by\s+iit(?:\'s|\s*’\s*s)?\s*(?:&|and)\s*iim'
            r'|placement\s+assistance\s+with\s+\d+\+?\s+companies'
            r'|limited\s+seats\s+only|only\s+\d+\s+seats\s+left|only\s+\d+\s+seats\b'
            r'|(?:python|automation|web|full\s+stack|data|(?:java|java\s+script))\s+(?:mastery|master)\s+course'
            r'|final\s+call\s*(?:–|-)\s*only|hurry\s+[.!]*\s*limited\s+seats'
            r'|don\'?t\s+let.*gap\s+year|fast-?track.*course|beginner-?friendly.*course'
            r'|job[-\s]*assured\s+.*\bcourse\b|unlimited\s+plac(?:e)?ment\s+calls?'
               r'|till\s+you\s+get\s+placed|full\s+stack\s+course\s+starting'
               r'|skill.{0,3}development\s+program|internshala\s+trainings?'
               r'|\(not\s+a\s+job\s+offer\)|get\s+certified\s+courses?'
            r'|enroll\s+using\s+this\s+link|industry.{0,5}recognized\s+certificate'
            r'|recruitment\s+fair\s+for\s+\d{4}|meet\s+hiring\s+managers\s+face-to-face'
            r'|event\s+details\s*:\s*|register\s+now\s*:\s*https?://(?:go\.)?gov\.sg/'
            r'|for\s+singaporeans\s+only|last\s+registration\s+at\s*\d'
            r'|free\s+(?:automation|qa|testing)\s+masterclass|next\s+gen\s+qa'
            r'|no-?code\s+automation|register\s+now\s+for\s+free|live\s+q\s*&\s*a'
            r'|participation\s+certificate|guvi\.in/|link\.guvi\.in/)\b',
            _re.IGNORECASE,
        ),
    ),
    # Paid HR-contact lead generation / resume shortcut ads (not real direct job posts)
    (
        "hr_leadgen_ad",
        _re.compile(
            r'(?:\b\d+\+\s*verified\s+hr\s+email\s+ids?\b'
            r'|connect\s+directly\s+with\s+hr(?:s|\s+professionals)?'
            r'|increase\s+your\s+interview\s+chances'
            r'|only\s*₹?\s*\d+\s*(?:only)?\s*$'
            r'|whatsapp\s+us\s+on\s*:\s*\+?\d{10,}'
            r'|direct\s+test\s+hiring\s*,?\s*no\s+resume\s+shortlisting'
            r'|eligible\s+batches\s*:\s*20\d{2}(?:\s*,\s*20\d{2}){2,})',
            _re.IGNORECASE,
        ),
    ),
    # Paid resume-writing / ATS optimization service offers (not direct jobs)
    (
        "resume_service_ad",
        _re.compile(
            r'(?:stop\s+getting\s+rejected\s+by\s+ats'
            r'|\b\d{1,3}%\s+of\s+resumes\s+are\s+filtered\s+out\b'
            r'|premium\s+resume\s+service\s*\(\s*worth\s*₹?\s*\d{1,3}(?:,\d{3})*\s*\)'
            r'|ats\s+optimization\s*:\s*we\s+use\s+high-?ranking\s+keywords'
            r'|only\s+\d+\s+candidates\s+for\s+this\s+batch'
            r'|book\s+your\s+slot\s+now\s*:\s*https?://rzp\.io/rzp/'
            r'|special\s+limited\s+time\s+offer'
            r'|price\s+will\s+go\s+back\s+to\s+₹?\s*\d{1,3}(?:,\d{3})*)',
            _re.IGNORECASE,
        ),
    ),
    # Funded prop-trading opportunity ads (not standard placement jobs)
    (
        "trading_offer_ad",
        _re.compile(
            r'(?:\bfunded\s+day\s+trader\b'
            r'|\bprop\s+trading\s+team\b'
            r'|\bfirm-?backed\s+capital\b'
            r'|labor24\.in/day-trader_'
            r'|forex\s*,?\s*crypto\s*&\s*metals)',
            _re.IGNORECASE,
        ),
    ),
    # Digital marketing paid-ads hiring templates to exclude from this pipeline
    (
        "marketing_hiring_ad",
        _re.compile(
            r'(?:\bpaid\s+ads\s+experts?\b'
            r'|\bgoogle\s+ads\s+specialist\b'
            r'|\bmeta\s+ads\s+specialist\b'
            r'|\bmedia\s+buyer\b'
            r'|\bperformance\s+max\b'
            r'|\b(?:roas|cpa|cpl|ctr)\b.{0,40}\b(?:roas|cpa|cpl|ctr)\b'
            r'|pixel\s+setup\s*&\s*event\s+tracking\s*\(meta\)'
            r'|share\s+your\s+cv\s+at\s*:\s*[^\s]+@jobscapital\.in)'
            ,
            _re.IGNORECASE,
        ),
    ),
    # Job-search toolkit + training bundle promos (not direct job postings)
    (
        "job_search_tools_promo",
        _re.compile(
            r'(?:ai[-\s]*powered\s+tools\s+to\s+accelerate\s+your\s+job\s+search'
            r'|ats\s+score\s+checker|resume\s+builder|interview\s+preparation'
            r'|cover\s+letter\s+generator|salary\s+estimator'
            r'|careertoolkit\.in/(?:ats-score-checker|resume-builder|interview-preparation|cover-letter-generator|salary-estimator)'
            r'|trainings?\s*&\s*certifications?'
            r'|new\s+batches\s+starting\s+soon(?:\s*[–-]\s*register\s+now)?'
            r'|selenium\s+java\s+free\s+demo|fill\s+google\s+form|forms\.gle/'
            r'|free\s+job\s+opportunities\s*&\s*it\s+referrals?'
            r'|it\s+referral\s+jobs?\s*[–-]?\s*telegram)'
            r'',
            _re.IGNORECASE,
        ),
    ),
    # Pay After Placement / Training Program ads
    (
        "pay_after_placement_ad",
        _re.compile(
            r'\b(?:pay\s+after\s+placement|trusted\s+by\s+\d+\+?\s+students'
            r'|hiring\s+partners|avg\.?\s+package:|highest:\s+₹|lpa\s*\|'
            r'|iit\s+alumni.*top\s+tech|placement\s+training\s+program'
            r'|course\s+enrollment|learn\s+full\s+stack)\b',
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
    # Incomplete / truncated job postings (missing job title or core details)
    (
        "incomplete_posting",
        _re.compile(
            r'^\s*(?:\d+\.|day\s+\d+:).*(?:interview|offer|joining)'
            r'|(?:immediate\s+joiner|hit\s+the\s+ground\s+running)(?:.*only)?$'
            r'|^\s*>.*immediate\s+joiner|notice\s+period.*ground\s+running',
            _re.IGNORECASE | _re.MULTILINE,
        ),
    ),
    # Group-invite / community-rule messages (not actual job postings)
    (
        "group_invite_rules",
        _re.compile(
            r'\b(?:welcome\s+to\s+vrs\s+jobs|only\s+indian\s+group|group\s+link'
            r'|no\s+external\s+links\s*\(other\s+than\s+vrs\s+jobs\s+portal\s+link\)'
            r'|team\s+vrsamadhan|join\s+our\s+community\s+without\s+worrying\s+about\s+your\s+privacy'
            r'|exclusive\s+access\s*:\s*our\s+private\s+channel'
            r'|elite\s+methods|unique\s+strategies|join\s+now\s+or\s+you\s+will\s+miss\s+it'
            r'|unlock\s+kijiye|t\.me/\+)\b',
            _re.IGNORECASE,
        ),
    ),
    # Walk-in drives / shortlist announcements / link dumps (broadcast ads, not direct job cards)
    (
        "hiring_broadcast_ad",
        _re.compile(
            r'(?:walk-?in\s+(?:drive|interview)\s+alert'
            r'|walk-?in\s+details'
            r'|you\s+may\s+also\s+directly\s+walk\s+in\s+to\s+the\s+mentioned\s+venue'
            r'|mode\s+of\s+interview\s*:\s*(?:face\s*to\s*face|in-?person)(?:\s*/\s*(?:face\s*to\s*face|in-?person))?'
            r'|registration\s*time\s*:\s*\d{1,2}:\d{2}\s*(?:am|pm)?\s*(?:to|-)\s*\d{1,2}:\d{2}\s*(?:am|pm)?'
            r'|search\s+with\s+the\s+job\s+id(?:\'s|s)?\s+available\s+in\s+the\s+attached\s+flyer'
            r'|offline\s+pen\s+paper\s+test|shortlisted\s+for\s+an\s+“?offline\s+pen\s+paper\s+test'
            r'|this\s+week\'s\s+trending\s+jobs|must\s+apply\s+asap'
            r'|commonjobs\.in/job/|sending\s+test\s+emails'
            r'|try\s+to\s+get\s+a\s+referral\s+for\s+each\s+of\s+them'
            r'|please\s+do\s+react\s+to\s+the\s+post\s*[—-]?\s*it\s+takes\s+effort'
            r'|share\s+or\s+repost\s+within\s+your\s+network)',
            _re.IGNORECASE,
        ),
    ),
    # Low-quality lead-gen WFH promos (salary-only + WhatsApp/email apply format)
    (
        "wfh_leadgen_ad",
        _re.compile(
            r'\b(?:work\s+from\s+home\s+job.*business\s+development\s+executive'
            r'|only\s+female\s+candidates?\s+are\s+suitable\s+for\s+this\s+job\s+role'
            r'|share\s+your\s+resume\s+to\s*\+?91\s*73392\s*62880'
            r'|elitewheelallience@gmail\.com)\b',
            _re.IGNORECASE,
        ),
    ),
]


def _is_non_job_spam(text: str) -> Optional[str]:
    """Return the matched label string if text matches a non-job spam pattern, else None."""
    normalized_text = _ud.normalize("NFKD", text)

    for label, pattern in _NON_JOB_PATTERNS:
        if pattern.search(text) or pattern.search(normalized_text):
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


def _keyword_present(text: str, keyword: str) -> bool:
    """Case-insensitive keyword match with word-boundary safety for simple terms."""
    if not text or not keyword:
        return False

    normalized_text = text.lower()
    normalized_keyword = keyword.strip().lower()

    if not normalized_keyword:
        return False

    if _re.match(r"^[a-z0-9][a-z0-9\s\-/]*[a-z0-9]$", normalized_keyword):
        body = _re.escape(normalized_keyword).replace(r"\ ", r"\s+")
        return bool(_re.search(rf"\b{body}\b", normalized_text, flags=_re.IGNORECASE))

    return normalized_keyword in normalized_text


def _find_first_keyword_match(text: str, keywords: List[str]) -> Optional[str]:
    """Return first matching keyword from a list, else None."""
    for keyword in keywords:
        if _keyword_present(text, keyword):
            return keyword
    return None


def _find_first_skill_overlap(extracted_skills: List[str], blocked_skills: List[str]) -> Optional[str]:
    """Return first blocked skill that appears in extracted skills (case-insensitive)."""
    extracted_lower = {skill.strip().lower() for skill in extracted_skills if skill and skill.strip()}
    for blocked in blocked_skills:
        if blocked and blocked.strip().lower() in extracted_lower:
            return blocked
    return None


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
        self.senior_role_keywords = (
            self.quality_scorer.excluded_keywords.get("senior_roles", [])
            if self.quality_scorer and self.quality_scorer.excluded_keywords
            else []
        )
        
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

    def _is_senior_keyword_match(self, text: str) -> Optional[str]:
        """
        Detect senior-role keyword unless text is clearly a learning/training context.
        """
        if not text:
            return None

        if any(_keyword_present(text, term) for term in _LEARNING_CONTEXT_TERMS):
            return None

        return _find_first_keyword_match(text, self.senior_role_keywords)

    @staticmethod
    def _is_experience_over_threshold(extraction, max_years: int) -> bool:
        """Return True if extraction's min/max experience exceeds configured threshold."""
        min_exp = getattr(extraction, "experience_min", None)
        max_exp = getattr(extraction, "experience_max", None)

        min_val = int(min_exp) if isinstance(min_exp, (int, float)) else None
        max_val = int(max_exp) if isinstance(max_exp, (int, float)) else None

        if max_val is not None and max_val > max_years:
            return True
        if min_val is not None and min_val > max_years:
            return True
        return False

    @staticmethod
    def _should_allow_soft_negative_keyword(keyword: str, text: str, links: List[str]) -> bool:
        """
        Allow only soft negative keywords when clear entry-level signal + apply path exist.
        Hard negative keywords are never bypassed.
        """
        normalized_keyword = (keyword or "").strip().lower()
        normalized_text = (text or "").lower()

        if not normalized_keyword:
            return False

        if normalized_keyword in _HARD_NEGATIVE_KEYWORDS:
            return False

        has_apply_path = bool(links) or _has_obfuscated_email(text) or bool(
            _re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
        )

        has_entry_level_signal = any(
            _keyword_present(normalized_text, signal) for signal in _ENTRY_LEVEL_SIGNALS
        )

        return has_apply_path and has_entry_level_signal
    
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
        
        try:
            for idx, message in enumerate(messages, 1):
                try:
                    logger.info(f"\n📝 Processing message {idx}/{total_messages}")
                    logger.info(f"   Channel: {message.get('channel_username', 'Unknown')}")  # Changed from channel_name
                    logger.info(f"   Message ID: {message.get('message_id')}")
                    
                    result = self._process_single_message(message, db, min_confidence)
                    
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
        min_confidence: float
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

                    # Hard experience gate — driven by settings.MAX_FRESHER_EXPERIENCE_YEARS
                    # To raise/lower the threshold change that single config value.
                    # is_fresher_friendly is already normalised by the extractor override,
                    # so we just check the flag here.
                    if (
                        not extraction.is_fresher_friendly
                        or self._is_experience_over_threshold(
                            extraction, settings.MAX_FRESHER_EXPERIENCE_YEARS
                        )
                    ):
                        logger.warning(
                            f"      ⚠️  Job rejected: experience_min={extraction.experience_min} yrs "
                            f"(gate: max {settings.MAX_FRESHER_EXPERIENCE_YEARS} yrs)"
                        )
                        result['quality_filtered'] += 1
                        continue

                    # Reject jobs with no extractable title (avoids 'Job Opening' garbage)
                    if not extraction.job_title and not extraction.company_name:
                        logger.warning(f"      ⚠️  Job rejected: no title or company extractable")
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

                    if not quality_result.meets_criteria:
                        logger.warning("      ⚠️  Job rejected: failed relevance criteria checks")
                        result['quality_filtered'] += 1
                        continue
                    
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
