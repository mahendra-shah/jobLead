"""
ML Processor Service
Processes unprocessed messages from MongoDB, classifies them, and stores jobs to PostgreSQL
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional
from pymongo import MongoClient
from sqlalchemy.orm import Session

from app.config import Settings
from app.db.session import get_sync_db
from app.db.base import Base  # Import Base to initialize all models
# Import all models at once to ensure proper relationship initialization
from app.models import Job, Company, Channel, Application, Student, User
from app.ml.sklearn_classifier import SklearnClassifier
from app.ml.spacy_extractor import SpacyExtractor
from app.ml.enhanced_extractor import get_enhanced_extractor

# Load settings
settings = Settings()

logger = logging.getLogger(__name__)


class MLProcessorService:
    """
    Service to process Telegram messages using ML classification
    """
    
    def __init__(self):
        """Initialize ML processor with classifier and MongoDB connection"""
        self.classifier = SklearnClassifier()
        self.extractor = SpacyExtractor()
        self.enhanced_extractor = get_enhanced_extractor()  # NEW: Enhanced extraction
        
        # MongoDB connection
        self.mongo_client = MongoClient(settings.MONGODB_URI)
        self.mongo_db = self.mongo_client[settings.MONGODB_DB_NAME]
        self.messages_collection = self.mongo_db["raw_messages"]  # Changed from telegram_messages
        
        logger.info("✅ ML Processor Service initialized")
        logger.info(f"   Classifier loaded: {self.classifier.is_loaded}")
        logger.info(f"   Classifier version: {self.classifier.model_version}")
        logger.info(f"   spaCy extractor: {self.extractor.is_loaded}")
        logger.info("   Enhanced extractor: True")
    
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
                        
                        if result['stored_to_postgres']:
                            stats['stored_to_postgres'] += result['jobs_created']
                        if result['low_confidence']:
                            stats['low_confidence'] += 1
                    else:
                        stats['non_jobs'] += 1
                    
                    # Mark as processed in MongoDB
                    self.messages_collection.update_one(
                        {"_id": message["_id"]},
                        {
                            "$set": {
                                "is_processed": True,  # Changed from processed to is_processed
                                "processed_at": datetime.utcnow(),
                                "ml_classification": {
                                    "is_job": result['is_job'],
                                    "confidence": result['confidence'],
                                    "reason": result['reason']
                                }
                            }
                        }
                    )
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message {message.get('message_id')}: {e}")
                    stats['errors'] += 1
                    
                    # Still mark as processed to avoid reprocessing
                    self.messages_collection.update_one(
                        {"_id": message["_id"]},
                        {
                            "$set": {
                                "is_processed": True,  # Changed from processed to is_processed
                                "processed_at": datetime.utcnow(),
                                "processing_error": str(e)
                            }
                        }
                    )
            
            db.commit()
            
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
            "job_ids": []
        }
        
        # 2. If it's a job, extract details and store
        if classification.is_job:
            
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
                    logger.info(f"      Location: {extraction.location or 'Not found'}")
                    logger.info(f"      Salary: {extraction.salary_raw or 'Not found'}")
                    logger.info(f"      Experience: {extraction.experience_raw or 'Not found'}")
                    logger.info(f"      Skills: {', '.join(extraction.skills[:5]) if extraction.skills else 'None'}")
                    logger.info(f"      Apply Link: {extraction.apply_link or 'Not found'}")
                    logger.info(f"      Confidence: {extraction.confidence:.2f}")
                    
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
                    
                    # Create Job entry with all enhanced fields
                    job = Job(
                        title=extraction.job_title or extraction.company_name or "Job Opening",
                        company_id=extraction.company_id,
                        description=extraction.description[:5000] if extraction.description else text[:5000],
                        location=extraction.location,
                        
                        # Job details with enhanced extraction
                        skills_required=extraction.skills if extraction.skills else [],
                        experience_required=extraction.experience_raw,
                        salary_range={
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
                        
                        # ML metadata
                        ml_confidence=str(round(extraction.confidence, 2)),
                        
                        # Status
                        is_active=classification.confidence >= min_confidence,
                        is_verified=False,
                        
                        # Stats
                        view_count=0,
                        application_count=0
                    )
                    
                    db.add(job)
                    db.flush()  # Get the ID
                    
                    result['jobs_created'] += 1
                    result['job_ids'].append(str(job.id))
                    logger.info(f"      ✅ Stored to PostgreSQL (Job ID: {job.id})")
                    
                except Exception as e:
                    logger.error(f"      ❌ Error storing job {idx} to PostgreSQL: {e}")
                    # Continue processing other jobs in the message
                    continue
            
            # Mark as stored if at least one job was created
            if result['jobs_created'] > 0:
                result['stored_to_postgres'] = True
        
        return result
    
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
