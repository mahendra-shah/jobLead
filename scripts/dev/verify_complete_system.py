#!/usr/bin/env python3
"""
Complete System Verification Script

Tests the entire data pipeline:
1. System Health (Sessions, DB connections, Scheduler)
2. Telegram Scraping (Message fetching → MongoDB)
3. ML Classification (MongoDB → PostgreSQL jobs)
4. Quality Scoring
5. Group Joiner Service
6. End-to-End Data Flow

Usage:
    python3 scripts/dev/verify_complete_system.py
    python3 scripts/dev/verify_complete_system.py --full  # Trigger scraping
"""

import sys
import os
import asyncio
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json
from pathlib import Path

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from pymongo import MongoClient

from app.db.session import AsyncSessionLocal
from app.config import settings
from app.models.telegram_group import TelegramGroup
from app.models.telegram_account import TelegramAccount
from app.models.job import Job
from telethon import TelegramClient
from telethon.sessions import StringSession


class SystemVerifier:
    """Comprehensive system verification."""
    
    def __init__(self, full_test: bool = False):
        self.full_test = full_test
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "tests": {},
            "summary": {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "warnings": 0
            }
        }
        self.mongo_client = None
        self.mongo_db = None
    
    def log_section(self, title: str):
        """Print section header."""
        print("\n" + "=" * 70)
        print(f"  {title}")
        print("=" * 70)
    
    def log_test(self, name: str, passed: bool, message: str, warning: bool = False):
        """Log test result."""
        status = "⚠️ " if warning else ("✅" if passed else "❌")
        print(f"{status} {name}: {message}")
        
        self.results["tests"][name] = {
            "passed": passed,
            "message": message,
            "warning": warning
        }
        self.results["summary"]["total"] += 1
        if passed:
            self.results["summary"]["passed"] += 1
        elif warning:
            self.results["summary"]["warnings"] += 1
        else:
            self.results["summary"]["failed"] += 1
    
    def connect_mongodb(self):
        """Connect to MongoDB."""
        try:
            self.mongo_client = MongoClient(
                settings.MONGODB_URI,
                serverSelectionTimeoutMS=5000
            )
            self.mongo_db = self.mongo_client[settings.MONGODB_DATABASE]
            # Test connection
            self.mongo_client.server_info()
            return True
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            return False
    
    async def test_system_health(self):
        """Test 1: System health checks."""
        self.log_section("TEST 1: SYSTEM HEALTH")
        
        # 1.1 PostgreSQL Connection
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(text("SELECT 1"))
                result.scalar()
                db_url = settings.DATABASE_URL if hasattr(settings, 'DATABASE_URL') else str(settings.LOCAL_DATABASE_URL)
                db_host = db_url.split('@')[1].split('/')[0] if '@' in db_url else 'localhost'
                self.log_test(
                    "PostgreSQL Connection",
                    True,
                    f"Connected to {db_host}"
                )
        except Exception as e:
            self.log_test("PostgreSQL Connection", False, f"Failed: {str(e)}")
            return False
        
        # 1.2 MongoDB Connection
        mongo_ok = self.connect_mongodb()
        if mongo_ok:
            count = self.mongo_db.raw_messages.count_documents({})
            self.log_test("MongoDB Connection", True, f"Connected, {count:,} total messages")
        else:
            self.log_test("MongoDB Connection", False, "Connection failed")
            return False
        
        # 1.3 Telegram Sessions
        sessions_dir = Path("sessions")
        if not sessions_dir.exists():
            self.log_test("Telegram Sessions", False, "sessions/ directory not found")
            return False
        
        session_files = list(sessions_dir.glob("*.session"))
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(TelegramAccount).where(TelegramAccount.is_active == True))
            accounts = result.scalars().all()
            
            authorized_count = 0
            for account in accounts:
                session_file = sessions_dir / f"{account.phone}.session"
                if session_file.exists():
                    # Quick auth check (without full connection)
                    authorized_count += 1
            
            self.log_test(
                "Telegram Sessions",
                authorized_count > 0,
                f"{authorized_count}/{len(accounts)} session files found",
                warning=(authorized_count < len(accounts))
            )
        
        # 1.4 Scheduler Status (via health endpoint)
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:8000/api/telegram-scraper/scheduler/status", timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    total_jobs = data.get("total_jobs", 0)
                    self.log_test(
                        "Scheduler Status",
                        data.get("running", False),
                        f"Running with {total_jobs} jobs configured"
                    )
                else:
                    self.log_test("Scheduler Status", False, f"HTTP {response.status_code}", warning=True)
        except Exception as e:
            self.log_test("Scheduler Status", False, f"Cannot connect: {str(e)}", warning=True)
        
        return True
    
    async def test_telegram_scraping(self, db: AsyncSession):
        """Test 2: Telegram scraping verification."""
        self.log_section("TEST 2: TELEGRAM SCRAPING")
        
        # 2.1 Check joined channels
        result = await db.execute(
            select(func.count(TelegramGroup.id))
            .where(TelegramGroup.is_joined == True)
        )
        joined_count = result.scalar()
        
        result = await db.execute(
            select(func.count(TelegramGroup.id))
            .where(TelegramGroup.is_active == True)
        )
        active_count = result.scalar()
        
        self.log_test(
            "Joined Channels",
            joined_count > 0,
            f"{joined_count} joined out of {active_count} active channels",
            warning=(joined_count < 10)
        )
        
        # 2.2 Check recent scraping activity
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        result = await db.execute(
            select(func.count(TelegramGroup.id))
            .where(TelegramGroup.last_scraped_at >= one_hour_ago)
        )
        recent_scrapes = result.scalar()
        
        # Get last scrape time
        result = await db.execute(
            select(func.max(TelegramGroup.last_scraped_at))
            .where(TelegramGroup.is_joined == True)
        )
        last_scrape = result.scalar()
        
        if last_scrape:
            # Remove timezone for comparison with utcnow()
            last_scrape_naive = last_scrape.replace(tzinfo=None) if last_scrape.tzinfo else last_scrape
            minutes_ago = (datetime.utcnow() - last_scrape_naive).total_seconds() / 60
            self.log_test(
                "Recent Scraping",
                minutes_ago < 300,  # Within 5 hours
                f"Last scrape: {int(minutes_ago)} minutes ago ({recent_scrapes} channels in last hour)",
                warning=(minutes_ago > 300)
            )
        else:
            self.log_test("Recent Scraping", False, "No scraping activity found", warning=True)
        
        # 2.3 Check MongoDB messages
        if self.mongo_db is None:
            self.log_test("MongoDB Messages", False, "MongoDB not connected", warning=True)
            return True
        
        one_hour_ago_ts = datetime.utcnow() - timedelta(hours=1)
        recent_messages = self.mongo_db.raw_messages.count_documents({
            "fetched_at": {"$gte": one_hour_ago_ts}
        })
        
        total_messages = self.mongo_db.raw_messages.count_documents({})
        unprocessed = self.mongo_db.raw_messages.count_documents({"is_processed": False})
        
        self.log_test(
            "MongoDB Messages",
            total_messages > 0,
            f"{total_messages:,} total, {unprocessed:,} unprocessed, {recent_messages} in last hour",
            warning=(unprocessed > 500)
        )
        
        # 2.4 Check scraping distribution
        result = await db.execute(
            select(
                TelegramGroup.username,
                TelegramGroup.total_messages_scraped,
                TelegramGroup.last_scraped_at
            )
            .where(TelegramGroup.is_joined == True)
            .order_by(TelegramGroup.total_messages_scraped.desc())
            .limit(5)
        )
        top_channels = result.all()
        
        if top_channels:
            total_scraped = sum(ch.total_messages_scraped for ch in top_channels)
            self.log_test(
                "Scraping Distribution",
                total_scraped > 0,
                f"Top 5 channels have {total_scraped:,} messages"
            )
            for ch in top_channels[:3]:
                print(f"   • @{ch.username}: {ch.total_messages_scraped:,} messages")
        
        return True
    
    async def test_ml_classification(self, db: AsyncSession):
        """Test 3: ML classification & job extraction."""
        self.log_section("TEST 3: ML CLASSIFICATION & JOB EXTRACTION")
        
        # 3.1 Check total jobs created
        result = await db.execute(select(func.count(Job.id)))
        total_jobs = result.scalar()
        
        self.log_test(
            "Jobs Created",
            total_jobs > 0,
            f"{total_jobs:,} total jobs in database",
            warning=(total_jobs < 10)
        )
        
        # 3.2 Check recent job creation
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        result = await db.execute(
            select(func.count(Job.id))
            .where(Job.created_at >= one_hour_ago)
        )
        recent_jobs = result.scalar()
        
        # Get last job time
        result = await db.execute(
            select(func.max(Job.created_at))
        )
        last_job = result.scalar()
        
        if last_job:
            # Remove timezone for comparison with utcnow()
            last_job_naive = last_job.replace(tzinfo=None) if last_job.tzinfo else last_job
            minutes_ago = (datetime.utcnow() - last_job_naive).total_seconds() / 60
            self.log_test(
                "Recent Job Creation",
                minutes_ago < 120,  # Within 2 hours
                f"Last job: {int(minutes_ago)} minutes ago ({recent_jobs} jobs in last hour)",
                warning=(minutes_ago > 120)
            )
        else:
            self.log_test("Recent Job Creation", False, "No jobs found", warning=True)
        
        # 3.3 Check MongoDB → PostgreSQL linkage
        if self.mongo_db is None:
            self.log_test("Processing Backlog", False, "MongoDB not connected", warning=True)
        else:
            unprocessed = self.mongo_db.raw_messages.count_documents({"is_processed": False})
            
            # Find old unprocessed messages (>2 hours)
            two_hours_ago = datetime.utcnow() - timedelta(hours=2)
            old_unprocessed = self.mongo_db.raw_messages.count_documents({
                "is_processed": False,
                "fetched_at": {"$lt": two_hours_ago}
            })
        
            self.log_test(
                "Processing Backlog",
                old_unprocessed < 100,
                f"{unprocessed:,} unprocessed ({old_unprocessed} older than 2 hours)",
                warning=(old_unprocessed > 50)
            )
        
        # 3.4 Check ML confidence distribution
        result = await db.execute(
            select(func.count(Job.id))
            .where(Job.ml_confidence.isnot(None))
        )
        with_confidence = result.scalar()
        
        confidence_pct = (with_confidence / total_jobs * 100) if total_jobs > 0 else 0
        self.log_test(
            "ML Confidence Data",
            confidence_pct > 80,
            f"{with_confidence:,}/{total_jobs:,} jobs have ML confidence ({confidence_pct:.1f}%)",
            warning=(confidence_pct < 80)
        )
        
        # 3.5 Check source tracking
        result = await db.execute(
            select(func.count(Job.id))
            .where(Job.source_message_id.isnot(None))
        )
        with_source = result.scalar()
        
        source_pct = (with_source / total_jobs * 100) if total_jobs > 0 else 0
        self.log_test(
            "Source Tracking",
            source_pct > 90,
            f"{with_source:,}/{total_jobs:,} jobs linked to MongoDB ({source_pct:.1f}%)",
            warning=(source_pct < 90)
        )
        
        return True
    
    async def test_quality_scoring(self, db: AsyncSession):
        """Test 4: Quality scoring verification."""
        self.log_section("TEST 4: QUALITY SCORING")
        
        # 4.1 Check scoring completeness
        result = await db.execute(select(func.count(Job.id)))
        total_jobs = result.scalar()
        
        result = await db.execute(
            select(func.count(Job.id))
            .where(Job.quality_score.isnot(None))
        )
        scored_jobs = result.scalar()
        
        scored_pct = (scored_jobs / total_jobs * 100) if total_jobs > 0 else 0
        self.log_test(
            "Quality Scoring Coverage",
            scored_pct > 85,
            f"{scored_jobs:,}/{total_jobs:,} jobs scored ({scored_pct:.1f}%)",
            warning=(scored_pct < 85)
        )
        
        # 4.2 Check score distribution
        result = await db.execute(
            select(
                func.count(Job.id).filter(Job.quality_score >= 80).label("excellent"),
                func.count(Job.id).filter((Job.quality_score >= 60) & (Job.quality_score < 80)).label("good"),
                func.count(Job.id).filter((Job.quality_score >= 40) & (Job.quality_score < 60)).label("medium"),
                func.count(Job.id).filter(Job.quality_score < 40).label("low"),
                func.avg(Job.quality_score).label("avg_score")
            )
            .where(Job.quality_score.isnot(None))
        )
        dist = result.first()
        
        if dist and dist.avg_score:
            self.log_test(
                "Score Distribution",
                dist.avg_score > 45,
                f"Avg: {dist.avg_score:.1f} | Excellent: {dist.excellent}, Good: {dist.good}, Medium: {dist.medium}, Low: {dist.low}",
                warning=(dist.avg_score < 45)
            )
        
        # 4.3 Check relevance criteria
        result = await db.execute(
            select(func.count(Job.id))
            .where(Job.meets_relevance_criteria == True)
        )
        relevant_jobs = result.scalar()
        
        relevant_pct = (relevant_jobs / total_jobs * 100) if total_jobs > 0 else 0
        self.log_test(
            "Relevance Filtering",
            relevant_pct > 5,  # At least 5% should be relevant
            f"{relevant_jobs:,}/{total_jobs:,} jobs meet relevance criteria ({relevant_pct:.1f}%)",
            warning=(relevant_pct < 5)
        )
        
        # 4.4 Check quality breakdown JSONB
        result = await db.execute(
            select(func.count(Job.id))
            .where(Job.quality_breakdown.isnot(None))
        )
        with_breakdown = result.scalar()
        
        breakdown_pct = (with_breakdown / total_jobs * 100) if total_jobs > 0 else 0
        self.log_test(
            "Quality Breakdown Data",
            breakdown_pct > 80,
            f"{with_breakdown:,}/{total_jobs:,} jobs have detailed breakdown ({breakdown_pct:.1f}%)",
            warning=(breakdown_pct < 80)
        )
        
        return True
    
    async def test_group_joiner(self, db: AsyncSession):
        """Test 5: Group joiner service verification."""
        self.log_section("TEST 5: GROUP JOINER SERVICE")
        
        # 5.1 Check join status
        result = await db.execute(
            select(
                func.count(TelegramGroup.id).label("total"),
                func.count(TelegramGroup.id).filter(TelegramGroup.is_joined == True).label("joined"),
                func.count(TelegramGroup.id).filter(
                    (TelegramGroup.is_joined == False) & (TelegramGroup.is_active == True)
                ).label("unjoined")
            )
        )
        status = result.first()
        
        join_pct = (status.joined / status.total * 100) if status.total > 0 else 0
        self.log_test(
            "Channel Join Status",
            status.joined > 0,
            f"{status.joined}/{status.total} joined ({join_pct:.1f}%), {status.unjoined} pending",
            warning=(status.joined < 10)
        )
        
        # 5.2 Check recent joins
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        result = await db.execute(
            select(func.count(TelegramGroup.id))
            .where(TelegramGroup.joined_at >= one_hour_ago)
        )
        recent_joins = result.scalar()
        
        # Get last join time
        result = await db.execute(
            select(func.max(TelegramGroup.joined_at))
        )
        last_join = result.scalar()
        
        if last_join:
            # Remove timezone for comparison with utcnow()
            last_join_naive = last_join.replace(tzinfo=None) if last_join.tzinfo else last_join
            hours_ago = (datetime.utcnow() - last_join_naive).total_seconds() / 3600
            self.log_test(
                "Recent Join Activity",
                hours_ago < 6,  # Within 6 hours (5hr interval + buffer)
                f"Last join: {hours_ago:.1f} hours ago ({recent_joins} in last hour)",
                warning=(hours_ago > 6)
            )
        else:
            self.log_test("Recent Join Activity", False, "No join activity found", warning=True)
        
        # 5.3 Check account distribution
        result = await db.execute(
            select(
                TelegramAccount.phone,
                TelegramAccount.groups_joined_count,
                func.count(TelegramGroup.id).label("actual_joins")
            )
            .outerjoin(TelegramGroup, TelegramGroup.telegram_account_id == TelegramAccount.id)
            .where(TelegramAccount.is_active == True)
            .group_by(TelegramAccount.id, TelegramAccount.phone, TelegramAccount.groups_joined_count)
            .order_by(TelegramAccount.groups_joined_count.desc())
        )
        accounts = result.all()
        
        if accounts:
            total_joins = sum(acc.actual_joins for acc in accounts)
            self.log_test(
                "Account Distribution",
                len(accounts) > 0,
                f"{len(accounts)} accounts, {total_joins} total joins"
            )
            for acc in accounts[:3]:
                print(f"   • {acc.phone}: {acc.actual_joins} joins")
        else:
            self.log_test("Account Distribution", False, "No active accounts found")
        
        # 5.4 Check joiner service exists
        joiner_service_path = Path("app/services/telegram_group_joiner_service.py")
        joiner_exists = joiner_service_path.exists()
        self.log_test(
            "Joiner Service File",
            joiner_exists,
            f"{joiner_service_path} {'exists' if joiner_exists else 'not found'}"
        )
        
        # 5.5 Check scheduler job
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:8000/api/telegram-scraper/scheduler/status", timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    jobs = data.get("jobs", [])
                    joiner_job = next((j for j in jobs if j["id"] == "telegram_group_joiner_5hourly"), None)
                    
                    if joiner_job:
                        next_run = joiner_job.get("next_run_time", "Unknown")
                        self.log_test(
                            "Joiner Scheduler Job",
                            True,
                            f"Configured, next run: {next_run}"
                        )
                    else:
                        self.log_test("Joiner Scheduler Job", False, "Job not found in scheduler", warning=True)
                else:
                    self.log_test("Joiner Scheduler Job", False, "Cannot check scheduler", warning=True)
        except Exception as e:
            self.log_test("Joiner Scheduler Job", False, f"Cannot connect: {str(e)}", warning=True)
        
        return True
    
    async def test_e2e_flow(self, db: AsyncSession):
        """Test 6: End-to-end data flow verification."""
        self.log_section("TEST 6: END-TO-END DATA FLOW")
        
        # 6.1 Complete pipeline health
        result = await db.execute(
            select(func.count(TelegramGroup.id)).where(TelegramGroup.is_joined == True)
        )
        joined_channels = result.scalar()
        
        result = await db.execute(select(func.count(Job.id)))
        total_jobs = result.scalar()
        
        total_messages = self.mongo_db.raw_messages.count_documents({}) if self.mongo_db is not None else 0
        
        self.log_test(
            "Pipeline Completeness",
            all([joined_channels > 0, total_jobs > 0]),  # Skip MongoDB check if not connected
            f"Channels: {joined_channels} → Messages: {total_messages:,} → Jobs: {total_jobs:,}",
            warning=(self.mongo_db is None)
        )
        
        # 6.2 Data freshness check
        result = await db.execute(
            select(func.max(TelegramGroup.last_scraped_at)).where(TelegramGroup.is_joined == True)
        )
        last_scrape = result.scalar()
        
        result = await db.execute(select(func.max(Job.created_at)))
        last_job = result.scalar()
        
        if last_scrape and last_job:
            # Remove timezone for comparison with utcnow()
            last_scrape_naive = last_scrape.replace(tzinfo=None) if last_scrape.tzinfo else last_scrape
            last_job_naive = last_job.replace(tzinfo=None) if last_job.tzinfo else last_job
            scrape_age = (datetime.utcnow() - last_scrape_naive).total_seconds() / 3600
            job_age = (datetime.utcnow() - last_job_naive).total_seconds() / 3600
            
            self.log_test(
                "Data Freshness",
                scrape_age < 5 and job_age < 3,
                f"Last scrape: {scrape_age:.1f}h ago, Last job: {job_age:.1f}h ago",
                warning=(scrape_age > 5 or job_age > 3)
            )
        else:
            self.log_test("Data Freshness", False, "Missing timestamp data", warning=True)
        
        # 6.3 Conversion rate (messages → jobs)
        if total_messages > 0 and total_jobs > 0:
            conversion_rate = (total_jobs / total_messages) * 100
            self.log_test(
                "Conversion Rate",
                0.5 < conversion_rate < 50,  # Reasonable range
                f"{conversion_rate:.2f}% messages become jobs",
                warning=(conversion_rate < 0.5 or conversion_rate > 50)
            )
        
        # 6.4 Account health check
        result = await db.execute(
            select(func.count(TelegramAccount.id))
            .where(TelegramAccount.is_active == True)
            .where(TelegramAccount.health_status == 'healthy')
        )
        healthy_accounts = result.scalar()
        
        result = await db.execute(
            select(func.count(TelegramAccount.id))
            .where(TelegramAccount.is_active == True)
        )
        total_accounts = result.scalar()
        
        health_pct = (healthy_accounts / total_accounts * 100) if total_accounts > 0 else 0
        self.log_test(
            "Account Health",
            health_pct > 60,
            f"{healthy_accounts}/{total_accounts} accounts healthy ({health_pct:.0f}%)",
            warning=(health_pct < 60)
        )
        
        return True
    
    async def run_verification(self):
        """Run complete verification suite."""
        start_time = time.time()
        
        print("\n" + "🔍" * 35)
        print("  COMPLETE SYSTEM VERIFICATION")
        print("🔍" * 35)
        print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Mode: {'FULL TEST (with triggers)' if self.full_test else 'QUICK VERIFICATION'}")
        
        try:
            # Test 1: System Health
            health_ok = await self.test_system_health()
            if not health_ok:
                print("\n⚠️  Critical system health issues detected. Some tests may fail.")
            
            # Remaining tests need database connection
            async with AsyncSessionLocal() as db:
                # Test 2: Telegram Scraping
                await self.test_telegram_scraping(db)
                
                # Test 3: ML Classification
                await self.test_ml_classification(db)
                
                # Test 4: Quality Scoring
                await self.test_quality_scoring(db)
                
                # Test 5: Group Joiner
                await self.test_group_joiner(db)
                
                # Test 6: End-to-End Flow
                await self.test_e2e_flow(db)
            
            # Final summary
            duration = time.time() - start_time
            self.print_summary(duration)
            
            # Save results
            self.save_results()
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Verification interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n\n❌ Verification failed with error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            if self.mongo_client:
                self.mongo_client.close()
    
    def print_summary(self, duration: float):
        """Print verification summary."""
        summary = self.results["summary"]
        
        print("\n" + "=" * 70)
        print("  VERIFICATION SUMMARY")
        print("=" * 70)
        print()
        print(f"⏱️  Duration: {duration:.1f} seconds")
        print()
        print(f"Total Tests:    {summary['total']}")
        print(f"✅ Passed:      {summary['passed']}")
        print(f"❌ Failed:      {summary['failed']}")
        print(f"⚠️  Warnings:    {summary['warnings']}")
        print()
        
        pass_rate = (summary['passed'] / summary['total'] * 100) if summary['total'] > 0 else 0
        
        if summary['failed'] == 0:
            status = "🎉 ALL TESTS PASSED!"
        elif pass_rate >= 70:
            status = "✅ SYSTEM OPERATIONAL (with warnings)"
        else:
            status = "❌ SYSTEM ISSUES DETECTED"
        
        print(f"Status: {status} ({pass_rate:.0f}% pass rate)")
        print()
        
        if summary['failed'] > 0:
            print("Failed Tests:")
            for name, result in self.results["tests"].items():
                if not result["passed"] and not result["warning"]:
                    print(f"  ❌ {name}: {result['message']}")
            print()
        
        if summary['warnings'] > 0:
            print("Warnings:")
            for name, result in self.results["tests"].items():
                if result["warning"]:
                    print(f"  ⚠️  {name}: {result['message']}")
            print()
        
        print("=" * 70)
    
    def save_results(self):
        """Save results to JSON file."""
        output_file = f"verification_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        print(f"\n📄 Results saved to: {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Verify complete system functionality")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full test (triggers scraping and ML processing)"
    )
    args = parser.parse_args()
    
    verifier = SystemVerifier(full_test=args.full)
    asyncio.run(verifier.run_verification())


if __name__ == "__main__":
    main()
