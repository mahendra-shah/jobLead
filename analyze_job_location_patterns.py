"""
Job Location Pattern Analysis Script

Analyzes MongoDB raw messages and PostgreSQL jobs to identify:
1. International job patterns (non-India locations)
2. Onsite-only job patterns (no remote/hybrid option)
3. Common keywords and phrases for filtering

Run: python analyze_job_location_patterns.py
"""

import asyncio
import re
import json
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List

from pymongo import MongoClient
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from app.config import settings
from app.models.job import Job
from app.db.session import sync_engine  # Use existing sync engine


class JobLocationAnalyzer:
    """Analyze job data for location and work mode patterns"""
    
    def __init__(self):
        # MongoDB connection
        self.mongo_client = MongoClient(settings.MONGODB_URI)
        self.mongo_db = self.mongo_client[settings.MONGODB_DATABASE]
        self.raw_messages = self.mongo_db['raw_messages']
        
        # PostgreSQL connection - use existing sync engine
        self.pg_engine = sync_engine
        self.Session = sessionmaker(bind=self.pg_engine)
        
        # International location keywords
        self.international_keywords = [
            'usa', 'united states', 'america', 'us only', 'california', 'new york', 
            'texas', 'silicon valley', 'uk', 'united kingdom', 'london', 'manchester',
            'england', 'canada', 'toronto', 'vancouver', 'australia', 'sydney',
            'melbourne', 'singapore', 'dubai', 'uae', 'abu dhabi', 'europe',
            'germany', 'berlin', 'france', 'paris', 'netherlands', 'amsterdam',
            'ireland', 'dublin', 'switzerland', 'sweden', 'norway', 'japan',
            'china', 'israel', 'overseas', 'international', 'gulf', 'abroad',
            'saudi', 'kuwait', 'qatar', 'bahrain'
        ]
        
        # Onsite-only indicators
        self.onsite_keywords = [
            'onsite only', 'on-site only', 'on site only', 'office only',
            'work from office', 'wfo mandatory', 'wfo only', 'office mandatory',
            'must work from office', 'no remote', 'no wfh', 'not remote',
            'in-office', 'in office', '100% office', 'fully office',
            'relocate', 'relocation required', 'must be based in',
            'local candidates only', 'relocation mandatory'
        ]
        
        # India location indicators
        self.india_keywords = [
            'india', 'indian', 'delhi', 'mumbai', 'bangalore', 'bengaluru',
            'hyderabad', 'chennai', 'kolkata', 'pune', 'ahmedabad', 'jaipur',
            'lucknow', 'kanpur', 'nagpur', 'indore', 'bhopal', 'noida', 'gurgaon',
            'gurugram', 'chandigarh', 'kochi', 'trivandrum', 'mysore', 
            'bhubaneswar', 'jamshedpur', 'ranchi'
        ]

    def analyze_mongodb_messages(self, limit: int = 200) -> Dict:
        """Analyze raw messages from MongoDB"""
        print("\n" + "="*80)
        print("🍃 ANALYZING MONGODB RAW MESSAGES")
        print("="*80)
        
        results = {
            'international_jobs': [],
            'onsite_only_jobs': [],
            'international_keywords_found': Counter(),
            'onsite_keywords_found': Counter(),
            'total_analyzed': 0
        }
        
        # Sample recent messages
        cursor = self.raw_messages.find(
            {'is_processed': True}
        ).sort('message_date', -1).limit(limit)
        
        for msg in cursor:
            text = msg.get('message_text', '').lower()
            if not text:
                continue
                
            results['total_analyzed'] += 1
            
            # Check for international patterns
            has_india = any(kw in text for kw in self.india_keywords)
            has_international = any(kw in text for kw in self.international_keywords)
            
            if has_international and not has_india:
                # Count which keywords
                for kw in self.international_keywords:
                    if kw in text:
                        results['international_keywords_found'][kw] += 1
                
                if len(results['international_jobs']) < 20:
                    results['international_jobs'].append({
                        'text': msg.get('message_text', '')[:500],
                        'channel': msg.get('channel_username', 'Unknown'),
                        'date': str(msg.get('message_date', ''))
                    })
            
# Check for onsite-only patterns
            for kw in self.onsite_keywords:
                if kw in text:
                    results['onsite_keywords_found'][kw] += 1
                    
                    if len(results['onsite_only_jobs']) < 20:
                        results['onsite_only_jobs'].append({
                            'text': msg.get('message_text', '')[:500],
                            'channel': msg.get('channel_username', 'Unknown'),
                            'date': str(msg.get('message_date', ''))
                        })
                    break
        
        return results

    def analyze_postgres_jobs(self) -> Dict:
        """Analyze processed jobs from PostgreSQL"""
        print("\n" + "="*80)
        print("🐘 ANALYZING POSTGRESQL JOBS")
        print("="*80)
        
        results = {
            'international_jobs': [],
            'onsite_only_jobs': [],
            'location_distribution': Counter(),
            'work_type_distribution': Counter(),
            'total_jobs': 0,
            'international_count': 0,
            'onsite_count': 0
        }
        
        with self.Session() as session:
            # Total jobs
            results['total_jobs'] = session.query(func.count(Job.id)).scalar()
            
            # Location distribution
            locations = session.query(
                Job.location,
                func.count(Job.id).label('count')
            ).filter(
                Job.location.isnot(None),
                Job.is_active == True
            ).group_by(Job.location).all()
            
            for loc, count in locations:
                if loc:
                    results['location_distribution'][loc] = count
            
            # Work type distribution
            work_types = session.query(
                Job.work_type,
                func.count(Job.id).label('count')
            ).filter(
                Job.work_type.isnot(None),
                Job.is_active == True
            ).group_by(Job.work_type).all()
            
            for wt, count in work_types:
                if wt:
                    results['work_type_distribution'][wt] = count
            
            # Sample jobs with raw_text
            jobs = session.query(Job).filter(
                Job.is_active == True,
                Job.raw_text.isnot(None)
            ).limit(500).all()
            
            for job in jobs:
                if not job.raw_text:
                    continue
                
                text_lower = job.raw_text.lower()
                location_lower = (job.location or '').lower()
                
                # Check international
                has_india = any(kw in location_lower for kw in self.india_keywords)
                has_international = any(kw in location_lower for kw in self.international_keywords)
                
                if has_international and not has_india:
                    results['international_count'] += 1
                    if len(results['international_jobs']) < 20:
                        results['international_jobs'].append({
                            'id': str(job.id),
                            'title': job.title,
                            'location': job.location,
                            'work_type': job.work_type,
                            'text': job.raw_text[:300]
                        })
                
                # Check onsite-only
                text_check = f"{text_lower} {location_lower}".lower()
                if any(kw in text_check for kw in self.onsite_keywords):
                    if job.work_type not in ['remote', 'hybrid']:
                        results['onsite_count'] += 1
                        if len(results['onsite_only_jobs']) < 20:
                            results['onsite_only_jobs'].append({
                                'id': str(job.id),
                                'title': job.title,
                                'location': job.location,
                                'work_type': job.work_type,
                                'text': job.raw_text[:300]
                            })
        
        return results

    def generate_report(self, mongo_results: Dict, postgres_results: Dict):
        """Generate comprehensive analysis report"""
        print("\n" + "="*80)
        print("📊 LOCATION PATTERN ANALYSIS REPORT")
        print("="*80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Summary
        print("\n### SUMMARY STATISTICS")
        print(f"Total PostgreSQL Jobs: {postgres_results['total_jobs']}")
        print(f"MongoDB Messages Analyzed: {mongo_results['total_analyzed']}")
        print(f"\nInternational Jobs Found:")
        print(f"  - MongoDB: {len(mongo_results['international_jobs'])}")
        print(f"  - PostgreSQL: {postgres_results['international_count']}")
        print(f"\nOnsite-Only Jobs Found:")
        print(f"  - MongoDB: {len(mongo_results['onsite_only_jobs'])}")
        print(f"  - PostgreSQL: {postgres_results['onsite_count']}")
        
        # Location distribution
        print("\n### TOP 20 LOCATIONS")
        for loc, count in postgres_results['location_distribution'].most_common(20):
            print(f"  {loc}: {count}")
        
        # Work type distribution
        print("\n### WORK TYPE DISTRIBUTION")
        for wt, count in postgres_results['work_type_distribution'].most_common():
            print(f"  {wt}: {count}")
        
        # International keywords
        print("\n### TOP INTERNATIONAL KEYWORDS (MongoDB)")
        for kw, count in mongo_results['international_keywords_found'].most_common(15):
            print(f"  '{kw}': {count} times")
        
        # Onsite keywords
        print("\n### TOP ONSITE-ONLY KEYWORDS (MongoDB)")
        for kw, count in mongo_results['onsite_keywords_found'].most_common(15):
            print(f"  '{kw}': {count} times")
        
        # Sample international jobs
        print("\n### SAMPLE INTERNATIONAL JOBS (First 10)")
        print("-" * 80)
        for i, job in enumerate(mongo_results['international_jobs'][:10], 1):
            print(f"\n{i}. Channel: {job['channel']} | Date: {job['date']}")
            print(f"   Text: {job['text'][:250]}...")
        
        # Sample onsite jobs
        print("\n### SAMPLE ONSITE-ONLY JOBS (First 10)")
        print("-" * 80)
        for i, job in enumerate(mongo_results['onsite_only_jobs'][:10], 1):
            print(f"\n{i}. Channel: {job['channel']} | Date: {job['date']}")
            print(f"   Text: {job['text'][:250]}...")
        
        # Impact analysis
        if postgres_results['total_jobs'] > 0:
            intl_pct = (postgres_results['international_count'] / postgres_results['total_jobs']) * 100
            onsite_pct = (postgres_results['onsite_count'] / postgres_results['total_jobs'] * 100)
            
            print(f"\n### FILTERING IMPACT ESTIMATE")
            print(f"International onsite jobs: ~{postgres_results['international_count']} ({intl_pct:.1f}%)")
            print(f"Strict onsite-only jobs: ~{postgres_results['onsite_count']} ({onsite_pct:.1f}%)")
        
        # Recommendations
        print("\n### RECOMMENDED FILTER KEYWORDS")
        print("\nInternational (to exclude when not remote):")
        intl_top = [kw for kw, _ in mongo_results['international_keywords_found'].most_common(20)]
        print(json.dumps(intl_top, indent=2))
        
        print("\nOnsite-only indicators (to flag or score low):")
        onsite_top = [kw for kw, _ in mongo_results['onsite_keywords_found'].most_common(15)]
        print(json.dumps(onsite_top, indent=2))

    def save_results(self, mongo_results: Dict, postgres_results: Dict):
        """Save detailed results to JSON file"""
        output = {
            'analysis_date': datetime.now().isoformat(),
            'mongodb_analysis': {
                'total_analyzed': mongo_results['total_analyzed'],
                'international_count': len(mongo_results['international_jobs']),
                'onsite_count': len(mongo_results['onsite_only_jobs']),
                'international_keywords': dict(mongo_results['international_keywords_found'].most_common()),
                'onsite_keywords': dict(mongo_results['onsite_keywords_found'].most_common()),
                'sample_international': mongo_results['international_jobs'][:10],
                'sample_onsite': mongo_results['onsite_only_jobs'][:10]
            },
            'postgres_analysis': {
                'total_jobs': postgres_results['total_jobs'],
                'international_count': postgres_results['international_count'],
                'onsite_count': postgres_results['onsite_count'],
                'location_distribution': dict(postgres_results['location_distribution'].most_common(30)),
                'work_type_distribution': dict(postgres_results['work_type_distribution']),
                'sample_international': postgres_results['international_jobs'][:10],
                'sample_onsite': postgres_results['onsite_only_jobs'][:10]
            }
        }
        
        filename = f"job_location_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        print(f"\n✅ Detailed results saved to: {filename}")
        return filename

    def run(self):
        """Run complete analysis"""
        try:
            print("\n🚀 Starting Job Location Pattern Analysis...")
            
            # Analyze MongoDB
            mongo_results = self.analyze_mongodb_messages(limit=200)
            
            # Analyze PostgreSQL
            postgres_results = self.analyze_postgres_jobs()
            
            # Generate report
            self.generate_report(mongo_results, postgres_results)
            
            # Save results
            filename = self.save_results(mongo_results, postgres_results)
            
            print("\n✅ Analysis Complete!")
            return filename
            
        except Exception as e:
            print(f"\n❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.mongo_client.close()
            # Don't dispose shared sync_engine


if __name__ == "__main__":
    analyzer = JobLocationAnalyzer()
    analyzer.run()
