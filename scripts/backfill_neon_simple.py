"""
Simple backfill script for Neon database using raw SQL
Works around ORM/model differences
"""
import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal
from app.ml.enhanced_extractor import EnhancedJobExtractor


async def backfill_jobs_simple():
    """Backfill using raw SQL queries"""
    print("🔄 Starting Neon Database Backfill (Simple Mode)")
    print("=" * 50)
    
    extractor = EnhancedJobExtractor()
    
    async with AsyncSessionLocal() as db:
        # Get jobs that need processing
        result = await db.execute(text("""
            SELECT id, title, description, company_id
            FROM jobs 
            WHERE is_fresher IS NULL
            ORDER BY created_at DESC
        """))
        jobs = result.fetchall()
        
        total = len(jobs)
        print(f"\n📊 Found {total} jobs to process")
        
        if total == 0:
            print("✅ All jobs already processed!")
            return
        
        processed = 0
        errors = 0
        
        for i, (job_id, title, description, company_id) in enumerate(jobs, 1):
            try:
                print(f"\n[{i}/{total}] Processing: {title[:60]}")
                
                # Extract structured data from the job
                job_text = f"{title}\n{description or ''}"
                jobs_extracted = extractor.extract_jobs_from_message(job_text)
                
                if not jobs_extracted:
                    print(f"  ⚠️  No data extracted")
                    continue
                
                # Use first extracted job
                job_data = jobs_extracted[0]
                
                # Determine if it's a fresher job
                is_fresher_job = job_data.is_fresher_friendly or (
                    job_data.experience_min is not None and job_data.experience_min == 0
                )
                
                # Get work type from description
                job_text = f"{title}\n{description or ''}"
                work_type = 'remote' if any(word in job_text.lower() for word in ['remote', 'work from home', 'wfh']) else 'on-site'
                if any(word in job_text.lower() for word in ['hybrid']):
                    work_type = 'hybrid'
                
                # Update job with extracted data
                await db.execute(text("""
                    UPDATE jobs 
                    SET is_fresher = :is_fresher,
                        work_type = :work_type,
                        experience_min = :experience_min,
                        experience_max = :experience_max,
                        salary_min = :salary_min,
                        salary_max = :salary_max
                    WHERE id = :job_id
                """), {
                    'job_id': job_id,
                    'is_fresher': is_fresher_job,
                    'work_type': work_type,
                    'experience_min': job_data.experience_min,
                    'experience_max': job_data.experience_max,
                    'salary_min': job_data.salary_min,
                    'salary_max': job_data.salary_max,
                })
                
                processed += 1
                
                # Print extracted info
                exp_info = "Fresher" if is_fresher_job else f"{job_data.experience_min}-{job_data.experience_max}Y"
                salary_min = job_data.salary_min
                salary_max = job_data.salary_max
                salary_info = f"₹{salary_min/100000:.1f}-{salary_max/100000:.1f}L" if salary_min else "Not specified"
                print(f"  ✅ Experience: {exp_info}, Work: {work_type}, Salary: {salary_info}")
                
                # Commit every 10 jobs
                if processed % 10 == 0:
                    await db.commit()
                    print(f"\n💾 Saved progress: {processed}/{total} jobs processed")
                
            except Exception as e:
                errors += 1
                print(f"  ❌ Error: {str(e)[:100]}")
                continue
        
        # Final commit
        await db.commit()
        
        print("\n" + "=" * 50)
        print("✅ Backfill Complete!")
        print(f"📊 Total jobs: {total}")
        print(f"✅ Successfully processed: {processed}")
        print(f"❌ Errors: {errors}")
        print("=" * 50)
        
        # Show summary statistics
        print("\n📈 Summary Statistics:")
        
        # Fresher jobs
        result = await db.execute(text("SELECT COUNT(*) FROM jobs WHERE is_fresher = true"))
        fresher_count = result.scalar()
        print(f"  👨‍🎓 Fresher jobs: {fresher_count}")
        
        # Jobs with experience
        result = await db.execute(text("SELECT COUNT(*) FROM jobs WHERE experience_min IS NOT NULL"))
        exp_count = result.scalar()
        print(f"  💼 Jobs with experience data: {exp_count}")
        
        # Jobs with salary
        result = await db.execute(text("SELECT COUNT(*) FROM jobs WHERE salary_min IS NOT NULL"))
        salary_count = result.scalar()
        print(f"  💰 Jobs with salary data: {salary_count}")
        
        # Work type breakdown
        result = await db.execute(text("""
            SELECT work_type, COUNT(*) 
            FROM jobs 
            WHERE work_type IS NOT NULL 
            GROUP BY work_type
        """))
        work_types = result.fetchall()
        print("\n  📊 Work Type Breakdown:")
        for work_type, count in work_types:
            print(f"    {work_type}: {count}")


if __name__ == "__main__":
    asyncio.run(backfill_jobs_simple())
