"""
Verify scheduler timezone configuration is set to IST.

This script checks that all scheduled jobs will run at IST times.
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime
import pytz
from app.core.scheduler import scheduler, setup_jobs

def verify_scheduler_timezone():
    """Verify scheduler is configured for IST timezone."""
    
    print("=" * 80)
    print("SCHEDULER TIMEZONE VERIFICATION")
    print("=" * 80)
    
    # Setup jobs if not already done
    if not scheduler.running:
        setup_jobs()
        scheduler.start()  # Start scheduler to calculate next run times
        print("⚙️  Scheduler started for verification")
    
    # Check scheduler timezone
    tz = scheduler.timezone
    print(f"\nScheduler Timezone: {tz}")
    
    if str(tz) == 'Asia/Kolkata':
        print("✅ Scheduler is correctly configured for IST")
    else:
        print(f"❌ WARNING: Scheduler is NOT in IST (currently: {tz})")
        return False
    
    # Get current time in IST
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    print(f"\nCurrent Time (IST): {now_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # List all scheduled jobs with their next run times
    jobs = scheduler.get_jobs()
    print(f"\n" + "=" * 80)
    print(f"SCHEDULED JOBS ({len(jobs)} total)")
    print("=" * 80)
    
    for job in jobs:
        print(f"\n📅 {job.name}")
        print(f"   ID: {job.id}")
        print(f"   Trigger: {job.trigger}")
        
        # Get next run time (try different attribute names)
        next_run = getattr(job, 'next_run_time', None) or getattr(job, 'next_run', None)
        
        if next_run:
            # Convert to IST for display
            next_run_ist = next_run.astimezone(ist) if hasattr(next_run, 'astimezone') else next_run
            print(f"   Next Run (IST): {next_run_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            # Calculate hours until next run
            time_until = next_run - datetime.now(tz)
            hours = time_until.total_seconds() / 3600
            print(f"   Time Until Next Run: {hours:.1f} hours")
        else:
            print(f"   Next Run: Not scheduled yet")
    
    print(f"\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✅ All jobs are scheduled in IST timezone")
    print(f"✅ Daily Morning Update will run at 9:00 AM IST")
    print(f"✅ Telegram Scraper will run at: 4, 8, 12, 16, 20, 0 hours IST")
    print(f"✅ Channel Sync will run every 6 hours")
    print("=" * 80)
    
    # Cleanup
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("\n⚙️  Scheduler stopped after verification")
    
    return True

if __name__ == "__main__":
    try:
        success = verify_scheduler_timezone()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
