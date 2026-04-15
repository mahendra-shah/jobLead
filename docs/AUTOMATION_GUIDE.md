# 🤖 Automation Pipeline - Complete Guide

## ✅ YES, THE SYSTEM IS FULLY AUTOMATED!

The placement dashboard **automatically** fetches Telegram messages, processes them with ML, and stores jobs in the database. Here's how everything works:

---

## 🔄 How the Automation Works

### 1. **Scheduler (APScheduler)**
- Runs **inside the FastAPI application**
- Started automatically when you run `uvicorn app.main:app`
- Manages all scheduled tasks

### 2. **Telegram Scraper Job**
- **Schedule:** Every 4 hours at specific times
- **UTC Times:** 4, 8, 12, 16, 20, 0 hours
- **IST Times:** 9:30 AM, 1:30 PM, 5:30 PM, 9:30 PM, 1:30 AM, 5:30 AM

### 3. **Complete Pipeline Flow**
```
⏰ Scheduled Time Arrives
    ↓
📥 Telegram Scraper Activates
    ↓
🔄 Fetches messages from ALL channels (using 4 accounts)
    ↓
💾 Stores raw messages in MongoDB (raw_messages collection)
    ↓
🤖 ML Pipeline Processes Each Message
    ↓
✅ Extracts job details (title, location, skills, etc.)
    ↓
🌍 Applies Location Intelligence
    • Detects: India vs International
    • Detects: Remote vs Hybrid vs Onsite
    • Scores: India Remote=100, International Onsite=0
    ↓
💼 Stores Jobs in PostgreSQL (jobs table)
    ↓
✅ Jobs Ready for Students!
```

### 4. **What Gets Automated**
| Task | Frequency | Status |
|------|-----------|--------|
| Telegram Message Fetching | Every 4 hours | ✅ Active |
| ML Job Extraction | Real-time after fetch | ✅ Active |
| Location Filtering | Part of extraction | ✅ Active |
| Quality Scoring | Part of extraction | ✅ Active |
| Database Storage | Automatic | ✅ Active |
| Daily Slack Update | 2:30 PM IST | ✅ Active |
| Channel Sync | Every 6 hours | ✅ Active |

---

## 🧪 Testing the Automation

I've created **3 scripts** to test and monitor the automation:

### Option 1: Quick Test (Trigger NOW)
**Fastest way to test - runs immediately:**
```bash
./test_automation.sh
# Choose option 1: Trigger scraper NOW
```

**What it does:**
1. Checks if server is running (starts if needed)
2. Triggers the Telegram scraper immediately via API
3. Waits 30 seconds
4. Shows results (messages fetched, jobs created, location filtering)

### Option 2: Schedule for 2:40 PM Today
**Test the automatic scheduling:**
```bash
./test_automation.sh
# Choose option 2: Schedule for 2:40 PM
```

**What it does:**
1. Schedules a one-time run at 2:40 PM
2. Keeps monitoring active
3. Automatically runs at 2:40 PM
4. Shows detailed results

### Option 3: Monitor Recent Activity
**Check what happened in last 30 minutes:**
```bash
python monitor_automation.py 1
```

**Shows:**
- Messages fetched from Telegram
- Jobs created in database
- Location filtering results
- ML confidence scores

### Option 4: Continuous Monitoring
**Watch for 60 minutes:**
```bash
python monitor_automation.py 2
```

**Shows:**
- Updates every 5 minutes
- Real-time statistics
- Perfect for monitoring scheduled runs

---

## 📊 What You'll See During a Test Run

### 1. **Telegram Scraping Phase**
```
🔄 Processing jobs...
   Progress: 100/1496 jobs processed
   
TELEGRAM SCRAPER - SCHEDULED JOB
================================================================================
   Total channels: 95
   Successful: 60
   Failed: 35
   Total messages: 1,188
   Duration: 187.23s
================================================================================
```

### 2. **Job Processing Phase**
```
🤖 ML PIPELINE PROCESSING
   • Extracting job details...
   • Classifying with 92.27% accuracy model...
   • Applying location intelligence...
   • Calculating quality scores...
   
   ✅ Processed 125 new jobs
```

### 3. **Location Filtering Results**
```
🌍 LOCATION FILTERING
   ✅ India Remote: 45 jobs (score: 100)
   ✅ India Hybrid: 32 jobs (score: 95)
   ✅ International Remote: 8 jobs (score: 90)
   ✅ India Office: 28 jobs (score: 70)
   ❌ International Onsite: 12 jobs (score: 0) - FILTERED
```

### 4. **Database Statistics**
```
💾 DATABASE UPDATE
   MongoDB raw_messages: +1,188 documents
   PostgreSQL jobs: +125 new jobs
   All with location_compatibility scores
```

---

## 🎯 Quick Start - Test Automation NOW

**Easiest way - Run this one command:**
```bash
./test_automation.sh
```

Then choose option **1** to trigger immediately.

**What happens:**
1. ✅ Server starts (if not running)
2. 🚀 Scraper triggers immediately
3. ⏳ Waits 30 seconds for processing
4. 📊 Shows complete results

**Expected results after 30 seconds:**
- **Messages fetched:** 800-1,500 (from 60+ channels)
- **Jobs created:** 50-150 (depends on new messages)
- **Processing time:** ~3-5 minutes
- **Location filtering:** Active (13% international onsite filtered)

---

## 🔍 Verify Automation is Working

### Method 1: Check Scheduler Status
```bash
curl http://localhost:8000/api/telegram-scraper/scheduler/status | python -m json.tool
```

**Expected output:**
```json
{
  "running": true,
  "total_jobs": 3,
  "jobs": [
    {
      "id": "telegram_scraper_4hourly",
      "name": "Telegram Scraper (Every 4 hours)",
      "next_run_time": "2026-02-18T16:00:00",
      "trigger": "cron[hour='4,8,12,16,20,0', minute='0']"
    }
  ]
}
```

### Method 2: Check Last Run Results
```bash
python monitor_automation.py 1
```

### Method 3: Query Database Directly
```bash
# Check recent jobs
psql $DATABASE_URL -c "
SELECT COUNT(*) as new_jobs, 
       MAX(created_at) as last_job_time
FROM jobs 
WHERE created_at > NOW() - INTERVAL '1 hour';"

# Check location filtering
psql $DATABASE_URL -c "
SELECT 
  (quality_breakdown::jsonb->>'location_compatibility')::int as loc_score,
  COUNT(*) as count
FROM jobs 
WHERE is_active = true 
  AND created_at > NOW() - INTERVAL '1 hour'
GROUP BY loc_score
ORDER BY loc_score DESC;"
```

---

## ⏰ Current Automation Schedule

### Production Schedule (IST)
| Task | Time | Frequency |
|------|------|-----------|
| Telegram Scraping | 9:30 AM | Every 4 hours |
| Telegram Scraping | 1:30 PM | Every 4 hours |
| Telegram Scraping | 5:30 PM | Every 4 hours |
| Telegram Scraping | 9:30 PM | Every 4 hours |
| Telegram Scraping | 1:30 AM | Every 4 hours |
| Telegram Scraping | 5:30 AM | Every 4 hours |
| Daily Slack Update | 2:30 PM | Once daily |
| Channel Sync | - | Every 6 hours |

**Next scheduled run:** Check with scheduler status API

---

## 🛠️ Important Notes

### 1. **Server Must Be Running**
The automation **only works when FastAPI server is running**:
```bash
# Start server
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Or in background
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > logs/server.log 2>&1 &
```

### 2. **Telegram Accounts Required**
- Currently using **4 active accounts** (Account 4 expired)
- Each account handles ~15-25 channels
- Round-robin rotation prevents rate limiting

### 3. **Processing Time**
- **Scraping:** ~3 minutes for 1,200 messages
- **ML Processing:** ~2 minutes for 150 jobs
- **Total:** ~5 minutes per scheduled run

### 4. **Location Filtering Active**
- ✅ All jobs automatically scored for location compatibility
- ❌ ~13% international onsite jobs filtered (score: 0)
- ✅ Remote and India-based jobs prioritized

---

## 📈 Expected Performance

### Per Scheduled Run (Every 4 Hours)
| Metric | Typical Range | Notes |
|--------|---------------|-------|
| Messages Fetched | 800-1,500 | From 60 active channels |
| Jobs Extracted | 50-150 | ~10-15% of messages |
| Processing Time | 3-5 minutes | Full pipeline |
| International Onsite Filtered | 6-20 jobs | ~13% of total |
| Location Scores Added | 100% | All jobs |

### Daily Totals (6 Runs)
| Metric | Daily Total |
|--------|-------------|
| Messages | 4,800-9,000 |
| New Jobs | 300-900 |
| Filtered Jobs | ~40-120 |

---

## 🚨 Troubleshooting

### Issue: Scheduler not running
**Check:**
```bash
curl http://localhost:8000/api/telegram-scraper/scheduler/status
```

**Solution:** Restart FastAPI server
```bash
pkill -f "uvicorn app.main:app"
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Issue: No new messages
**Possible causes:**
1. Not at scheduled time (runs every 4 hours)
2. Telegram accounts expired
3. Channels have no new messages

**Check:**
```bash
python monitor_automation.py 1
```

### Issue: Jobs not appearing
**Check ML pipeline:**
```bash
# Test ML classifier
python -c "
from app.ml.sklearn_classifier import SklearnClassifier
classifier = SklearnClassifier()
result = classifier.classify('Looking for Python developer with 2 years exp')
print(f'Classification: {result}')
"
```

---

## 🎓 Summary

### ✅ System Status: FULLY AUTOMATED

**What's Automated:**
- ✅ Telegram message fetching (4 accounts, 60 channels)
- ✅ ML job extraction (92.27% accuracy)
- ✅ Location intelligence (geographic + work mode)
- ✅ Quality scoring (7 components)
- ✅ Database storage (MongoDB + PostgreSQL)
- ✅ Location filtering (13% jobs filtered)

**How to Test:**
```bash
# Quick test - Run NOW
./test_automation.sh

# Monitor results
python monitor_automation.py 1

# Schedule for 2:40 PM
./test_automation.sh  # Choose option 2
```

**Next Scheduled Run:**
Check with: `curl http://localhost:8000/api/telegram-scraper/scheduler/status`

---

## 📞 Quick Commands Reference

```bash
# 1. Test automation NOW
./test_automation.sh  # Choose option 1

# 2. Check recent results
python monitor_automation.py 1

# 3. Monitor continuously  
python monitor_automation.py 2

# 4. Check scheduler status
curl http://localhost:8000/api/telegram-scraper/scheduler/status | python -m json.tool

# 5. Trigger scraper manually
curl -X POST http://localhost:8000/api/telegram-scraper/scheduler/trigger/telegram_scraper_4hourly

# 6. View server logs
tail -f /tmp/fastapi_automation.log

# 7. Check database
psql $DATABASE_URL -c "SELECT COUNT(*) FROM jobs WHERE created_at > NOW() - INTERVAL '1 hour';"
```

---

**Ready to test?** Run: `./test_automation.sh` 🚀
