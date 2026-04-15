# Storage Architecture Documentation

## 📊 Data Flow Overview

This document explains the **correct** storage architecture for the Telegram scraping and job classification system.

---

## 🏗️ Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TELEGRAM CHANNELS                            │
│              (95 channels monitored for job postings)                │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ ① TELEGRAM SCRAPER SERVICE
                             │    (5 accounts, random delays 0.5-2s)
                             │    Reads channel list from: MongoDB channels
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      MONGODB (Raw Storage)                           │
│                                                                       │
│  Collection: raw_messages                                            │
│  ├─ All raw telegram messages (unprocessed)                          │
│  ├─ Fields: message_id, channel_username, text, sender_id, etc.     │
│  └─ Status: is_processed (false until ML processes)                 │
│                                                                       │
│  Collection: channels                                                │
│  ├─ Channel metadata for Lambda scraper                             │
│  ├─ Synced from PostgreSQL telegram_groups                          │
│  └─ Fields: username, is_active, health_score, last_fetched_at      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ ② ML CLASSIFIER (Periodic Job)
                             │    Reads: MongoDB raw_messages (is_processed=false)
                             │    Classifies: job vs non-job
                             │    Extracts: title, company, location, salary, etc.
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   POSTGRESQL (Structured Storage)                    │
│                                                                       │
│  Table: jobs                                                         │
│  ├─ Classified job postings only                                    │
│  ├─ Extracted structured data                                       │
│  └─ Fields: title, company, location, is_relevant, quality_score    │
│                                                                       │
│  Table: telegram_groups                                             │
│  ├─ Source of truth for channel configuration                       │
│  ├─ Tracks channel health and performance                           │
│  └─ Fields: username, health_score, is_active, relevance_ratio      │
│                                                                       │
│  Table: telegram_accounts                                           │
│  ├─ 5 accounts for scraping rotation                                │
│  └─ Fields: phone, health_status, consecutive_errors                │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ ③ CHANNEL SYNC SERVICE
                             │    Bidirectional: PostgreSQL ↔ MongoDB
                             │    Runs: Every 6 hours
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   MONGODB channels (synced back)                     │
│  Lambda functions read updated channel list with health scores      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Storage Responsibilities

### MongoDB (Document Store - Raw Data)

**Purpose**: Store raw, unprocessed data from Telegram

**Collections**:
1. **`raw_messages`** (Primary)
   - All raw Telegram messages
   - Stored as-is with minimal processing
   - Fields: `message_id`, `channel_username`, `text`, `sender_id`, `message_date`, `is_processed`
   - Retention: Keep until processed + 30 days

2. **`channels`** (Synced from PostgreSQL)
   - Channel metadata for Lambda scraper
   - Synced from PostgreSQL `telegram_groups` every 6 hours
   - Fields: `username`, `is_active`, `health_score`, `last_fetched_at`

**Why MongoDB for raw messages?**
- Fast writes for high-volume ingestion
- Flexible schema for varied message formats
- Cost-effective for large raw data storage
- Lambda functions have easy MongoDB integration

---

### PostgreSQL (Relational DB - Structured Data)

**Purpose**: Store classified, structured job data and system configuration

**Tables**:
1. **`jobs`** (Classified Messages Only)
   - Only messages classified as job postings
   - Structured data extracted by ML classifier
   - Fields: `title`, `company`, `location`, `experience`, `salary`, `is_relevant`, `quality_score`
   - Source: Processed from MongoDB `raw_messages`

2. **`telegram_groups`** (Source of Truth)
   - Channel configuration and health tracking
   - Fields: `username`, `title`, `health_score`, `is_active`, `relevance_ratio`
   - Synced TO MongoDB `channels` collection

3. **`telegram_accounts`** (5 accounts)
   - Account health and rotation management
   - Fields: `phone`, `api_id`, `api_hash`, `health_status`, `consecutive_errors`

**Why PostgreSQL for jobs?**
- ACID transactions for data integrity
- Complex queries and joins for analytics
- Better for structured, relational data
- Easier to maintain data consistency

---

## 🔄 Synchronization Logic

### ✅ CORRECT Sync: PostgreSQL ↔ MongoDB Channels

**Service**: `app/services/channel_sync_service.py`

**Frequency**: Every 6 hours (via `run_channel_sync` scheduled job)

**Direction**: Primarily PostgreSQL → MongoDB (one-way)

**What gets synced**:
```python
{
    "channel_id": str(channel.id),
    "username": channel.username,
    "is_active": channel.is_active,
    "health_score": channel.health_score,  # 0-100 score
    "last_fetched_at": channel.last_scraped_at,
    "category": channel.category,
    "synced_from_postgres": True
}
```

**Why this sync?**
- Scraper reads from MongoDB channels to know which channels to fetch
- Health scores determine scraping priority (low-score channels scraped less)
- PostgreSQL is source of truth for channel configuration

---

### ❌ INCORRECT Sync: MongoDB ↔ PostgreSQL Raw Messages

**Service**: ~~`app/services/storage_sync_validator.py`~~ (DEPRECATED)

**Why this is wrong?**
- Raw messages should ONLY be in MongoDB
- PostgreSQL should NOT have a `raw_telegram_messages` table
- This creates unnecessary data duplication
- Increases storage costs and sync complexity

**The `raw_telegram_messages` table in PostgreSQL is NOT used and can be ignored/removed.**

---

## 🔧 Service Breakdown

### 1. Telegram Scraper Service

**File**: `app/services/telegram_scraper_service.py`

**What it does**:
- Connects to Telegram using 5 accounts (rotation)
- Reads channel list from **MongoDB `channels` collection**
- Fetches new messages from each channel
- Saves raw messages to **MongoDB `raw_messages`**
- Updates scraping metadata (last_message_id, last_scraped_at)

**Key Features**:
- Random delays (0.5-2.0s) for human-like behavior
- FloodWait handling
- Account rotation for ban prevention
- First-fetch limit (10 messages per channel)

**Does NOT**:
- ❌ Save to PostgreSQL `raw_telegram_messages`
- ❌ Classify messages (done separately by ML)

---

### 2. ML Classifier Service

**File**: `app/ml/ensemble_classifier.py`

**What it does**:
- Reads unprocessed messages from **MongoDB `raw_messages`** (`is_processed=false`)
- Runs classification (job vs non-job)
- Extracts structured data (title, company, location, etc.)
- Saves classified jobs to **PostgreSQL `jobs` table**
- Marks messages as processed in MongoDB

**Runs**: Periodically via cron or manual trigger

---

### 3. Channel Sync Service

**File**: `app/services/channel_sync_service.py`

**What it does**:
- Syncs **PostgreSQL `telegram_groups`** → **MongoDB `channels`**
- Ensures Lambda functions have updated channel list
- Includes health scores for scraping prioritization

**Runs**: Every 6 hours via scheduler

---

## 🚫 Deprecated/Unused Services

### ❌ `app/services/telegram_service.py`

**Status**: Legacy service, DO NOT USE for main scraping

**Why deprecated?**
- Saves raw messages to PostgreSQL (incorrect architecture)
- Only used by `lambda/group_joiner` (legacy Lambda)
- Main scraping should use `telegram_scraper_service.py`

**Action**: Do not use this service for new development

---

### ❌ `app/services/storage_sync_validator.py`

**Status**: DEPRECATED as of 2026-02-17

**Why deprecated?**
- Validates MongoDB `raw_messages` ↔ PostgreSQL `raw_telegram_messages` sync
- This sync is incorrect and not needed
- Raw messages should only be in MongoDB

**Action**: File kept for reference but marked as deprecated

---

## 📝 Summary

### ✅ Correct Data Flow

1. **Scraping**: Telegram → telegram_scraper_service → **MongoDB raw_messages**
2. **Classification**: **MongoDB raw_messages** → ML Classifier → **PostgreSQL jobs**
3. **Channel Sync**: **PostgreSQL telegram_groups** ↔ **MongoDB channels** (every 6 hours)

### ❌ Incorrect Patterns to Avoid

1. Saving raw messages to PostgreSQL ❌
2. Using `telegram_service.py` for main scraping ❌
3. Syncing raw_messages between MongoDB and PostgreSQL ❌

### 🎯 Key Principle

**Separation of Concerns**:
- **MongoDB** = Raw, unstructured incoming data
- **PostgreSQL** = Classified, structured job data + system configuration
- **Sync only channels** (configuration data), not raw messages

---

## 🔗 Related Files

- ✅ `app/services/telegram_scraper_service.py` - Main scraper (USE THIS)
- ✅ `app/services/channel_sync_service.py` - Channel sync (CORRECT)
- ✅ `app/ml/ensemble_classifier.py` - ML classification
- ❌ `app/services/telegram_service.py` - Legacy (DO NOT USE)
- ❌ `app/services/storage_sync_validator.py` - Deprecated (WRONG APPROACH)

---

Last Updated: February 17, 2026
