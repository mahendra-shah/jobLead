"""
Create enhanced training_data.json from real Telegram CSV data
FIXED VERSION - Preserves full text and better job detection
"""

import pandas as pd
import json
import re
from datetime import datetime
from typing import List, Dict

# Spam/Non-Job Patterns (updated)
SPAM_PATTERNS = [
    "interview support",
    "interview help", 
    "proxy interview",
    "fwb", "friends with benefits",
    "bench sales",
    "resume selling",
    "fake experience",
    "₹3,000–6,000 on one-time interview",
    "no recruiter can catch you"
]

# Strong Job Indicators (updated)
JOB_INDICATORS = [
    # Action words
    "hiring", "we're hiring", "we are hiring", "join", "apply",
    "vacancy", "opening", "position", "role", "opportunity",
    "job", "career", "recruitment", "hiring for",
    
    # Application-related
    "apply now", "apply here", "apply link", "apply at",
    "send resume", "send cv", "share resume", "email resume",
    "interested candidates", "contact", "register",
    
    # Experience/Skills
    "experience", "years", "fresher", "freshers", "graduate",
    "skills", "required", "qualification", "eligibility",
    
    # Compensation
    "salary", "ctc", "lpa", "package", "compensation",
    
    # Location
    "location", "bangalore", "mumbai", "delhi", "hyderabad",
    "chennai", "pune", "remote", "work from home"
]

# Apply Link Patterns
APPLY_LINK_PATTERNS = [
    r'https?://[^\s]+apply',
    r'apply[:\s]+https?://[^\s]+',
    r'link[:\s]+https?://[^\s]+',
    r'https?://careers\.',
    r'https?://jobs\.',
    r'https?://[^\s]+/job/',
    r'https?://forms\.gle',
    r'https?://[^\s]+careers',
    r'https?://[^\s]+workable\.com'
]


def has_apply_link(text: str) -> bool:
    """Check if message contains an apply link."""
    text_lower = text.lower()
    for pattern in APPLY_LINK_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    return False


def extract_apply_link(text: str) -> str:
    """Extract the apply link from text."""
    for pattern in APPLY_LINK_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Extract full URL (might extend beyond pattern)
            url_match = re.search(r'https?://[^\s"\'<>]+', text[match.start():])
            if url_match:
                return url_match.group(0).rstrip('/')
    return None


def is_spam(text: str) -> bool:
    """Check if message is spam/non-job."""
    text_lower = text.lower()
    
    # Check spam patterns
    spam_count = sum(1 for pattern in SPAM_PATTERNS if pattern in text_lower)
    if spam_count >= 2:  # Multiple spam indicators
        return True
    
    # Single strong spam indicator
    if any(pattern in text_lower for pattern in [
        "interview support for less price",
        "proxy interview",
        "fake experience",
        "bench sales"
    ]):
        return True
        
    return False


def is_job(text: str, job_type: str = None) -> bool:
    """
    Enhanced job detection.
    Returns True if message is a real job posting.
    """
    text_lower = text.lower()
    
    # 1. First check - is it spam?
    if is_spam(text):
        return False
    
    # 2. Strong positive indicators
    strong_indicators = 0
    
    # Has apply link (very strong indicator)
    if has_apply_link(text):
        strong_indicators += 3
    
    # Has job type from CSV
    if job_type and job_type in ['tech', 'non-tech', 'healthcare', 'sales', 'tech_fresher']:
        strong_indicators += 2
    
    # Has action words
    if any(word in text_lower for word in [
        "we're hiring", "we are hiring", "join our team",
        "apply now", "apply here", "send resume"
    ]):
        strong_indicators += 2
    
    # Has company/recruiter language
    if any(phrase in text_lower for phrase in [
        "company:", "position:", "role:", "location:",
        "experience:", "salary:", "ctc:", "qualification:"
    ]):
        strong_indicators += 1
    
    # 3. Count general job indicators
    indicator_count = sum(
        1 for indicator in JOB_INDICATORS 
        if indicator in text_lower
    )
    
    # 4. Decision logic
    if strong_indicators >= 3:
        return True  # Strong evidence of job
    
    if strong_indicators >= 1 and indicator_count >= 5:
        return True  # Moderate evidence + multiple indicators
    
    if indicator_count >= 8:
        return True  # Many job-related words
    
    # 5. Check message length
    # Very short messages are unlikely to be jobs
    if len(text) < 100:
        return False
    
    return False  # Default to non-job if unsure


def clean_text(text: str) -> str:
    """Clean and normalize text - KEEP FULL LENGTH."""
    if pd.isna(text):
        return ""
    
    # Remove excessive whitespace
    text = " ".join(text.split())
    
    # DON'T truncate - keep full text!
    # Apply links are usually at the end
    
    return text


def process_csv_to_training_data(csv_file: str) -> List[Dict]:
    """Process CSV and create training examples with FULL text."""
    
    df = pd.read_csv(csv_file)
    
    training_examples = []
    
    print(f"\n📊 Processing {len(df)} messages from CSV...")
    
    job_count = 0
    non_job_count = 0
    
    for idx, row in df.iterrows():
        text = clean_text(row['Full Message Text'])
        
        if not text or len(text) < 50:  # Skip very short messages
            continue
        
        # Get job type from CSV
        job_type = row.get('Job Type', None)
        if pd.notna(job_type):
            job_type = str(job_type).lower()
        
        # Determine if it's a job
        is_job_posting = is_job(text, job_type)
        
        if is_job_posting:
            job_count += 1
        else:
            non_job_count += 1
        
        # Extract apply link if exists
        apply_link = extract_apply_link(text)
        
        # Create training example with FULL metadata
        example = {
            "text": text,  # FULL TEXT - no truncation!
            "is_job": is_job_posting,
            "metadata": {
                "source": "telegram_csv",
                "message_id": str(row['Message ID']),
                "channel": str(row['Group Name']),
                "date": str(row['Date']),
                "job_type": job_type if pd.notna(job_type) else None,
                "keywords_found": str(row.get('Keywords Found', '')),
                "has_apply_link": apply_link is not None,
                "apply_link": apply_link,
                "confidence": "auto_labeled",
                "text_length": len(text)
            }
        }
        
        training_examples.append(example)
    
    print(f"✅ Processed {len(training_examples)} valid messages")
    print(f"   Jobs: {job_count} ({job_count/len(training_examples)*100:.1f}%)")
    print(f"   Non-jobs: {non_job_count} ({non_job_count/len(training_examples)*100:.1f}%)")
    
    return training_examples


def merge_with_existing_training_data(
    new_examples: List[Dict],
    existing_file: str = "app/ml/training/training_data.json"
) -> Dict:
    """Merge new examples with existing training data."""
    
    try:
        with open(existing_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = {
            "version": "1.0",
            "created": datetime.now().strftime("%Y-%m-%d"),
            "description": "Training data for job classification",
            "examples": []
        }
    
    # Keep manual examples (don't overwrite them)
    manual_examples = [
        ex for ex in existing_data.get("examples", [])
        if ex.get("metadata", {}).get("source") == "manual"
    ]
    
    # Combine manual + new CSV examples
    all_examples = manual_examples + new_examples
    
    # Update metadata
    existing_data["version"] = "3.0"  # New version with full text
    existing_data["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    existing_data["examples"] = all_examples
    existing_data["total_examples"] = len(all_examples)
    
    # Calculate statistics
    job_count = sum(1 for ex in all_examples if ex["is_job"])
    non_job_count = len(all_examples) - job_count
    
    # Count by source
    sources = {}
    for ex in all_examples:
        source = ex.get("metadata", {}).get("source", "unknown")
        sources[source] = sources.get(source, 0) + 1
    
    # Count messages with apply links
    with_links = sum(
        1 for ex in all_examples 
        if ex.get("metadata", {}).get("has_apply_link", False)
    )
    
    existing_data["statistics"] = {
        "total": len(all_examples),
        "jobs": job_count,
        "non_jobs": non_job_count,
        "ratio": f"{job_count}:{non_job_count}",
        "sources": sources,
        "with_apply_links": with_links,
        "avg_text_length": int(sum(
            ex.get("metadata", {}).get("text_length", 0) 
            for ex in all_examples
        ) / len(all_examples)) if all_examples else 0
    }
    
    return existing_data


def main():
    print("🚀 Creating Enhanced Training Data from Telegram CSV")
    print("=" * 60)
    print("✅ IMPROVEMENTS:")
    print("  - Full text preserved (no truncation)")
    print("  - Better job detection (apply links, job titles)")
    print("  - Complete metadata (job type, keywords, apply links)")
    print("=" * 60)
    
    # Process CSV
    csv_file = "Telegram Jobs - Daily Export - 2026-01-14.csv"
    print(f"\n📁 Reading CSV: {csv_file}")
    
    new_examples = process_csv_to_training_data(csv_file)
    
    # Merge with existing data
    print("\n🔄 Merging with existing training data...")
    merged_data = merge_with_existing_training_data(new_examples)
    
    # Backup old file
    backup_file = f"app/ml/training/training_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open("app/ml/training/training_data.json", 'r') as f:
            old_data = json.load(f)
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(old_data, f, indent=2, ensure_ascii=False)
        print(f"   💾 Backed up old data to: {backup_file}")
    except FileNotFoundError:
        print("   ℹ️  No existing training data to backup")
    
    # Save updated training data
    output_file = "app/ml/training/training_data.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Saved enhanced training data to: {output_file}")
    print("\n📊 Final Statistics:")
    for key, value in merged_data["statistics"].items():
        print(f"  {key}: {value}")
    
    print("\n🎯 Key Improvements:")
    print(f"  ✓ Average text length: {merged_data['statistics']['avg_text_length']} chars")
    print(f"  ✓ Messages with apply links: {merged_data['statistics']['with_apply_links']}")
    print(f"  ✓ Job detection accuracy: Improved with apply link detection")
    
    print("\n🔍 Sample Job with Apply Link:")
    # Show a sample job with apply link
    job_with_link = next(
        (ex for ex in merged_data["examples"] 
         if ex["is_job"] and ex.get("metadata", {}).get("has_apply_link")),
        None
    )
    if job_with_link:
        print(f"  Channel: {job_with_link['metadata']['channel']}")
        print(f"  Apply Link: {job_with_link['metadata']['apply_link']}")
        print(f"  Text Length: {job_with_link['metadata']['text_length']} chars")
    
    print("\n🎯 Next Steps:")
    print("1. Review sample data: python -c \"import json; d=json.load(open('app/ml/training/training_data.json')); print(json.dumps(d['examples'][50], indent=2))\"")
    print("2. Retrain the model: python scripts/retrain_model.py")
    print("3. Test predictions on new messages")


if __name__ == "__main__":
    main()
