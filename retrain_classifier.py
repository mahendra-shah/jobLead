"""
Phase 4B: Retrain the TF-IDF + Random Forest job classifier.

Data sources (merged in priority order):
1. app/ml/training/training_data.json   — 293 curated synthetic examples
2. app/ml/training/data/messages_latest.csv — ~1360 real MongoDB messages
                                              (labeled by the previous model)
3. HARD_NEGATIVES (hand-crafted)         — Telegram FP patterns observed in
                                           the 300-message pipeline test

Usage (inside Docker):
    docker-compose exec backend python retrain_classifier.py

Flags:
    --dry-run   Show data stats only, do not train
    --no-real   Skip messages_latest.csv (use synthetic + hard negatives only)
"""

import argparse
import csv
import json
import sys
import os
from pathlib import Path
from typing import List, Dict, Tuple

# ── Hard-negative examples ──────────────────────────────────────────────────
# These are the FP patterns the existing model kept passing as jobs.
# Each is labelled is_job=False.
HARD_NEGATIVES: List[str] = [
    # Telegram channel promo lists
    "Python :- t.me/pythondeveloper\nJavaScript :- t.me/javascript_dev\n"
    "React :- t.me/react_frontend\nNodeJS :- t.me/nodejs_backend",

    "🔗 Join these channels for jobs:\n1. Python :- http://t.me/pydev\n"
    "2. Data Analytics :- t.me/data_analytics_jobs\n"
    "3. Learn Excel :- t.me/excel_tips",

    "Best Telegram channels for coding:\nt.me/leetcode_practice\n"
    "t.me/system_design\nt.me/java_developer",

    "Follow for free resources:\nBI :- t.me/powerbi_analyst\n"
    "ML :- t.me/machine_learning_india",

    "📢 Free resources:\nLearn Excel: http://t.me/excelmaster\n"
    "Learn SQL: http://t.me/sqlbasics\n"
    "Learn Python: http://t.me/pythonbasics",

    # Q&A and interview prep
    "Questions & Answers 🔥\n1. What is polymorphism?\n"
    "2. Difference between abstract class and interface?\n"
    "3. What is SOLID principle?",

    "Top 10 Java Interview Questions:\n"
    "Q1: What is JVM?\nQ2: What is garbage collection?\n"
    "Q3: Explain multithreading.",

    "SQL Interview Questions & Answers 📝\n"
    "Q: What is a primary key?\nA: A column with unique non-null values.\n"
    "Q: What is a foreign key?",

    "OOP Concepts - Important for Interviews:\n"
    "1. Encapsulation\n2. Abstraction\n3. Inheritance\n4. Polymorphism",

    "Crack FAANG with these DSA questions:\n"
    "1. Two Sum (Easy)\n2. LRU Cache (Medium)\n3. Word Ladder (Hard)",

    # Course announcements
    "🎓 Free Courses by Cisco:\n- Python Essentials\n- Cybersecurity Basics\n"
    "- Data Analytics Fundamentals\nEnroll here: cisco.com/courses",

    "FREE AWS Certification Course — Enroll Now!\n"
    "Valid till: March 2026\nLink: aws.amazon.com/training",

    "Google is offering free Gen AI courses on Coursera!\n"
    "Link: coursera.org/google-ai\nThis week only — no job application needed.",

    "🎁 FREE Data Science Bootcamp by IIT:\n"
    "Duration: 3 months\nMode: Online\nRegister: iit.ac.in/datascience",

    "Microsoft Learn — Free certificates:\n✅ Azure Fundamentals\n"
    "✅ Power BI Analyst\n✅ Security Operations",

    # Announcements / promos with no job content
    "🚨 Announced Guys! Big Hiring Drive is LIVE!\n"
    "Check the pinned message for details.",

    "Drive is LIVE — visit the link in our bio for all open positions.",

    "📣 BIG ANNOUNCEMENT: Join our WhatsApp group for daily job updates!\n"
    "Link: wa.me/91XXXXXXXXXX",

    "Follow this channel for daily job posts 🔔\n"
    "Turn on notifications so you never miss an opening.",

    "Morning update 🌅 — new jobs added today!\n"
    "Scroll up to see all today's postings.",

    # Inspirational / motivational
    "Hard work always pays off 💪\nKeep grinding, your dream job is next!",

    "Never give up on your goals. Success is just around the corner 🌟",

    # News / general tech content
    "OpenAI releases GPT-5 with multimodal capabilities.\n"
    "Read the full research paper at arxiv.org",

    "Google I/O 2026 highlights: new Gemini features, Android 16 announced.",
]


def load_synthetic(path: str) -> List[Dict]:
    """Load examples from training_data.json (dict with 'examples' key)."""
    with open(path) as f:
        data = json.load(f)
    examples = data.get('examples', [])
    out = []
    for ex in examples:
        if isinstance(ex, dict) and 'text' in ex and 'is_job' in ex:
            out.append({'text': ex['text'], 'is_job': bool(ex['is_job'])})
    return out


def load_csv(path: str) -> List[Dict]:
    """Load examples from messages_latest.csv."""
    out = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = row.get('text', '').strip()
            is_job_str = row.get('is_job', '').strip()
            if not text:
                continue
            # Handle both bool strings and 1/0
            if is_job_str.lower() in ('true', '1'):
                is_job = True
            elif is_job_str.lower() in ('false', '0'):
                is_job = False
            else:
                continue
            out.append({'text': text, 'is_job': is_job})
    return out


def deduplicate(dataset: List[Dict]) -> List[Dict]:
    """
    Remove duplicate messages (by exact text match).

    Keeps the first occurrence in the list, so synthetic examples
    (which are added first) take priority over CSV labels.
    """
    seen: set = set()
    result = []
    for ex in dataset:
        key = ex['text'].strip().lower()[:200]
        if key not in seen:
            seen.add(key)
            result.append(ex)
    return result


def build_dataset(
    use_real: bool = True,
) -> Tuple[List[Dict], Dict]:
    """
    Assemble the combined training dataset.

    Returns:
        (dataset, stats_dict)
    """
    base_dir = Path(__file__).parent / 'app' / 'ml' / 'training'
    synthetic_path = base_dir / 'training_data.json'
    csv_path = base_dir / 'data' / 'messages_latest.csv'

    # 1. Synthetic examples (highest quality)
    synthetic = load_synthetic(str(synthetic_path))
    print(f"Loaded {len(synthetic)} synthetic examples")

    # 2. Real labeled messages
    real: List[Dict] = []
    if use_real and csv_path.exists():
        real = load_csv(str(csv_path))
        print(f"Loaded {len(real)} real labeled messages from CSV")
    elif use_real:
        print(f"⚠  CSV not found at {csv_path}, skipping")

    # 3. Hard negatives
    hard_negs = [{'text': t, 'is_job': False} for t in HARD_NEGATIVES]
    print(f"Added {len(hard_negs)} hand-crafted hard negatives")

    # Merge: synthetic first (priority in dedup), then real, then hard negs
    combined = synthetic + real + hard_negs
    combined = deduplicate(combined)

    jobs = [x for x in combined if x['is_job']]
    not_jobs = [x for x in combined if not x['is_job']]

    stats = {
        'total': len(combined),
        'jobs': len(jobs),
        'not_jobs': len(not_jobs),
        'ratio': f"{len(jobs)}:{len(not_jobs)}",
        'sources': {
            'synthetic': len(synthetic),
            'real_csv': len(real),
            'hard_negatives': len(hard_negs),
        },
    }
    return combined, stats


def main() -> None:
    """Entry point: build dataset and retrain the classifier."""
    parser = argparse.ArgumentParser(
        description='Retrain the job classifier for Phase 4B'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show dataset stats only, do not train'
    )
    parser.add_argument(
        '--no-real', action='store_true',
        help='Skip messages_latest.csv (synthetic + hard negatives only)'
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Phase 4B — Classifier Retrain")
    print("=" * 60)

    dataset, stats = build_dataset(use_real=not args.no_real)

    print()
    print("Dataset summary:")
    print(f"  Total examples : {stats['total']}")
    print(f"  Jobs           : {stats['jobs']}")
    print(f"  Not-jobs       : {stats['not_jobs']}")
    print(f"  Ratio          : {stats['ratio']}")
    print(f"  Sources        : {stats['sources']}")
    print()

    if args.dry_run:
        print("Dry-run mode — skipping training.")
        return

    if stats['jobs'] < 10 or stats['not_jobs'] < 10:
        print("❌ Not enough examples to train. Need at least 10 of each class.")
        sys.exit(1)

    # Import here so the script can be run outside Docker for dry-run checks
    from app.ml.sklearn_classifier import SklearnClassifier

    print("Initialising classifier (will load existing model if present)...")
    clf = SklearnClassifier()

    print("Training...")
    metrics = clf.train(dataset)

    print()
    if metrics.get('success'):
        print("✅ Retrain complete!")
        print(f"   Train accuracy : {metrics['train_accuracy']:.3f}")
        print(f"   Test accuracy  : {metrics['test_accuracy']:.3f}")
        print(f"   Precision      : {metrics['test_precision']:.3f}")
        print(f"   Recall         : {metrics['test_recall']:.3f}")
        print(f"   F1 Score       : {metrics['test_f1']:.3f}")
        print(f"   Model version  : {metrics['model_version']}")
    else:
        print(f"❌ Retrain failed: {metrics.get('error')}")
        sys.exit(1)


if __name__ == '__main__':
    main()
