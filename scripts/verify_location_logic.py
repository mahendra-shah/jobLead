"""
Local verification script for the allowlist location logic.
No DB or Docker needed — instantiates EnhancedJobExtractor and tests sample posts.
Run: PYTHONPATH=. python3 scripts/verify_location_logic.py
"""
import json, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.ml.enhanced_extractor import EnhancedJobExtractor

extractor = EnhancedJobExtractor()

# Each sample is (label, text, expected_scope)
SAMPLES = [
    # ── Should be INDIA ────────────────────────────────────────────────────────
    (
        "India city in text",
        "Hiring for Software Engineer - Bangalore. 0-2 years exp. Apply now!",
        "india",
    ),
    (
        "India state in text",
        "Looking for freshers in Maharashtra. WFO, Pune office.",
        "india",
    ),
    (
        "word 'india' in text",
        "Best freshers job — anywhere in India. Full remote.",
        "india",
    ),
    (
        "Remote, no location",
        "Work from home | 0-1 year | apply for data entry role. Salary 15k/month.",
        "india",
    ),
    (
        "Location: Mumbai (bot format with India city)",
        "📢 **Data Analyst**\n**Company:** TCS\n**Location:** Mumbai 📍\n**Experience:** 0-2 years",
        "india",
    ),
    (
        "Bengaluru job post",
        "Hiring: Product Manager | 1-2 years | Bengaluru | CTC 6-10 LPA",
        "india",
    ),
    (
        "Tamil Nadu state name",
        "Job opening in Tamil Nadu — freshers welcome. Coimbatore office.",
        "india",
    ),

    # ── Should be INTERNATIONAL (rejected) ─────────────────────────────────────
    (
        "Family Office / Bristol (UK city)",
        "📢 **Associate Advisor**\n**Company:** Family Office\n**Location:** Bristol 📍\n**Date Posted:** 2026-03-01 📅\n**Work Type:** On-site",
        "international",
    ),
    (
        "Phoenix AZ (US city)",
        "📢 **Financial Analyst**\n**Company:** CIM Group\n**Location:** Phoenix 📍\n**Date Posted:** 2026-03-03 📅",
        "international",
    ),
    (
        "Addis Ababa Ethiopia",
        "📢 **CNC Machine Operator**\n**Location:** Addis Ababa 📍\nEthiopia manufacturing plant",
        "international",
    ),
    (
        "Freelanceethbot Ethiopia",
        "Job at AAI Logistics | 3 years exp | Addis Ababa, Ethiopia | Apply via link",
        "international",
    ),
    (
        "London explicit",
        "Software Engineer needed in London. £80k salary. Start ASAP.",
        "international",
    ),
    (
        "Remote but explicitly UAE",
        "100% remote | Based in Dubai, UAE | Part-time developer",
        "international",
    ),
    (
        "Toronto Canada",
        "Hiring: Cloud Engineer | Toronto office | 2-4 years | CAD 90k",
        "international",
    ),

    # ── 'uk' word-boundary regression tests ────────────────────────────────
    (
        "WFH post with 'Mukul' in name (was falsely blocked by 'uk' substring)",
        "Work from home | 0-1 yr | WhatsApp Mukul: +91-9876543210 | Data entry freshers",
        "india",
    ),
    (
        "Bulk hiring with WFH (was falsely blocked by 'bulk' containing 'uk')",
        "Bulk hiring for freshers | WFH | 12k/month | Gujarat",
        "india",
    ),
    (
        "Truck driver job in India (was falsely blocked by 'truck')",
        "Truck driver vacancy | Rajasthan | 18k salary | apply now",
        "india",
    ),
    # ── 'sec' removal regression tests ──────────────────────────────────────
    (
        "Security engineer post (was falsely India-flagged by 'sec')",
        "Security engineer needed in Phoenix, Arizona. 5+ years exp. Remote possible.",
        "international",
    ),
    (
        "'Sector 44' Gurgaon — should still be India via Gurgaon city name",
        "Job in Sector 44, Gurgaon. Freshers welcome. 15k/month.",
        "india",
    ),
    (
        "No location at all",
        "Freshers needed for BPO voice process. 12th pass / Graduate. Immediate joiners preferred.",
        "unspecified",
    ),
    (
        "indiana (should NOT block as india)",
        "Opening at Indiana University Research Center. Philadelphia campus.",
        "international",
    ),
    (
        "indiamart reference (should still work if city found)",
        "Job at IndiaMart, Noida office. Freshers apply.",
        "india",
    ),
]

results = []
passed = 0
failed = 0

for label, text, expected in SAMPLES:
    data = extractor._extract_location_enhanced(text)
    scope = data.get("geographic_scope", "?")
    cities = data.get("cities", [])
    raw_loc = data.get("raw_location")
    is_remote = data.get("is_remote", False)
    ok = scope == expected
    status = "✅ PASS" if ok else "❌ FAIL"
    if ok:
        passed += 1
    else:
        failed += 1
    results.append({
        "label": label,
        "expected": expected,
        "got": scope,
        "cities": cities,
        "raw_location": raw_loc,
        "is_remote": is_remote,
        "status": status,
    })
    flag = "✅" if ok else "❌"
    print(f"{flag}  [{scope:>14s}]  {label}")
    if not ok:
        print(f"       expected={expected}  cities={cities}  raw_loc={raw_loc}")

print()
print(f"Results: {passed}/{len(SAMPLES)} passed, {failed} failed")

# Write JSON for detailed inspection
out_path = "/tmp/location_logic_verify.json"
with open(out_path, "w") as f:
    json.dump(results, f, indent=2)
print(f"Full results saved → {out_path}")
