"""Legacy script retained for compatibility.

This column is intentionally removed from the schema.
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.db.session import get_sync_db
from sqlalchemy import text

db = next(get_sync_db())
try:
    result = db.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'jobs'
              AND column_name = 'extraction_completeness_score'
            LIMIT 1
            """
        )
    ).fetchone()
    if result:
        print("⚠️ extraction_completeness_score exists, but this field is deprecated and should be removed via migration.")
    else:
        print("✅ extraction_completeness_score is not present (expected).")
finally:
    db.close()
