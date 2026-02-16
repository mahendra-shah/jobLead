"""Add missing extraction_completeness_score column"""
import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.db.session import get_sync_db
from sqlalchemy import text

db = next(get_sync_db())
try:
    db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS extraction_completeness_score FLOAT"))
    db.commit()
    print("✅ Added extraction_completeness_score column")
except Exception as e:
    print(f"Error: {e}")
    db.rollback()
finally:
    db.close()
