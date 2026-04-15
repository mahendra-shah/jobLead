#!/usr/bin/env python3
"""
Set all channels to initial health score of 100
For first-time scraping to ensure all channels get attempted
"""
import os
import sys
from sqlalchemy import create_engine, text

# Database URL from environment
db_url = os.getenv('LOCAL_DATABASE_URL', 'postgresql://neondb_owner:yourdbcred/placement_db?sslmode=require')

engine = create_engine(db_url)

print("\n🔧 Setting all channels to health_score = 100.0")
print("=" * 80)

with engine.connect() as conn:
    # Get current count
    result = conn.execute(text("SELECT COUNT(*) FROM telegram_groups;"))
    total = result.scalar()
    print(f"Total channels in database: {total}\n")
    
    # Update all to 100
    result = conn.execute(text("""
        UPDATE telegram_groups 
        SET health_score = 100.0,
            last_score_update = NOW()
        WHERE health_score != 100.0;
    """))
    conn.commit()
    
    updated = result.rowcount
    print(f"✅ Updated {updated} channels to health_score = 100.0")
    
    # Verify
    result = conn.execute(text("""
        SELECT 
            COUNT(*) as total,
            MIN(health_score) as min_score,
            MAX(health_score) as max_score,
            AVG(health_score) as avg_score
        FROM telegram_groups;
    """))
    
    row = result.fetchone()
    print(f"\n📊 Verification:")
    print(f"   Total channels: {row.total}")
    print(f"   Min score: {row.min_score:.2f}")
    print(f"   Max score: {row.max_score:.2f}")
    print(f"   Avg score: {row.avg_score:.2f}")
    print("\n✅ All channels ready for first-time scraping!\n")
