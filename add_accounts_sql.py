#!/usr/bin/env python3
"""
Add Telegram accounts directly via SQL (avoiding ORM enum issues)
"""
import os
from sqlalchemy import create_engine, text

db_url = os.getenv('LOCAL_DATABASE_URL', 'postgresql://yourdb')

engine = create_engine(db_url)

# Real Telegram account configuration with session file mapping
accounts = [
    {
        'name': 'Account 1',
        'phone': '+919100000000',
        'api_id': 24242582,
        'api_hash': 'ddjsfhsd76sfj3476ms7dfnse40ac48c1669',
        'session_name': 'session_account1'
    },
    {
        'name': 'Account 2',
        'phone': '+919000000000',
        'api_id': 23717746,
        'api_hash': '23f3b527b36bf2443jdf782jsdfs270',
        'session_name': 'session_account2'
    }
]

print("\n🔧 Adding Telegram Accounts to Database")
print("=" * 80)
print(f"Total accounts to add: {len(accounts)}\n")

with engine.connect() as conn:
    added_count = 0
    exists_count = 0
    
    for idx, account in enumerate(accounts, start=1):
        # Check if exists
        result = conn.execute(
            text("SELECT id FROM telegram_accounts WHERE phone = :phone"),
            {'phone': account['phone']}
        )
        
        if result.fetchone():
            print(f"⚠️  {account['name']} ({account['phone']}) already exists - skipped")
            exists_count += 1
        else:
            # Insert with SQL (lowercase 'healthy' for enum)
            # Build notes with account name and session mapping for tracking
            notes = f"{account['name']} - Session: {account['session_name']}"
            
            conn.execute(text("""
                INSERT INTO telegram_accounts 
                (id, phone, api_id, api_hash, is_active, is_banned, health_status, consecutive_errors, groups_joined_count, notes)
                VALUES 
                (gen_random_uuid(), :phone, :api_id, :api_hash, true, false, 'healthy', 0, 0, :notes)
            """), {
                'phone': account['phone'],
                'api_id': str(account['api_id']),
                'api_hash': account['api_hash'],
                'notes': notes
            })
            conn.commit()
            print(f"✅ {account['name']} ({account['phone']}) - {account['session_name']} - ADDED")
            added_count += 1
    
    # Summary
    result = conn.execute(text("SELECT COUNT(*) FROM telegram_accounts;"))
    total = result.scalar()
    
    print("\n" + "=" * 80)
    print(f"📊 SUMMARY:")
    print(f"   ✅ Newly added: {added_count}")
    print(f"   ⚠️  Already existed: {exists_count}")
    print(f"   📱 Total accounts in database: {total}")
    
    if total >= 5:
        print(f"\n✅ EXCELLENT! You have {total} accounts for multi-account scraping")
        print("   System ready for distributed telegram scraping with ban prevention")
    else:
        print(f"\n⚠️  Only {total} accounts. Recommended: 5 accounts for optimal load balancing")
    
    print("\n" + "=" * 80)
    print("\n🎯 Next Steps:")
    print("   1. Verify accounts: python check_accounts.py")
    print("   2. Test scraping: curl -X POST http://localhost:8000/api/telegram-scraper/scrape/trigger")
    print("   3. Monitor: curl http://localhost:8000/api/visibility/dashboard\n")
