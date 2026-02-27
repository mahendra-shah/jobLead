#!/usr/bin/env python3
"""
Add Telegram accounts to database for multi-account scraping
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import model
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.db.session import SyncSessionLocal

# Account configuration from .env
accounts = [
    {
        'phone': '+918826304387',
        'api_id': '22268483',
        'api_hash': '91d20fcc8dda60181199e28343d6b6cb',
        'notes': 'my account'
    },
    # Add more accounts here if you have them
]

print("\n🔧 Adding Telegram Accounts to Database")
print("=" * 80)

db = SyncSessionLocal()

try:
    for idx, account_data in enumerate(accounts, start=1):
        # Check if account already exists
        existing = db.query(TelegramAccount).filter(
            TelegramAccount.phone == account_data['phone']
        ).first()
        
        if existing:
            print(f"⚠️  Account {idx}: {account_data['phone']} already exists (ID: {existing.id})")
        else:
            # Create new account
            account = TelegramAccount(
                phone=account_data['phone'],
                api_id=account_data['api_id'],
                api_hash=account_data['api_hash'],
                is_active=True,
                is_banned=False,
                health_status=HealthStatus.HEALTHY,
                consecutive_errors=0,
                groups_joined_count=0,
                notes=account_data['notes']
            )
            
            db.add(account)
            db.flush()  # Get ID
            
            print(f"✅ Account {idx}: {account_data['phone']} added (ID: {account.id})")
    
    db.commit()
    
    # Show summary
    total = db.query(TelegramAccount).count()
    
    print("=" * 80)
    print(f"\n📊 Total accounts in database: {total}")
    
    if total < 5:
        print(f"⚠️  Recommended: Add {5 - total} more accounts for better load balancing")
        print("   Edit this script and add more phone numbers to the 'accounts' list")
    else:
        print("✅ Good! You have 5+ accounts for multi-account scraping")
    
    print()
    
finally:
    db.close()
