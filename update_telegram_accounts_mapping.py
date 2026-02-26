#!/usr/bin/env python3
"""
Script to map joined_by_account_id (1-5) to telegram_account UUIDs in telegram_groups table.
Updates joined_by_phone as well based on account phone numbers.
"""
import asyncio
import sys
from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from app.db.session import AsyncSessionLocal
from app.models.telegram_account import TelegramAccount
from app.models.telegram_group import TelegramGroup


# Account mapping: account_id -> phone_number
# Update these phone numbers for your 5 accounts
ACCOUNT_PHONE_MAPPING = {
    1: "phone number ",  # Replace with actual phone
    2: "phone number",  # Replace with actual phone
    3: "phone number ",      # Your specified phone for account 3
    4: "PHONE_NUMBER_4",  # Replace with actual phone
    5: "PHONE_NUMBER_5",  # Replace with actual phone
}


async def get_or_create_telegram_account(db, phone: str) -> TelegramAccount:
    """Get existing or create new telegram account by phone."""
    result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.phone == phone)
    )
    account = result.scalar_one_or_none()
    
    if not account:
        # Create new account (you'll need to add api_id and api_hash manually later)
        account = TelegramAccount(
            phone=phone,
            api_id="PLACEHOLDER_API_ID",  # Update manually
            api_hash="PLACEHOLDER_API_HASH",  # Update manually
            is_active=True
        )
        db.add(account)
        await db.commit()
        await db.refresh(account)
        print(f"✅ Created TelegramAccount: {phone} (ID: {account.id})")
    else:
        print(f"📱 Found existing TelegramAccount: {phone} (ID: {account.id})")
    
    return account


async def update_telegram_groups_mapping():
    """Update telegram_groups to use telegram_account_id instead of joined_by_account_id."""
    async with AsyncSessionLocal() as db:
        print("\n🔄 Starting telegram groups account mapping update...\n")
        
        # Step 1: Ensure all accounts exist in telegram_accounts
        account_id_to_uuid = {}
        for account_id, phone in ACCOUNT_PHONE_MAPPING.items():
            if phone.startswith("PHONE_NUMBER_"):
                print(f"⚠️  Skipping account {account_id}: Phone number not configured")
                continue
            
            account = await get_or_create_telegram_account(db, phone)
            account_id_to_uuid[account_id] = account.id
        
        if not account_id_to_uuid:
            print("\n❌ No valid phone numbers configured. Update ACCOUNT_PHONE_MAPPING in script.")
            return
        
        print(f"\n📊 Account ID to UUID mapping:")
        for acc_id, uuid in account_id_to_uuid.items():
            print(f"   Account {acc_id} -> {uuid} ({ACCOUNT_PHONE_MAPPING[acc_id]})")
        
        # Step 2: Check if telegram_groups has telegram_account_id column
        result = await db.execute(text(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'telegram_groups' 
            AND column_name = 'telegram_account_id'
            """
        ))
        has_new_column = result.scalar_one_or_none() is not None
        
        if not has_new_column:
            print("\n⚠️  telegram_account_id column doesn't exist yet.")
            print("   Adding column to telegram_groups...")
            await db.execute(text(
                "ALTER TABLE telegram_groups ADD COLUMN IF NOT EXISTS telegram_account_id UUID"
            ))
            await db.commit()
            print("   ✅ Column added")
        
        # Step 3: Update telegram_groups records
        result = await db.execute(
            select(TelegramGroup).where(TelegramGroup.joined_by_account_id.isnot(None))
        )
        groups = result.scalars().all()
        
        print(f"\n📝 Updating {len(groups)} telegram groups...")
        updated_count = 0
        
        for group in groups:
            old_account_id = group.joined_by_account_id
            
            if old_account_id not in account_id_to_uuid:
                print(f"   ⚠️  Group {group.username}: No UUID mapping for account_id={old_account_id}")
                continue
            
            new_uuid = account_id_to_uuid[old_account_id]
            new_phone = ACCOUNT_PHONE_MAPPING[old_account_id]
            
            # Update using raw SQL to set UUID column
            await db.execute(
                text(
                    """
                    UPDATE telegram_groups 
                    SET telegram_account_id = :uuid, 
                        joined_by_phone = :phone
                    WHERE id = :group_id
                    """
                ),
                {"uuid": new_uuid, "phone": new_phone, "group_id": group.id}
            )
            updated_count += 1
            print(f"   ✅ {group.username}: account_id={old_account_id} -> UUID={new_uuid}")
        
        await db.commit()
        
        print(f"\n✨ Update complete! Updated {updated_count} groups.")
        print("\n📋 Summary:")
        print(f"   - Accounts configured: {len(account_id_to_uuid)}")
        print(f"   - Groups updated: {updated_count}")
        print("\n⚠️  Next steps:")
        print("   1. Update PLACEHOLDER_API_ID and PLACEHOLDER_API_HASH in telegram_accounts table")
        print("   2. Run migration to add FK constraint: telegram_account_id -> telegram_accounts(id)")
        print("   3. Optionally drop joined_by_account_id column after verification")


if __name__ == "__main__":
    print("="*70)
    print("Telegram Accounts Mapping Updater")
    print("="*70)
    print("\n⚙️  Configuration:")
    print(f"   Total accounts: {len(ACCOUNT_PHONE_MAPPING)}")
    for aid, phone in ACCOUNT_PHONE_MAPPING.items():
        status = "✅" if not phone.startswith("PHONE_NUMBER_") else "❌"
        print(f"   {status} Account {aid}: {phone}")
    
    print("\n" + "="*70)
    proceed = input("\n🚀 Proceed with update? (yes/no): ").strip().lower()
    
    if proceed != "yes":
        print("❌ Aborted by user.")
        sys.exit(0)
    
    asyncio.run(update_telegram_groups_mapping())
