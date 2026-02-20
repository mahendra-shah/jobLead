"""
Login Telegram Accounts
This script helps you authenticate your Telegram accounts using credentials from database.
Run once for each account.
"""
import asyncio
from telethon import TelegramClient
import sys
from app.db.session import SyncSessionLocal
from app.models.telegram_account import TelegramAccount

# Load accounts from database with their actual API credentials
def get_accounts_from_db():
    """Get accounts from database with their API credentials"""
    db = SyncSessionLocal()
    accounts = db.query(TelegramAccount).filter(
        TelegramAccount.is_active == True,
        TelegramAccount.is_banned == False
    ).all()
    
    result = []
    for acc in accounts:
        result.append({
            "phone": acc.phone,
            "api_id": acc.api_id,
            "api_hash": acc.api_hash,
            "id": acc.id
        })
    
    db.close()
    return result

# Get accounts from database
ACCOUNTS = get_accounts_from_db()

async def login_account(phone, api_id, api_hash):
    """Login to Telegram account"""
    print("=" * 60)
    print(f"🔐 Logging in: {phone}")
    print("=" * 60)
    
    # Convert api_id to int if it's a string
    api_id = int(api_id) if isinstance(api_id, str) else api_id
    
    client = TelegramClient(f"sessions/{phone}", api_id, api_hash)
    
    try:
        await client.start(phone=phone)
        
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Successfully logged in!")
            print(f"   Name: {me.first_name} {me.last_name or ''}")
            print(f"   Username: @{me.username if me.username else 'N/A'}")
            print(f"   Phone: {phone}")
            print()
        
        await client.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        await client.disconnect()
        return False

async def main():
    """Login all accounts"""
    print()
    print("=" * 60)
    print("🤖 Telegram Account Login Tool")
    print("=" * 60)
    print()
    print("This will login each of your 5 Telegram accounts.")
    print("You'll need to enter the verification code sent to each phone.")
    print()
    
    # Ask which account to login
    print("Select account to login:")
    for i, acc in enumerate(ACCOUNTS, 1):
        print(f"  {i}. {acc['phone']}")
    print(f"  {len(ACCOUNTS) + 1}. Login ALL accounts (one by one)")
    print()
    
    choice = input("Enter choice (1-6): ").strip()
    
    if choice == str(len(ACCOUNTS) + 1):
        # Login all
        for i, acc in enumerate(ACCOUNTS, 1):
            print()
            print(f"📱 Account {i}/{len(ACCOUNTS)}")
            await login_account(acc['phone'], acc['api_id'], acc['api_hash'])
            if i < len(ACCOUNTS):
                print()
                input("Press Enter to continue to next account...")
    else:
        # Login single account
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(ACCOUNTS):
                acc = ACCOUNTS[idx]
                await login_account(acc['phone'], acc['api_id'], acc['api_hash'])
            else:
                print("❌ Invalid choice")
        except ValueError:
            print("❌ Invalid input")
    
    print()
    print("=" * 60)
    print("✅ Login process complete!")
    print("=" * 60)
    print()
    print("Next step: Run the import script with --join flag:")
    print("  python3 scripts/import_channels_from_json.py --join")
    print()

if __name__ == "__main__":
    asyncio.run(main())
