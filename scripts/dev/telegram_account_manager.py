#!/usr/bin/env python3
"""
Telegram Account Manager - Complete Account Setup & Login
Supports:
1. Login existing accounts (all or specific)
2. Create and setup new accounts
"""

import asyncio
import sys
import os
import hashlib
import base64
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Database imports
from app.db.session import SyncSessionLocal
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.config import Settings

# Get encryption key from settings
settings = Settings()

# Derive encryption key from SECRET_KEY
def get_encryption_key():
    """Derive Fernet key from SECRET_KEY"""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'telegram_account_salt',  # Fixed salt for consistency
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(settings.SECRET_KEY.encode()))
    return Fernet(key)

cipher = get_encryption_key()

# Encryption functions
def encrypt_credential(credential):
    """Encrypt API credentials"""
    return cipher.encrypt(credential.encode()).decode()


def decrypt_credential(encrypted):
    """Decrypt API credentials"""
    return cipher.decrypt(encrypted.encode()).decode()


def print_header(title):
    """Print formatted header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


def print_section(section_title):
    """Print section divider"""
    print(f"\n{'─' * 70}")
    print(f"  📌 {section_title}")
    print(f"{'─' * 70}\n")


# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def get_all_accounts():
    """Get all active accounts from database"""
    db = SyncSessionLocal()
    try:
        accounts = db.query(TelegramAccount).filter(
            TelegramAccount.is_banned == False
        ).all()
        
        result = []
        for acc in accounts:
            result.append({
                "id": str(acc.id),
                "phone": acc.phone,
                "api_id": acc.api_id,
                "api_hash": acc.api_hash,
                "is_active": acc.is_active,
                "health_status": acc.health_status.value,
                "groups_joined_count": acc.groups_joined_count,
                "created_at": acc.created_at
            })
        
        return result
    finally:
        db.close()


def get_account_by_phone(phone):
    """Get account by phone number"""
    db = SyncSessionLocal()
    try:
        account = db.query(TelegramAccount).filter(
            TelegramAccount.phone == phone
        ).first()
        
        if account:
            return {
                "id": str(account.id),
                "phone": account.phone,
                "api_id": account.api_id,
                "api_hash": account.api_hash,
                "is_active": account.is_active,
                "health_status": account.health_status.value,
                "groups_joined_count": account.groups_joined_count
            }
        return None
    finally:
        db.close()


def create_new_account(phone, api_id, api_hash, notes=""):
    """Create new account in database with hashed credentials"""
    db = SyncSessionLocal()
    try:
        # Check if account already exists
        existing = db.query(TelegramAccount).filter(
            TelegramAccount.phone == phone
        ).first()
        
        if existing:
            print(f"⚠️  Account {phone} already exists!")
            return False
        
        # Encrypt the API credentials before storing
        encrypted_api_id = encrypt_credential(api_id)
        encrypted_api_hash = encrypt_credential(api_hash)
        
        # Store creation info in notes
        temp_notes = notes or f"Created via telegram_account_manager.py on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Create new account with encrypted credentials
        account = TelegramAccount(
            phone=phone,
            api_id=encrypted_api_id,  # Store encrypted
            api_hash=encrypted_api_hash,  # Store encrypted
            is_active=True,
            is_banned=False,
            health_status=HealthStatus.HEALTHY,
            consecutive_errors=0,
            groups_joined_count=0,
            notes=temp_notes
        )
        
        db.add(account)
        db.commit()
        
        print(f"\n✅ Account created successfully!")
        print(f"   📱 Phone: {phone}")
        print(f"   🔐 Credentials encrypted and saved securely")
        print(f"   💾 Saved to database with ID: {account.id}")
        
        return True, api_id, api_hash  # Return plain values for immediate login
    except Exception as e:
        print(f"❌ Error creating account: {e}")
        db.rollback()
        return False, None, None
    finally:
        db.close()


def update_account_status(phone, is_active, health_status):
    """Update account status"""
    db = SyncSessionLocal()
    try:
        account = db.query(TelegramAccount).filter(
            TelegramAccount.phone == phone
        ).first()
        
        if account:
            account.is_active = is_active
            account.health_status = HealthStatus(health_status.lower())
            db.commit()
            return True
        return False
    finally:
        db.close()


# ============================================================================
# TELEGRAM LOGIN FUNCTIONS
# ============================================================================

async def login_account(phone, api_id, api_hash):
    """Login to specific Telegram account"""
    
    print_section(f"Logging in: {phone}")
    
    # Convert api_id to int
    api_id = int(api_id) if isinstance(api_id, str) else api_id
    
    # Create session directory
    os.makedirs("sessions", exist_ok=True)
    
    client = TelegramClient(f"sessions/{phone}", api_id, api_hash)
    
    try:
        print(f"⏳ Connecting to Telegram...\n")
        
        # Check if already connected
        await client.connect()
        
        # Check if already authorized
        if await client.is_user_authorized():
            print(f"✅ Already authenticated for {phone}\n")
            me = await client.get_me()
            print(f"   👤 Name: {me.first_name} {me.last_name or ''}")
            print(f"   📱 Username: @{me.username if me.username else 'N/A'}")
            print(f"   ☎️  Phone: {phone}")
            
            # Update status in DB
            update_account_status(phone, True, "HEALTHY")
            print(f"\n✅ Account marked as HEALTHY in database")
            
            await client.disconnect()
            return True
        
        # Start login process
        await client.start(phone=phone)
        
        # Check if password needed
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Successfully logged in!")
            print(f"   👤 Name: {me.first_name} {me.last_name or ''}")
            print(f"   📱 Username: @{me.username if me.username else 'N/A'}")
            print(f"   ☎️  Phone: {phone}")
            
            # Update status in DB
            update_account_status(phone, True, "HEALTHY")
            print(f"\n✅ Account marked as HEALTHY in database")
            
            await client.disconnect()
            return True
        else:
            print(f"❌ Failed to authorize {phone}")
            await client.disconnect()
            return False
    
    except SessionPasswordNeededError:
        print(f"❌ Two-factor authentication required")
        print(f"   Please try again and enter your 2FA password")
        await client.disconnect()
        return False
    
    except PhoneCodeExpiredError:
        print(f"❌ Verification code expired")
        print(f"   Please request a new code and try again")
        await client.disconnect()
        return False
    
    except Exception as e:
        print(f"❌ Error during login: {e}")
        await client.disconnect()
        return False


async def login_all_accounts(accounts):
    """Login to all accounts one by one"""
    
    if not accounts:
        print(f"❌ No accounts found in database!")
        return
    
    print_header(f"Logging in {len(accounts)} accounts")
    
    success_count = 0
    failed_count = 0
    
    for idx, acc in enumerate(accounts, 1):
        print(f"\n[{idx}/{len(accounts)}] ", end="")
        
        result = await login_account(
            acc['phone'],
            acc['api_id'],
            acc['api_hash']
        )
        
        if result:
            success_count += 1
        else:
            failed_count += 1
        
        if idx < len(accounts):
            print(f"\n⏳ Moving to next account...")
            input("Press Enter to continue...\n")
    
    print_header("Login Summary")
    print(f"✅ Successfully logged in: {success_count}/{len(accounts)}")
    print(f"❌ Failed: {failed_count}/{len(accounts)}")
    print()


async def login_specific_account():
    """Login to a specific account by phone"""
    
    phone = input("\n📱 Enter phone number (with country code, e.g., +919876543210): ").strip()
    
    if not phone.startswith('+'):
        phone = '+' + phone
    
    # Get account from database
    account = get_account_by_phone(phone)
    
    if not account:
        print(f"\n❌ Account {phone} not found in database!")
        print(f"\n💡 Do you want to create a new account? (See Option 2)")
        return
    
    print(f"\n✅ Account found!")
    print(f"   Status: {'🟢 ACTIVE' if account['is_active'] else '🔴 INACTIVE'}")
    print(f"   Health: {account['health_status']}")
    
    # Login to account
    result = await login_account(
        account['phone'],
        account['api_id'],
        account['api_hash']
    )
    
    if result:
        print(f"\n✅ Login successful!")
    else:
        print(f"\n❌ Login failed!")


# ============================================================================
# NEW ACCOUNT SETUP FUNCTIONS
# ============================================================================

def get_api_credentials():
    """Get API credentials from user"""
    
    print_section("Get Telegram API Credentials")
    
    print("📖 Step 1: Go to https://my.telegram.org/apps")
    print("📖 Step 2: Login with your phone number")
    print("📖 Step 3: Click 'API Development Tools'")
    print("📖 Step 4: Create an app (any name/description)")
    print("📖 Step 5: Copy your 'api_id' and 'api_hash'\n")
    
    api_id = input("🔑 Enter your API ID: ").strip()
    
    if not api_id:
        print(f"❌ API ID cannot be empty!")
        return None
    
    api_hash = input("🔐 Enter your API Hash: ").strip()
    
    if not api_hash:
        print(f"❌ API Hash cannot be empty!")
        return None
    
    return {
        "api_id": api_id,
        "api_hash": api_hash
    }


async def setup_new_account():
    """Setup a completely new account"""
    
    print_header("🆕 Setup New Telegram Account")
    
    # Get phone number
    phone = input("📱 Enter phone number (with country code, e.g., +919876543210): ").strip()
    
    if not phone.startswith('+'):
        phone = '+' + phone
    
    # Validate phone format
    if not phone.startswith('+') or len(phone) < 10:
        print(f"❌ Invalid phone number!")
        return
    
    # Check if account already exists
    existing = get_account_by_phone(phone)
    if existing:
        print(f"⚠️  Account {phone} already exists!")
        print(f"   Status: {'🟢 ACTIVE' if existing['is_active'] else '🔴 INACTIVE'}")
        print(f"   Health: {existing['health_status']}")
        return
    
    # Get API credentials
    credentials = get_api_credentials()
    if not credentials:
        return
    
    api_id = credentials['api_id']
    api_hash = credentials['api_hash']
    
    # Create account in database
    print_section("Saving to Database")
    
    result = create_new_account(
        phone=phone,
        api_id=api_id,
        api_hash=api_hash,
        notes=f"Created via telegram_account_manager.py"
    )
    
    if not result[0]:
        return
    
    # Extract plain credentials for login
    _, plain_api_id, plain_api_hash = result
    
    # Ask if user wants to login immediately
    print_section("Next Steps")
    
    login_now = input("\n🔐 Do you want to login to this account now? (yes/no): ").strip().lower()
    
    if login_now in ['yes', 'y']:
        await login_account(phone, plain_api_id, plain_api_hash)
    else:
        print(f"\n💡 To login later, run the script and choose Option 1")
        print(f"   Note: You'll need your API credentials again for login")


# ============================================================================
# MAIN MENU
# ============================================================================

async def main_menu():
    """Main menu interface"""
    
    while True:
        print_header("🤖 Telegram Account Manager")
        
        print("Choose an option:\n")
        print("  1️⃣  LOGIN existing accounts")
        print("       - Login all accounts")
        print("       - Login specific account by phone\n")
        print("  2️⃣  CREATE new account")
        print("       - Add API ID and API Hash")
        print("       - Save to database (encrypted)\n")
        print("  0️⃣  EXIT\n")
        
        choice = input("Enter your choice (0-2): ").strip()
        
        if choice == '0':
            print_header("Goodbye! 👋")
            break
        
        elif choice == '1':
            print_header("🔐 Login Options")
            
            print("Choose login type:\n")
            print("  1. Login ALL accounts (one by one)")
            print("  2. Login SPECIFIC account (by phone number)\n")
            
            login_choice = input("Enter choice (1-2): ").strip()
            
            if login_choice == '1':
                await login_all_accounts()
            
            elif login_choice == '2':
                await login_specific_with_credentials()
            
            else:
                print(f"❌ Invalid choice!")
            
            input("\nPress Enter to return to main menu...")
        
        elif choice == '2':
            await setup_new_account()
            input("\nPress Enter to return to main menu...")
        
        else:
            print(f"❌ Invalid choice! Please try again.\n")
            input("Press Enter to continue...")


async def login_all_accounts():
    """Login to all accounts in database one by one"""
    
    print_header("🔐 Login All Accounts")
    
    # Get all accounts from database
    accounts = get_all_accounts()
    
    if not accounts:
        print("❌ No accounts found in database!")
        print("\n💡 Create a new account using Option 2")
        return
    
    active_accounts = [acc for acc in accounts if acc['is_active']]
    
    print(f"📊 Found {len(accounts)} total accounts, {len(active_accounts)} active\n")
    
    if not active_accounts:
        print("⚠️  No active accounts to login!")
        return
    
    # Show accounts
    print("Accounts to login:")
    for idx, acc in enumerate(active_accounts, 1):
        print(f"  {idx}. {acc['phone']} - {acc['health_status']}")
    print()
    
    confirm = input(f"Login all {len(active_accounts)} accounts? (yes/no): ").strip().lower()
    
    if confirm not in ['yes', 'y']:
        print("❌ Cancelled")
        return
    
    # Login each account
    success_count = 0
    failed_count = 0
    
    for idx, account in enumerate(active_accounts, 1):
        phone = account['phone']
        
        print("\n" + "=" * 70)
        print(f"📱 Account {idx}/{len(active_accounts)}: {phone}")
        print("=" * 70)
        
        try:
            # Decrypt credentials
            api_id = decrypt_credential(account['api_id'])
            api_hash = decrypt_credential(account['api_hash'])
            
            # Login
            result = await login_account(phone, api_id, api_hash)
            
            if result:
                success_count += 1
                print(f"✅ Successfully logged in {phone}")
            else:
                failed_count += 1
                print(f"❌ Failed to login {phone}")
                
        except Exception as e:
            failed_count += 1
            print(f"❌ Error logging in {phone}: {e}")
        
        # Ask to continue (except for last account)
        if idx < len(active_accounts):
            input("\n⏸️  Press Enter to continue to next account...")
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Login Summary")
    print("=" * 70)
    print(f"✅ Successful: {success_count}/{len(active_accounts)}")
    print(f"❌ Failed: {failed_count}/{len(active_accounts)}")
    print()


async def login_specific_with_credentials():
    """Login to specific account using stored encrypted credentials"""
    
    phone = input("\n📱 Enter phone number (with country code, e.g., +919876543210): ").strip()
    
    if not phone.startswith('+'):
        phone = '+' + phone
    
    # Get account from database
    account = get_account_by_phone(phone)
    
    if not account:
        print(f"\n❌ Account {phone} not found in database!")
        print(f"\n💡 Do you want to create a new account? (See Option 2)")
        return
    
    print(f"\n✅ Account found!")
    print(f"   Status: {'🟢 ACTIVE' if account['is_active'] else '🔴 INACTIVE'}")
    print(f"   Health: {account['health_status']}")
    
    # Decrypt stored credentials
    try:
        api_id = decrypt_credential(account['api_id'])
        api_hash = decrypt_credential(account['api_hash'])
        print(f"\n🔓 Credentials decrypted successfully!")
    except Exception as e:
        print(f"\n❌ Failed to decrypt credentials: {e}")
        return
    
    # Login to account with decrypted credentials
    result = await login_account(
        account['phone'],
        api_id,
        api_hash
    )
    
    if result:
        print(f"\n✅ Login successful!")
    else:
        print(f"\n❌ Login failed!")


async def handle_command_line_args():
    """Handle command line arguments for quick login"""
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--login' and len(sys.argv) > 2:
            phone = sys.argv[2]
            account = get_account_by_phone(phone)
            
            if not account:
                print(f"❌ Account {phone} not found in database!")
                sys.exit(1)
            
            result = await login_account(
                account['phone'],
                account['api_id'],
                account['api_hash']
            )
            
            if result:
                print(f"\n✅ Login successful!")
            else:
                print(f"\n❌ Login failed!")
            
            sys.exit(0 if result else 1)


if __name__ == "__main__":
    try:
        # Check for command line arguments
        if len(sys.argv) > 1:
            asyncio.run(handle_command_line_args())
        else:
            # Run interactive menu
            asyncio.run(main_menu())
    
    except KeyboardInterrupt:
        print_header("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
