#!/usr/bin/env python3
"""
Decrypt Telegram Account Credentials
This script shows how to decrypt API credentials stored in the database
"""

import sys
import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Database imports
from app.db.session import SyncSessionLocal
from app.models.telegram_account import TelegramAccount
from app.config import Settings

# Get encryption key from settings
settings = Settings()


def get_encryption_key():
    """
    Derive Fernet key from SECRET_KEY
    
    How it works:
    1. Uses PBKDF2 (Password-Based Key Derivation Function 2) with HMAC-SHA256
    2. Takes your SECRET_KEY from .env file
    3. Applies 100,000 iterations with a fixed salt
    4. Generates a 32-byte key that's then used for Fernet encryption
    
    The fixed salt ensures consistency - you can decrypt later with the same SECRET_KEY
    """
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'telegram_account_salt',  # Fixed salt for consistency
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(settings.SECRET_KEY.encode()))
    return Fernet(key)


cipher = get_encryption_key()


def decrypt_credential(encrypted):
    """
    Decrypt API credentials
    
    Args:
        encrypted (str): The encrypted credential from database
        
    Returns:
        str: The decrypted plain text value
    """
    return cipher.decrypt(encrypted.encode()).decode()


def list_all_accounts():
    """List all accounts with decrypted credentials"""
    
    print("\n" + "=" * 80)
    print("  📱 All Telegram Accounts (Decrypted)")
    print("=" * 80 + "\n")
    
    db = SyncSessionLocal()
    try:
        accounts = db.query(TelegramAccount).all()
        
        if not accounts:
            print("❌ No accounts found in database!")
            return
        
        for idx, acc in enumerate(accounts, 1):
            print(f"Account {idx}:")
            print(f"  📞 Phone: {acc.phone}")
            print(f"  🆔 ID: {acc.id}")
            print(f"  ✅ Active: {acc.is_active}")
            print(f"  🚫 Banned: {acc.is_banned}")
            print(f"  💚 Health: {acc.health_status.value}")
            
            # Decrypt credentials
            try:
                api_id = decrypt_credential(acc.api_id)
                api_hash = decrypt_credential(acc.api_hash)
                
                print(f"  🔑 API ID (decrypted): {api_id}")
                print(f"  🔐 API Hash (decrypted): {api_hash}")
            except Exception as e:
                print(f"  ❌ Error decrypting credentials: {e}")
            
            print(f"  📝 Notes: {acc.notes or 'N/A'}")
            print(f"  📅 Created: {acc.created_at}")
            print()
        
    finally:
        db.close()


def get_account_by_phone(phone):
    """Get specific account credentials by phone"""
    
    print("\n" + "=" * 80)
    print(f"  📱 Account Details: {phone}")
    print("=" * 80 + "\n")
    
    db = SyncSessionLocal()
    try:
        account = db.query(TelegramAccount).filter(
            TelegramAccount.phone == phone
        ).first()
        
        if not account:
            print(f"❌ Account {phone} not found in database!")
            return
        
        print(f"  📞 Phone: {account.phone}")
        print(f"  🆔 ID: {account.id}")
        print(f"  ✅ Active: {account.is_active}")
        print(f"  🚫 Banned: {account.is_banned}")
        print(f"  💚 Health: {account.health_status.value}")
        
        # Decrypt credentials
        try:
            api_id = decrypt_credential(account.api_id)
            api_hash = decrypt_credential(account.api_hash)
            
            print(f"  🔑 API ID (decrypted): {api_id}")
            print(f"  🔐 API Hash (decrypted): {api_hash}")
        except Exception as e:
            print(f"  ❌ Error decrypting credentials: {e}")
        
        print(f"  📝 Notes: {account.notes or 'N/A'}")
        print(f"  📅 Created: {account.created_at}")
        print()
        
    finally:
        db.close()


def show_encryption_info():
    """Show information about the encryption mechanism"""
    
    print("\n" + "=" * 80)
    print("  🔐 Encryption Information")
    print("=" * 80 + "\n")
    
    print("📚 Encryption Method:")
    print("  - Algorithm: Fernet (symmetric encryption)")
    print("  - Based on: AES-128 in CBC mode with HMAC for authentication")
    print()
    
    print("🔑 Key Derivation:")
    print("  - Method: PBKDF2-HMAC-SHA256")
    print("  - Iterations: 100,000")
    print("  - Salt: 'telegram_account_salt' (fixed)")
    print("  - Source: SECRET_KEY from .env file")
    print()
    
    print("🔒 Security Features:")
    print("  - Authenticated encryption (prevents tampering)")
    print("  - Key derived from your SECRET_KEY using PBKDF2")
    print("  - Same SECRET_KEY required to decrypt")
    print()
    
    print("⚠️  Important:")
    print("  - Keep your SECRET_KEY safe and backed up")
    print("  - If you lose SECRET_KEY, encrypted data cannot be recovered")
    print("  - Do not share encrypted values publicly (they can be decrypted with your key)")
    print()


def main():
    """Main menu"""
    
    if len(sys.argv) > 1:
        # Command line mode
        if sys.argv[1] == '--phone' and len(sys.argv) > 2:
            get_account_by_phone(sys.argv[2])
        elif sys.argv[1] == '--all':
            list_all_accounts()
        elif sys.argv[1] == '--info':
            show_encryption_info()
        else:
            print("Usage:")
            print("  python3 decrypt_telegram_credentials.py --all")
            print("  python3 decrypt_telegram_credentials.py --phone +919876543210")
            print("  python3 decrypt_telegram_credentials.py --info")
    else:
        # Interactive mode
        while True:
            print("\n" + "=" * 80)
            print("  🔐 Telegram Credentials Decryption Tool")
            print("=" * 80 + "\n")
            
            print("Choose an option:\n")
            print("  1️⃣  Show ALL accounts (with decrypted credentials)")
            print("  2️⃣  Show SPECIFIC account by phone")
            print("  3️⃣  Show encryption info")
            print("  0️⃣  EXIT\n")
            
            choice = input("Enter your choice (0-3): ").strip()
            
            if choice == '0':
                print("\n" + "=" * 80)
                print("  Goodbye! 👋")
                print("=" * 80 + "\n")
                break
            
            elif choice == '1':
                list_all_accounts()
                input("Press Enter to continue...")
            
            elif choice == '2':
                phone = input("\n📱 Enter phone number (with +): ").strip()
                if not phone.startswith('+'):
                    phone = '+' + phone
                get_account_by_phone(phone)
                input("Press Enter to continue...")
            
            elif choice == '3':
                show_encryption_info()
                input("Press Enter to continue...")
            
            else:
                print("❌ Invalid choice!")
                input("Press Enter to continue...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + "=" * 80)
        print("  Interrupted by user")
        print("=" * 80 + "\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
