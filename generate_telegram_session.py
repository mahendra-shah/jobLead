"""
Generate Telegram Session String for Lambda
This script authenticates with Telegram and generates a session string
that can be used in Lambda environment variables.
"""

import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
import os
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')


async def generate_session():
    """Generate and save Telegram session string."""
    print("=" * 60)
    print("🔐 Telegram Session Generator for Lambda")
    print("=" * 60)
    print()
    
    if not all([API_ID, API_HASH, PHONE]):
        print("❌ Missing Telegram credentials in .env file!")
        print("   Required: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
        return
    
    print(f"📱 Phone: {PHONE}")
    print(f"🔑 API ID: {API_ID}")
    print()
    print("⏳ Connecting to Telegram...")
    print()
    
    # Create client with StringSession
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    await client.start(phone=PHONE)
    
    # Check if connected
    if await client.is_user_authorized():
        session_string = client.session.save()
        
        print()
        print("=" * 60)
        print("✅ SUCCESS! Telegram Authentication Complete!")
        print("=" * 60)
        print()
        print("📋 Your Session String (save this securely):")
        print()
        print("─" * 60)
        print(session_string)
        print("─" * 60)
        print()
        print("📝 Next Steps:")
        print()
        print("1. Copy the session string above")
        print()
        print("2. Run this command to update Lambda:")
        print()
        print(f"   export TELEGRAM_SESSION_STRING='{session_string}'")
        print()
        print("   aws lambda update-function-configuration \\")
        print("     --function-name placement-telegram-scraper \\")
        print('     --environment "Variables={')
        mongo_uri = os.getenv("MONGODB_URI")
        print(f'       MONGODB_URI={mongo_uri},')
        print('       MONGODB_DATABASE=placement_db,')
        print(f'       TELEGRAM_API_ID={API_ID},')
        print(f'       TELEGRAM_API_HASH={API_HASH},')
        print(f'       TELEGRAM_PHONE={PHONE},')
        print(f'       TELEGRAM_SESSION_STRING={session_string}')
        print('     }" \\')
        print('     --region ap-south-1')
        print()
        print("3. Redeploy Lambda 2 with updated code (I'll help)")
        print()
        print("=" * 60)
        
        # Save to file for easy access
        with open('.telegram_session_string', 'w') as f:
            f.write(session_string)
        print("💾 Session string also saved to: .telegram_session_string")
        print()
    else:
        print("❌ Failed to authorize. Please try again.")
    
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(generate_session())
