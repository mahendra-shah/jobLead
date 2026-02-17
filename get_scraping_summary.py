#!/usr/bin/env python3
"""Quick summary of scraping results"""
import os
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# Convert postgresql:// to postgresql+asyncpg:// and remove sslmode (asyncpg uses ssl=require)
db_url = os.getenv('LOCAL_DATABASE_URL', 'postgresql://yourdb?sslmode=require')
db_url = db_url.replace('postgresql://', 'postgresql+asyncpg://').replace('sslmode=require', 'ssl=require')

engine = create_async_engine(db_url, echo=False)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_summary():
    async with async_session() as session:
        # Get total channels
        result = await session.execute(text("SELECT COUNT(*) FROM telegram_groups"))
        total_channels = result.scalar()
        
        # Get channels by status
        result = await session.execute(text("""
            SELECT is_active, is_joined, COUNT(*) as count 
            FROM telegram_groups 
            GROUP BY is_active, is_joined
        """))
        statusinfo = [f"Active={row[0]} Joined={row[1]}: {row[2]}" for row in result]
        
        # Get active channels (recently scraped)
        result = await session.execute(text("""
            SELECT COUNT(*) FROM telegram_groups 
            WHERE last_scraped_at > NOW() - INTERVAL '1 hour'
        """))
        recently_scraped = result.scalar()
        
        # Get message counts
        result = await session.execute(text("""
            SELECT COUNT(*) FROM jobs 
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """))
        recent_messages = result.scalar()
        
        # Get all time job count
        result = await session.execute(text("SELECT COUNT(*) FROM jobs"))
        total_jobs = result.scalar()
        
        # Get account statuses
        result = await session.execute(text("""
            SELECT phone, health_status, consecutive_errors, last_used_at 
            FROM telegram_accounts 
            WHERE is_active = true
            ORDER BY phone
        """))
        accounts = result.fetchall()
        
        print("\n" + "="*80)
        print("📊 TELEGRAM SCRAPING SUMMARY")
        print("="*80)
        
        print(f"\n📱 CHANNELS (Total: {total_channels}):")
        for status_info in statusinfo:
            print(f"   {status_info}")
        print(f"   Recently scraped (1h): {recently_scraped}")
        
        print(f"\n💬 MESSAGES:")
        print(f"   Total jobs in database: {total_jobs}")
        print(f"   Fetched (last 1 hour): {recent_messages}")
        
        print(f"\n🤖 TELEGRAM ACCOUNTS ({len(accounts)}):")
        for phone, health, errors, last_used in accounts:
            last = last_used.strftime("%H:%M:%S") if last_used else "Never"
            print(f"   {phone:<18} | {health or 'N/A':<10} | Errors: {errors or 0:<3} | Last: {last}")
        
        print("\n" + "="*80)
        
        if recent_messages > 0:
            print(f"✅ SUCCESS! Scraped {recent_messages} messages in the last hour")
        elif total_jobs > 0:
            print(f"✅ Database has {total_jobs} jobs total, but none in last hour")
        else:
            print("⚠️  No messages scraped yet")
        
        print("="*80 + "\n")

asyncio.run(get_summary())
