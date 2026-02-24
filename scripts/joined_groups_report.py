#!/usr/bin/env python3
"""
Report: Show joined groups statistics per account
"""

import sys
import asyncio
from pathlib import Path
from tabulate import tabulate

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.models.telegram_group import TelegramGroup
from app.models.telegram_account import TelegramAccount


async def generate_report():
    """Generate report of joined groups per account"""
    async with AsyncSessionLocal() as session:
        print("=" * 80)
        print("📊 TELEGRAM GROUPS JOINED REPORT")
        print("=" * 80)
        print()
        
        # Get total groups
        result = await session.execute(select(func.count(TelegramGroup.id)))
        total_groups = result.scalar()
        
        result = await session.execute(
            select(func.count(TelegramGroup.id))
            .where(TelegramGroup.is_joined == True)
        )
        joined_groups = result.scalar()
        
        unjoined_groups = total_groups - joined_groups
        
        print(f"📈 OVERALL STATS:")
        print(f"   Total groups in database: {total_groups}")
        print(f"   ✅ Joined groups: {joined_groups}")
        print(f"   ❌ Unjoined groups: {unjoined_groups}")
        print(f"   📊 Join rate: {(joined_groups/total_groups*100):.1f}%")
        print()
        
        # Get stats per account
        result = await session.execute(
            select(
                TelegramGroup.joined_by_account_id,
                TelegramGroup.joined_by_phone,
                func.count(TelegramGroup.id).label('count')
            )
            .where(TelegramGroup.is_joined == True)
            .group_by(TelegramGroup.joined_by_account_id, TelegramGroup.joined_by_phone)
            .order_by(TelegramGroup.joined_by_account_id)
        )
        
        rows = result.all()
        
        print("=" * 80)
        print("📱 GROUPS PER ACCOUNT:")
        print("=" * 80)
        print()
        
        if rows:
            table_data = []
            total_by_account = 0
            
            for account_id, phone, count in rows:
                table_data.append([
                    f"Account {account_id}",
                    phone if phone else "N/A",
                    count,
                    f"{(count/joined_groups*100):.1f}%" if joined_groups > 0 else "0%"
                ])
                total_by_account += count
            
            # Add total row
            table_data.append([
                "TOTAL",
                "",
                total_by_account,
                "100.0%"
            ])
            
            headers = ["Account", "Phone Number", "Groups Joined", "% of Total"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
            print()
        else:
            print("⚠️  No joined groups found!")
            print()
        
        # Get all accounts info
        print("=" * 80)
        print("🔐 ALL TELEGRAM ACCOUNTS:")
        print("=" * 80)
        print()
        
        result = await session.execute(
            select(TelegramAccount)
            .order_by(TelegramAccount.phone)
        )
        accounts = result.scalars().all()
        
        if accounts:
            table_data = []
            for idx, acc in enumerate(accounts[:5], 1):
                # Count groups joined by this account
                result = await session.execute(
                    select(func.count(TelegramGroup.id))
                    .where(
                        TelegramGroup.is_joined == True,
                        TelegramGroup.joined_by_account_id == idx
                    )
                )
                count = result.scalar() or 0
                
                status = "✅ Active" if acc.is_active else "❌ Inactive"
                banned = "🚫 BANNED" if acc.is_banned else "✅ OK"
                
                table_data.append([
                    f"Account {idx}",
                    acc.phone,
                    count,
                    status,
                    banned,
                    acc.health_status or "unknown"
                ])
            
            headers = ["Account", "Phone", "Groups Joined", "Status", "Banned", "Health"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
            print()
        
        # Show sample groups for Account 1
        print("=" * 80)
        print("📋 SAMPLE GROUPS JOINED BY ACCOUNT 1:")
        print("=" * 80)
        print()
        
        result = await session.execute(
            select(TelegramGroup)
            .where(
                TelegramGroup.is_joined == True,
                TelegramGroup.joined_by_account_id == 1
            )
            .limit(10)
        )
        
        sample_groups = result.scalars().all()
        
        if sample_groups:
            table_data = []
            for group in sample_groups:
                table_data.append([
                    f"@{group.username}",
                    group.title or "N/A",
                    group.category,
                    group.joined_by_phone or "N/A",
                    group.joined_at.strftime("%Y-%m-%d %H:%M") if group.joined_at else "N/A"
                ])
            
            headers = ["Username", "Title", "Category", "Phone", "Joined At"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
            print()
            
            # Show total for Account 1
            result = await session.execute(
                select(func.count(TelegramGroup.id))
                .where(
                    TelegramGroup.is_joined == True,
                    TelegramGroup.joined_by_account_id == 1
                )
            )
            total_account_1 = result.scalar() or 0
            print(f"📊 Total groups joined by Account 1: {total_account_1}")
            print()
        else:
            print("❌ No groups joined by Account 1 yet")
            print()
        
        print("=" * 80)


def main():
    """Main entry point"""
    asyncio.run(generate_report())


if __name__ == "__main__":
    main()
