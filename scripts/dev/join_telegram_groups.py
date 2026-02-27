#!/usr/bin/env python3
"""
Manual Telegram Group Joiner Script

Joins Telegram channels manually (for testing or on-demand joining).
Uses TelegramGroupJoinerService for consistent logic.

Usage:
    python3 scripts/dev/join_telegram_groups.py

Features:
- Joins 1 channel per account per run
- Early exits if no unjoined channels
- Displays statistics and next steps
"""

import sys
import os
import asyncio

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from app.services.telegram_group_joiner_service import TelegramGroupJoinerService


async def main():
    """Main entry point."""
    print("\n" + "=" * 70)
    print("  🔗 TELEGRAM GROUP JOINER (MANUAL)")
    print("=" * 70)
    print()
    
    try:
        # Create joiner service
        joiner = TelegramGroupJoinerService()
        
        # Run join cycle
        result = await joiner.run_join_cycle()
        
        # Display results
        print()
        print("=" * 70)
        print("  📊 RESULTS")
        print("=" * 70)
        print()
        
        if result["success"]:
            print(f"✅ Status: {result['message']}")
            print()
            
            stats = result["stats"]
            print(f"Successful joins:  {stats['successful_joins']}")
            print(f"Already joined:    {stats['already_joined']}")
            print(f"Failed joins:      {stats['failed_joins']}")
            print(f"Errors:            {len(stats['errors'])}")
            
            if stats['errors']:
                print()
                print("Errors encountered:")
                for error in stats['errors'][:5]:  # Show first 5
                    print(f"  • {error}")
            
        else:
            print(f"❌ Error: {result['message']}")
        
        print()
        print("=" * 70)
        print()
        print("✅ Next Steps:")
        print()
        print("1. Run again to join more channels:")
        print("   python3 scripts/dev/join_telegram_groups.py")
        print()
        print("2. Or wait for scheduler (runs every 5 hours automatically)")
        print()
        print("3. Check join status:")
        print("   python3 scripts/dev/verify_complete_system.py")
        print()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
