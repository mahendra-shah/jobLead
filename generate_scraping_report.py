#!/usr/bin/env python3
"""
Generate comprehensive scraping report
"""
import json
import sys

# Read the JSON output from stdin or file
if len(sys.argv) > 1:
    with open(sys.argv[1], 'r') as f:
        data = json.load(f)
else:
    data = json.load(sys.stdin)

print("\n" + "="*100)
print("📊 TELEGRAM SCRAPING REPORT")
print("="*100)

# Overall Summary
print(f"\n🎯 OVERALL SUMMARY:")
print(f"   Total Channels Attempted: {data['total_channels']}")
print(f"   ✅ Successful: {data['successful']}")
print(f"   ❌ Failed: {data['failed']}")
print(f"   📨 Total Messages Fetched: {data['total_messages']}")
print(f"   ⏱️  Duration: {data['duration_seconds']:.2f} seconds")
print(f"   Success Rate: {(data['successful']/data['total_channels']*100) if data['total_channels'] > 0 else 0:.1f}%")

# Account Stats
print(f"\n👥 ACCOUNT PERFORMANCE:")
print(f"{'Account':<12} {'Channels':<10} {'Messages':<10} {'Rate Limits':<12} {'Errors':<8}")
print("-" * 60)
for account_id, stats in data['account_stats'].items():
    print(f"Account {account_id:<5} {stats['channels_scraped']:<10} {stats['messages_found']:<10} {stats['rate_limits']:<12} {stats['errors']:<8}")

# Error Analysis
if data['failed'] > 0 and 'results' in data:
    print(f"\n❌ ERROR ANALYSIS:")
    error_types = {}
    for result in data['results']:
        if not result['success'] and result.get('error'):
            error = result['error']
            # Categorize error
            if 'Session file not found' in error:
                error_type = 'Missing Session File'
            elif 'ChannelPrivateError' in error or 'private' in error.lower():
                error_type = 'Private Channel'
            elif 'UsernameInvalidError' in error or 'invalid' in error.lower():
                error_type = 'Invalid Username'
            elif 'FloodWait' in error or 'rate limit' in error.lower():
                error_type = 'Rate Limited'
            elif 'AuthKeyError' in error or 'banned' in error.lower():
                error_type = 'Account Banned'
            else:
                error_type = 'Other'
            
            error_types[error_type] = error_types.get(error_type, 0) + 1
    
    print(f"{'Error Type':<30} {'Count':<10} {'%':<10}")
    print("-" * 50)
    for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / data['failed'] * 100) if data['failed'] > 0 else 0
        print(f"{error_type:<30} {count:<10} {percentage:.1f}%")

# Success Stories
if data['successful'] > 0 and 'results' in data:
    print(f"\n✅ SUCCESSFUL CHANNELS (Top 10):")
    successful = [r for r in data['results'] if r['success'] and r['messages_fetched'] > 0]
    successful.sort(key=lambda x: x['messages_fetched'], reverse=True)
    
    print(f"{'Channel':<30} {'Account':<10} {'Messages':<10}")
    print("-" * 50)
    for result in successful[:10]:
        print(f"{result['channel']:<30} Acc {result['account_id']:<7} {result['messages_fetched']:<10}")

# Recommendations
print(f"\n💡 RECOMMENDATIONS:")
if data['total_messages'] == 0:
    print("   ⚠️  No messages fetched - check session files and account authentication")
elif data['successful'] < data['total_channels'] * 0.5:
    print("   ⚠️  Low success rate - review error types above")
else:
    print("   ✅ Scraping is working well!")

if any(stats['rate_limits'] > 0 for stats in data['account_stats'].values()):
    print("   ⚠️  Rate limits detected - system will auto-handle with delays")

print("\n" + "="*100 + "\n")
