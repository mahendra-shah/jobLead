#!/bin/bash
"""
Log Cleanup Script
==================
Automatically deletes log files older than 7 days.

Usage:
    bash scripts/cleanup_logs.sh

Cron Setup:
    # Daily at midnight
    0 0 * * * /home/ubuntu/placementdashboard-be/scripts/cleanup_logs.sh
"""

# Set base directory
BASE_DIR="/home/ubuntu/placementdashboard-be"
LOG_DIR="$BASE_DIR/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "$(date): Starting log cleanup..."

# Delete logs older than 7 days
find "$LOG_DIR" -name "*.log" -type f -mtime +7 -delete

# Count remaining log files
LOG_COUNT=$(find "$LOG_DIR" -name "*.log" -type f | wc -l)

echo "$(date): Log cleanup complete. $LOG_COUNT log files remaining."

# Optional: Compress logs older than 1 day but newer than 7 days
find "$LOG_DIR" -name "*.log" -type f -mtime +1 -mtime -7 ! -name "*.gz" -exec gzip {} \;

echo "$(date): Log compression complete."
