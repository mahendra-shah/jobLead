#!/bin/bash

# 🕐 EC2 Cron Setup for ML Pipeline
# Run this ON EC2 to set up automated ML processing

set -e

LOG_FILE="/home/ubuntu/ml_pipeline.log"
ERROR_LOG="/home/ubuntu/ml_pipeline_errors.log"
SCRIPT_FILE="/home/ubuntu/run_ml_pipeline.sh"
PROJECT_DIR="/home/ubuntu/jobLead"

echo "=================================================================================================="
echo "🕐 Setting up ML Pipeline Cron Job on EC2"
echo "=================================================================================================="
echo ""

# Check if running on EC2
if [ ! -d "$PROJECT_DIR" ]; then
    echo "❌ Error: Project directory not found: $PROJECT_DIR"
    echo "   This script should be run ON the EC2 instance"
    exit 1
fi

echo "Step 1: Creating ML pipeline runner script..."

cat > $SCRIPT_FILE << 'EOF'
#!/bin/bash

set -e

LOG_FILE="/home/ubuntu/ml_pipeline.log"
ERROR_LOG="/home/ubuntu/ml_pipeline_errors.log"
PROJECT_DIR="/home/ubuntu/jobLead"
MAX_LOG_SIZE=10485760  # 10MB

# Rotate logs if too large
if [ -f "$LOG_FILE" ] && [ $(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0) -gt $MAX_LOG_SIZE ]; then
    mv "$LOG_FILE" "$LOG_FILE.old"
    echo "Log rotated at $(date)" > "$LOG_FILE"
fi

echo "========================================" >> $LOG_FILE
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ML Pipeline" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

cd $PROJECT_DIR || {
    echo "[$(date)] ERROR: Could not cd to $PROJECT_DIR" >> $ERROR_LOG
    exit 1
}

# Check Docker
if ! docker ps > /dev/null 2>&1; then
    echo "[$(date)] ERROR: Docker not running" >> $ERROR_LOG
    exit 1
fi

# Run ML pipeline (direct execution - no Celery)
docker-compose exec -T backend python scripts/run_ml_pipeline.py >> $LOG_FILE 2>> $ERROR_LOG

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ ML Pipeline completed successfully" >> $LOG_FILE
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ML Pipeline failed (exit code: $EXIT_CODE)" >> $ERROR_LOG
fi

echo "" >> $LOG_FILE
EOF

chmod +x $SCRIPT_FILE
echo "✅ Created: $SCRIPT_FILE"
echo ""

echo "Step 2: Testing the script..."
$SCRIPT_FILE
echo ""

if [ $? -eq 0 ]; then
    echo "✅ Test run successful! Check logs:"
    tail -20 $LOG_FILE
    echo ""
else
    echo "❌ Test run failed! Check error log:"
    tail -20 $ERROR_LOG
    exit 1
fi

echo ""
echo "Step 3: Setting up cron job..."
echo "   Choose schedule:"
echo "   1) Daily at 6:00 AM IST (0:30 AM UTC)"
echo "   2) Every 4 hours"
echo "   3) Custom schedule"
echo ""

read -p "Enter choice (1-3): " CRON_CHOICE

case $CRON_CHOICE in
    1)
        CRON_SCHEDULE="30 0 * * *"
        CRON_DESC="Daily at 6:00 AM IST"
        ;;
    2)
        CRON_SCHEDULE="0 */4 * * *"
        CRON_DESC="Every 4 hours"
        ;;
    3)
        read -p "Enter cron schedule (e.g., '30 0 * * *'): " CRON_SCHEDULE
        CRON_DESC="Custom: $CRON_SCHEDULE"
        ;;
    *)
        echo "Invalid choice. Using default: Daily at 6:00 AM IST"
        CRON_SCHEDULE="30 0 * * *"
        CRON_DESC="Daily at 6:00 AM IST"
        ;;
esac

echo ""
echo "Adding cron job: $CRON_DESC"
echo "Schedule: $CRON_SCHEDULE $SCRIPT_FILE"
echo ""

# Add to crontab
(crontab -l 2>/dev/null | grep -v "$SCRIPT_FILE"; echo "$CRON_SCHEDULE $SCRIPT_FILE") | crontab -

echo "✅ Cron job added"
echo ""

echo "Step 4: Verifying cron setup..."
echo "Current crontab:"
crontab -l | grep -A 1 "$SCRIPT_FILE" || echo "No matching entry found"
echo ""

# Check cron service
if systemctl is-active --quiet cron; then
    echo "✅ Cron service is running"
else
    echo "⚠️  Cron service is not running. Starting..."
    sudo systemctl start cron
    sudo systemctl enable cron
    echo "✅ Cron service started and enabled"
fi

echo ""
echo "=================================================================================================="
echo "✅ ML Pipeline Cron Setup Complete!"
echo "=================================================================================================="
echo ""
echo "📋 Summary:"
echo "   Script: $SCRIPT_FILE"
echo "   Schedule: $CRON_DESC"
echo "   Log file: $LOG_FILE"
echo "   Error log: $ERROR_LOG"
echo ""
echo "📝 Useful Commands:"
echo ""
echo "   # Run manually"
echo "   $SCRIPT_FILE"
echo ""
echo "   # View logs"
echo "   tail -f $LOG_FILE"
echo ""
echo "   # View errors"
echo "   tail -f $ERROR_LOG"
echo ""
echo "   # Check cron jobs"
echo "   crontab -l"
echo ""
echo "   # Edit cron jobs"
echo "   crontab -e"
echo ""
echo "   # Check cron service"
echo "   sudo systemctl status cron"
echo ""
echo "   # View cron execution in syslog"
echo "   grep CRON /var/log/syslog | tail -20"
echo ""
echo "=================================================================================================="
echo ""
echo "🎉 Setup complete! The ML pipeline will run automatically."
echo ""
