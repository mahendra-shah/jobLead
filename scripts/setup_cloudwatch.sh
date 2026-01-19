#!/bin/bash
# Setup CloudWatch Events (EventBridge) triggers for Lambda functions
# Usage: ./setup_cloudwatch.sh

set -e

REGION="${AWS_REGION:-ap-south-1}"

echo "========================================="
echo "Setting up CloudWatch Events"
echo "========================================="
echo "Region: $REGION"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Lambda 1: Group Joiner - Daily at 10 AM IST (4:30 AM UTC)
echo -e "${YELLOW}Setting up schedule for Group Joiner...${NC}"

aws events put-rule \
    --name placement-group-joiner-daily \
    --schedule-expression "cron(30 4 * * ? *)" \
    --state ENABLED \
    --description "Run Group Joiner daily at 10 AM IST" \
    --region $REGION

aws lambda add-permission \
    --function-name placement-group-joiner \
    --statement-id AllowEventBridgeInvoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:$(aws sts get-caller-identity --query Account --output text):rule/placement-group-joiner-daily" \
    --region $REGION 2>/dev/null || echo "  Permission already exists"

aws events put-targets \
    --rule placement-group-joiner-daily \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:$(aws sts get-caller-identity --query Account --output text):function:placement-group-joiner" \
    --region $REGION

echo -e "${GREEN}✓ Group Joiner scheduled (daily at 10 AM IST)${NC}"
echo ""

# Lambda 2: Message Scraper - Every 2 hours
echo -e "${YELLOW}Setting up schedule for Message Scraper...${NC}"

aws events put-rule \
    --name placement-message-scraper-2hourly \
    --schedule-expression "rate(2 hours)" \
    --state ENABLED \
    --description "Run Message Scraper every 2 hours" \
    --region $REGION

aws lambda add-permission \
    --function-name placement-message-scraper \
    --statement-id AllowEventBridgeInvoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:$(aws sts get-caller-identity --query Account --output text):rule/placement-message-scraper-2hourly" \
    --region $REGION 2>/dev/null || echo "  Permission already exists"

aws events put-targets \
    --rule placement-message-scraper-2hourly \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:$(aws sts get-caller-identity --query Account --output text):function:placement-message-scraper" \
    --region $REGION

echo -e "${GREEN}✓ Message Scraper scheduled (every 2 hours)${NC}"
echo ""

# Lambda 3: Job Processor - Every 30 minutes
echo -e "${YELLOW}Setting up schedule for Job Processor...${NC}"

aws events put-rule \
    --name placement-job-processor-30min \
    --schedule-expression "rate(30 minutes)" \
    --state ENABLED \
    --description "Run Job Processor every 30 minutes" \
    --region $REGION

aws lambda add-permission \
    --function-name placement-job-processor \
    --statement-id AllowEventBridgeInvoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:$(aws sts get-caller-identity --query Account --output text):rule/placement-job-processor-30min" \
    --region $REGION 2>/dev/null || echo "  Permission already exists"

aws events put-targets \
    --rule placement-job-processor-30min \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:$(aws sts get-caller-identity --query Account --output text):function:placement-job-processor" \
    --region $REGION

echo -e "${GREEN}✓ Job Processor scheduled (every 30 minutes)${NC}"
echo ""

echo "========================================="
echo -e "${GREEN}All schedules configured!${NC}"
echo "========================================="
echo ""
echo "Schedule Summary:"
echo "  • Group Joiner:     Daily at 10 AM IST"
echo "  • Message Scraper:  Every 2 hours"
echo "  • Job Processor:    Every 30 minutes"
echo ""
echo "To view schedules:"
echo "  aws events list-rules --region $REGION"
echo ""
echo "To disable a schedule:"
echo "  aws events disable-rule --name <rule-name> --region $REGION"
