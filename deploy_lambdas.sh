#!/bin/bash

# ============================================================================
# Lambda Deployment Script
# Deploys Channel Batcher (Lambda 1) and Telegram Scraper (Lambda 2)
# ============================================================================

set -e  # Exit on any error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Lambda Deployment${NC}\n"

# ============================================================================
# Configuration (UPDATE THESE VALUES)
# ============================================================================

# AWS Configuration
AWS_REGION="${AWS_REGION:-ap-south-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-your-aws-account-id}"  # ⚠️ UPDATE THIS
LAMBDA_ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::${AWS_ACCOUNT_ID}:role/LambdaDeploymentRole}"  # ⚠️ UPDATE THIS

# Lambda 1 Configuration (Channel Batcher)
BATCHER_FUNCTION_NAME="placement-channel-batcher"
BATCHER_HANDLER="lambda_function.lambda_handler"
BATCHER_RUNTIME="python3.11"
BATCHER_TIMEOUT=300  # 5 minutes
BATCHER_MEMORY=256   # MB

# Lambda 2 Configuration (Telegram Scraper)
SCRAPER_FUNCTION_NAME="placement-telegram-scraper"
SCRAPER_HANDLER="lambda_function.lambda_handler"
SCRAPER_RUNTIME="python3.11"
SCRAPER_TIMEOUT=600  # 10 minutes
SCRAPER_MEMORY=512   # MB

# Environment Variables (from .env file)
if [ -f .env ]; then
    # Load .env file using export syntax to handle special characters
    set -a  # Automatically export all variables
    source <(grep -v '^#' .env | grep -v '^$' | sed -e 's/^/export /')
    set +a  # Stop auto-exporting
    echo -e "${GREEN}✅ Loaded environment variables from .env${NC}"
else
    echo -e "${RED}❌ .env file not found${NC}"
    exit 1
fi

# Validate required environment variables
if [ -z "$MONGODB_URI" ]; then
    echo -e "${RED}❌ MONGODB_URI not set in .env${NC}"
    exit 1
fi

if [ -z "$TELEGRAM_API_ID" ] || [ -z "$TELEGRAM_API_HASH" ] || [ -z "$TELEGRAM_PHONE" ]; then
    echo -e "${RED}❌ Telegram credentials not set in .env${NC}"
    exit 1
fi

# ============================================================================
# Helper Functions
# ============================================================================

function check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}❌ AWS CLI not found. Please install it first.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ AWS CLI found${NC}"
}

function check_lambda_exists() {
    local function_name=$1
    aws lambda get-function --function-name "$function_name" --region "$AWS_REGION" &>/dev/null
    return $?
}

function create_lambda() {
    local function_name=$1
    local handler=$2
    local runtime=$3
    local timeout=$4
    local memory=$5
    local zip_file=$6
    local env_vars=$7

    echo -e "${YELLOW}📦 Creating Lambda function: $function_name${NC}"
    
    aws lambda create-function \
        --function-name "$function_name" \
        --runtime "$runtime" \
        --role "$LAMBDA_ROLE_ARN" \
        --handler "$handler" \
        --timeout "$timeout" \
        --memory-size "$memory" \
        --zip-file "fileb://$zip_file" \
        --environment "Variables={$env_vars}" \
        --region "$AWS_REGION"
    
    echo -e "${GREEN}✅ Lambda function created: $function_name${NC}"
}

function update_lambda() {
    local function_name=$1
    local handler=$2
    local timeout=$3
    local memory=$4
    local zip_file=$5
    local env_vars=$6

    echo -e "${YELLOW}🔄 Updating Lambda function: $function_name${NC}"
    
    # Update function code
    aws lambda update-function-code \
        --function-name "$function_name" \
        --zip-file "fileb://$zip_file" \
        --region "$AWS_REGION"
    
    # Wait for update to complete
    aws lambda wait function-updated \
        --function-name "$function_name" \
        --region "$AWS_REGION"
    
    # Update function configuration
    aws lambda update-function-configuration \
        --function-name "$function_name" \
        --handler "$handler" \
        --timeout "$timeout" \
        --memory-size "$memory" \
        --environment "Variables={$env_vars}" \
        --region "$AWS_REGION"
    
    echo -e "${GREEN}✅ Lambda function updated: $function_name${NC}"
}

# ============================================================================
# Deploy Lambda 1 (Channel Batcher)
# ============================================================================

echo -e "\n${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Lambda 1: Channel Batcher (Orchestrator)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}\n"

cd lambda/channel_batcher

# Clean up old packages
rm -rf function.zip package/
mkdir -p package

# Install dependencies
echo -e "${YELLOW}📦 Installing dependencies...${NC}"
python3 -m pip install -r requirements.txt -t package/ --platform manylinux2014_x86_64 --only-binary=:all:

# Copy Lambda function
cp lambda_function.py package/

# Create deployment package
cd package
zip -r ../function.zip . -q
cd ..

echo -e "${GREEN}✅ Deployment package created: $(du -h function.zip | cut -f1)${NC}"

# Environment variables for Lambda 1
BATCHER_ENV_VARS="MONGODB_URI=${MONGODB_URI},MONGODB_DATABASE=${MONGODB_DATABASE:-placement_db},SCRAPER_LAMBDA_NAME=${SCRAPER_FUNCTION_NAME}"

# Deploy Lambda 1
if check_lambda_exists "$BATCHER_FUNCTION_NAME"; then
    update_lambda "$BATCHER_FUNCTION_NAME" "$BATCHER_HANDLER" "$BATCHER_TIMEOUT" "$BATCHER_MEMORY" "function.zip" "$BATCHER_ENV_VARS"
else
    create_lambda "$BATCHER_FUNCTION_NAME" "$BATCHER_HANDLER" "$BATCHER_RUNTIME" "$BATCHER_TIMEOUT" "$BATCHER_MEMORY" "function.zip" "$BATCHER_ENV_VARS"
fi

cd ../..

# ============================================================================
# Deploy Lambda 2 (Telegram Scraper)
# ============================================================================

echo -e "\n${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Lambda 2: Telegram Scraper (Worker)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}\n"

cd lambda/telegram_scraper

# Clean up old packages
rm -rf function.zip package/
mkdir -p package

# Install dependencies
echo -e "${YELLOW}📦 Installing dependencies (including telethon)...${NC}"
python3 -m pip install -r requirements.txt -t package/

# Copy Lambda function
cp lambda_function.py package/

# Copy ALL Telegram session files (multi-account setup)
echo -e "${YELLOW}🔐 Copying Telegram session files...${NC}"
session_count=0
for i in 1 2 3 4 5; do
    if [ -f "session_account${i}.session" ]; then
        cp "session_account${i}.session" package/
        session_count=$((session_count + 1))
        echo -e "${GREEN}   ✓ Copied session_account${i}.session${NC}"
    fi
done

if [ $session_count -eq 0 ]; then
    echo -e "${RED}⚠️  Warning: No Telegram session files found!${NC}"
    echo -e "${RED}   Lambda will fail without authentication.${NC}"
    echo -e "${RED}   Please copy session files to lambda/telegram_scraper/${NC}"
else
    echo -e "${GREEN}✅ Copied $session_count session files${NC}"
fi

# Create deployment package
cd package
zip -r ../function.zip . -q
cd ..

echo -e "${GREEN}✅ Deployment package created: $(du -h function.zip | cut -f1)${NC}"

# Environment variables for Lambda 2
SCRAPER_ENV_VARS="MONGODB_URI=${MONGODB_URI},MONGODB_DATABASE=${MONGODB_DATABASE:-placement_db},TELEGRAM_API_ID=${TELEGRAM_API_ID},TELEGRAM_API_HASH=${TELEGRAM_API_HASH},TELEGRAM_PHONE=${TELEGRAM_PHONE}"

# Deploy Lambda 2
if check_lambda_exists "$SCRAPER_FUNCTION_NAME"; then
    update_lambda "$SCRAPER_FUNCTION_NAME" "$SCRAPER_HANDLER" "$SCRAPER_TIMEOUT" "$SCRAPER_MEMORY" "function.zip" "$SCRAPER_ENV_VARS"
else
    create_lambda "$SCRAPER_FUNCTION_NAME" "$SCRAPER_HANDLER" "$SCRAPER_RUNTIME" "$SCRAPER_TIMEOUT" "$SCRAPER_MEMORY" "function.zip" "$SCRAPER_ENV_VARS"
fi

cd ../..

# ============================================================================
# Grant Lambda 1 Permission to Invoke Lambda 2
# ============================================================================

echo -e "\n${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Permissions${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}\n"

echo -e "${YELLOW}🔐 Granting invoke permission...${NC}"

BATCHER_ARN="arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:${BATCHER_FUNCTION_NAME}"

# Remove existing policy if it exists (ignore errors)
aws lambda remove-permission \
    --function-name "$SCRAPER_FUNCTION_NAME" \
    --statement-id AllowInvokeFromBatcher \
    --region "$AWS_REGION" 2>/dev/null || true

# Add new policy
aws lambda add-permission \
    --function-name "$SCRAPER_FUNCTION_NAME" \
    --statement-id AllowInvokeFromBatcher \
    --action lambda:InvokeFunction \
    --principal lambda.amazonaws.com \
    --source-arn "$BATCHER_ARN" \
    --region "$AWS_REGION"

echo -e "${GREEN}✅ Permission granted${NC}"

# ============================================================================
# Create EventBridge Rule (Daily at 3 AM)
# ============================================================================

echo -e "\n${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  EventBridge Schedule${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}\n"

RULE_NAME="daily-placement-scraper"
SCHEDULE_EXPRESSION="cron(0 3 * * ? *)"  # 3 AM UTC daily

echo -e "${YELLOW}📅 Creating EventBridge rule: $RULE_NAME${NC}"

# Create or update rule
aws events put-rule \
    --name "$RULE_NAME" \
    --schedule-expression "$SCHEDULE_EXPRESSION" \
    --state ENABLED \
    --description "Triggers daily Telegram scraping at 3 AM UTC" \
    --region "$AWS_REGION"

echo -e "${GREEN}✅ EventBridge rule created${NC}"

# Add permission for EventBridge to invoke Lambda
echo -e "${YELLOW}🔐 Granting EventBridge invoke permission...${NC}"

aws lambda add-permission \
    --function-name "$BATCHER_FUNCTION_NAME" \
    --statement-id AllowEventBridgeInvoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region "$AWS_REGION" 2>/dev/null || echo "Permission already exists"

echo -e "${GREEN}✅ EventBridge permission granted${NC}"

# Add target (Lambda 1)
echo -e "${YELLOW}🎯 Adding Lambda as target...${NC}"

aws events put-targets \
    --rule "$RULE_NAME" \
    --targets "Id=1,Arn=${BATCHER_ARN}" \
    --region "$AWS_REGION"

echo -e "${GREEN}✅ Target added to EventBridge rule${NC}"

# ============================================================================
# Summary
# ============================================================================

echo -e "\n${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Deployment Complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}\n"

echo -e "${GREEN}Lambda 1 (Channel Batcher):${NC}"
echo -e "  Function Name: ${BATCHER_FUNCTION_NAME}"
echo -e "  Runtime: ${BATCHER_RUNTIME}"
echo -e "  Timeout: ${BATCHER_TIMEOUT}s"
echo -e "  Memory: ${BATCHER_MEMORY}MB"

echo -e "\n${GREEN}Lambda 2 (Telegram Scraper):${NC}"
echo -e "  Function Name: ${SCRAPER_FUNCTION_NAME}"
echo -e "  Runtime: ${SCRAPER_RUNTIME}"
echo -e "  Timeout: ${SCRAPER_TIMEOUT}s"
echo -e "  Memory: ${SCRAPER_MEMORY}MB"

echo -e "\n${GREEN}EventBridge Schedule:${NC}"
echo -e "  Rule Name: ${RULE_NAME}"
echo -e "  Schedule: Daily at 3 AM UTC"
echo -e "  Status: ENABLED"

echo -e "\n${YELLOW}Next Steps:${NC}"
echo -e "1. Run sync script to populate MongoDB channels:"
echo -e "   ${GREEN}python sync_channels_to_mongodb.py${NC}"
echo -e ""
echo -e "2. Test Lambda manually:"
echo -e "   ${GREEN}aws lambda invoke --function-name ${BATCHER_FUNCTION_NAME} --region ${AWS_REGION} output.json${NC}"
echo -e ""
echo -e "3. View CloudWatch logs:"
echo -e "   ${GREEN}aws logs tail /aws/lambda/${BATCHER_FUNCTION_NAME} --follow --region ${AWS_REGION}${NC}"
echo -e ""
echo -e "4. Monitor execution:"
echo -e "   ${GREEN}python monitor_lambda_stats.py${NC}"

echo -e "\n${GREEN}🎉 All done! Your automated scraper will run daily at 3 AM UTC${NC}\n"
