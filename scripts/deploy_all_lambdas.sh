#!/bin/bash
# Deploy all Lambda functions to AWS
# Usage: ./deploy_all_lambdas.sh

set -e  # Exit on error

# Configuration
REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_NAME="PlacementDashboardLambdaRole"
LAYER_NAME="placement-dependencies"

echo "========================================="
echo "Deploying Placement Dashboard Lambdas"
echo "========================================="
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo -e "${RED}AWS CLI not found. Please install it first.${NC}"
    exit 1
fi

# Check credentials
echo -e "${YELLOW}Checking AWS credentials...${NC}"
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}AWS credentials not configured. Run 'aws configure'${NC}"
    exit 1
fi
echo -e "${GREEN}✓ AWS credentials valid${NC}"
echo ""

# Create IAM role if it doesn't exist
echo -e "${YELLOW}Checking IAM role...${NC}"
if ! aws iam get-role --role-name $ROLE_NAME &> /dev/null; then
    echo "Creating IAM role: $ROLE_NAME"
    
    # Create trust policy
    cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
    
    aws iam create-role \
        --role-name $ROLE_NAME \
        --assume-role-policy-document file:///tmp/trust-policy.json
    
    # Attach policies
    aws iam attach-role-policy \
        --role-name $ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
    
    aws iam attach-role-policy \
        --role-name $ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    
    echo -e "${GREEN}✓ IAM role created${NC}"
    echo -e "${YELLOW}Waiting 10 seconds for role to propagate...${NC}"
    sleep 10
else
    echo -e "${GREEN}✓ IAM role exists${NC}"
fi

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo ""

# Function to deploy a Lambda
deploy_lambda() {
    local FUNCTION_NAME=$1
    local HANDLER_PATH=$2
    local TIMEOUT=${3:-300}  # Default 5 minutes
    local MEMORY=${4:-512}   # Default 512MB
    
    echo -e "${YELLOW}Deploying: $FUNCTION_NAME${NC}"
    
    cd "lambda/$HANDLER_PATH"
    
    # Create deployment package
    echo "  Creating deployment package..."
    rm -rf package deployment.zip
    mkdir -p package
    
    # Install dependencies
    if [ -f requirements.txt ]; then
        echo "  Installing dependencies..."
        pip install -r requirements.txt -t package/ --quiet
    fi
    
    # Copy application code
    echo "  Copying application code..."
    cp -r ../../app package/
    cp handler.py package/
    
    # Create zip
    cd package
    zip -r ../deployment.zip . -q
    cd ..
    
    # Check if function exists
    if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &> /dev/null; then
        echo "  Updating existing function..."
        aws lambda update-function-code \
            --function-name $FUNCTION_NAME \
            --zip-file fileb://deployment.zip \
            --region $REGION \
            --no-cli-pager > /dev/null
        
        aws lambda update-function-configuration \
            --function-name $FUNCTION_NAME \
            --timeout $TIMEOUT \
            --memory-size $MEMORY \
            --region $REGION \
            --no-cli-pager > /dev/null
    else
        echo "  Creating new function..."
        aws lambda create-function \
            --function-name $FUNCTION_NAME \
            --runtime python3.11 \
            --role $ROLE_ARN \
            --handler handler.lambda_handler \
            --zip-file fileb://deployment.zip \
            --timeout $TIMEOUT \
            --memory-size $MEMORY \
            --region $REGION \
            --environment "Variables={
                DATABASE_URL=$DATABASE_URL,
                AI_PROVIDER=$AI_PROVIDER,
                OPENAI_API_KEY=$OPENAI_API_KEY,
                GEMINI_API_KEY=$GEMINI_API_KEY,
                MAX_GROUPS_JOIN_PER_DAY=$MAX_GROUPS_JOIN_PER_DAY
            }" \
            --no-cli-pager > /dev/null
    fi
    
    # Cleanup
    rm -rf package deployment.zip
    
    cd ../..
    
    echo -e "${GREEN}  ✓ $FUNCTION_NAME deployed${NC}"
    echo ""
}

# Deploy Lambda functions
echo "========================================="
echo "Deploying Lambda Functions"
echo "========================================="
echo ""

deploy_lambda "placement-group-joiner" "group_joiner" 7200 512      # 2 hours, 512MB
deploy_lambda "placement-message-scraper" "message_scraper" 900 1024  # 15 min, 1GB
deploy_lambda "placement-job-processor" "job_processor" 600 1024      # 10 min, 1GB

echo "========================================="
echo -e "${GREEN}All Lambda functions deployed!${NC}"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Set up CloudWatch Events triggers"
echo "2. Configure environment variables"
echo "3. Test functions manually"
echo ""
echo "To set up schedules, run:"
echo "  ./scripts/setup_cloudwatch.sh"
