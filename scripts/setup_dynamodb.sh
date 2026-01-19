#!/bin/bash

# Setup DynamoDB table for raw Telegram messages
# This script creates the DynamoDB table with GSI indexes and TTL

set -e

echo "🚀 Setting up DynamoDB for Job Processing Pipeline"
echo "=================================================="

# Configuration
TABLE_NAME=${DYNAMODB_TABLE_NAME:-"raw_telegram_messages"}
REGION=${AWS_REGION:-"ap-south-1"}

echo ""
echo "📋 Configuration:"
echo "  Table Name: $TABLE_NAME"
echo "  Region: $REGION"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI is not installed. Please install it first:"
    echo "   brew install awscli"
    exit 1
fi

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS credentials are not configured. Please run:"
    echo "   aws configure"
    exit 1
fi

echo "✅ AWS CLI configured"
echo ""

# Check if table already exists
echo "🔍 Checking if table already exists..."
if aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION" &> /dev/null; then
    echo "⚠️  Table '$TABLE_NAME' already exists!"
    echo ""
    read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Deleting existing table..."
        aws dynamodb delete-table --table-name "$TABLE_NAME" --region "$REGION"
        
        echo "⏳ Waiting for table deletion..."
        aws dynamodb wait table-not-exists --table-name "$TABLE_NAME" --region "$REGION"
        echo "✅ Table deleted"
    else
        echo "ℹ️  Keeping existing table. Exiting."
        exit 0
    fi
fi

echo ""
echo "📦 Creating DynamoDB table..."

# Create table with GSI indexes
aws dynamodb create-table \
    --table-name "$TABLE_NAME" \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
        AttributeName=processed,AttributeType=S \
        AttributeName=created_at,AttributeType=S \
        AttributeName=processing_status,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --global-secondary-indexes \
        IndexName=processed-created_at-index,KeySchema=['{AttributeName=processed,KeyType=HASH},{AttributeName=created_at,KeyType=RANGE}'],Projection='{ProjectionType=ALL}' \
        IndexName=processing_status-created_at-index,KeySchema=['{AttributeName=processing_status,KeyType=HASH},{AttributeName=created_at,KeyType=RANGE}'],Projection='{ProjectionType=ALL}' \
    --billing-mode PAY_PER_REQUEST \
    --region "$REGION"

echo "⏳ Waiting for table to be active..."
aws dynamodb wait table-exists --table-name "$TABLE_NAME" --region "$REGION"

echo "✅ Table created successfully!"
echo ""

# Enable TTL
echo "⏰ Enabling TTL (auto-delete after 7 days)..."
aws dynamodb update-time-to-live \
    --table-name "$TABLE_NAME" \
    --time-to-live-specification Enabled=true,AttributeName=ttl \
    --region "$REGION"

echo "✅ TTL enabled"
echo ""

# Enable Point-in-Time Recovery (optional but recommended)
echo "💾 Enabling Point-in-Time Recovery..."
aws dynamodb update-continuous-backups \
    --table-name "$TABLE_NAME" \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
    --region "$REGION"

echo "✅ Point-in-Time Recovery enabled"
echo ""

# Display table details
echo "📊 Table Details:"
echo "================"
aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION" \
    --query 'Table.{
        Name:TableName,
        Status:TableStatus,
        ItemCount:ItemCount,
        SizeBytes:TableSizeBytes,
        BillingMode:BillingModeSummary.BillingMode,
        GSI:GlobalSecondaryIndexes[*].IndexName
    }' \
    --output table

echo ""
echo "🎉 DynamoDB setup complete!"
echo ""
echo "📝 Next steps:"
echo "  1. Add to .env file:"
echo "     DYNAMODB_TABLE_NAME=$TABLE_NAME"
echo "     AWS_REGION=$REGION"
echo ""
echo "  2. Test connection:"
echo "     python -c 'from app.services.dynamodb_service import DynamoDBService; import asyncio; asyncio.run(DynamoDBService.query_unprocessed())'"
echo ""
echo "  3. Monitor costs:"
echo "     aws dynamodb describe-table --table-name $TABLE_NAME --region $REGION --query 'Table.BillingModeSummary'"
echo ""
