#!/bin/bash

# Master setup script for Job Processing Pipeline
# This sets up everything needed for the hybrid DynamoDB + RDS architecture

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo ""
echo "🚀 Job Processing Pipeline Setup"
echo "=================================="
echo ""
echo "This will set up:"
echo "  ✓ Database migrations (RDS)"
echo "  ✓ DynamoDB table with GSI indexes"
echo "  ✓ Test connections"
echo ""

# Step 1: Check prerequisites
echo "📋 Step 1: Checking prerequisites..."
echo ""

# Check if Docker is running
if ! docker ps &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi
echo "  ✅ Docker running"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "  ⚠️  AWS CLI not installed (needed for DynamoDB)"
    echo "     Install: brew install awscli"
    read -p "     Continue without DynamoDB setup? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    SKIP_DYNAMODB=true
else
    echo "  ✅ AWS CLI installed"
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        echo "  ⚠️  AWS credentials not configured"
        echo "     Run: aws configure"
        read -p "     Continue without DynamoDB setup? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
        SKIP_DYNAMODB=true
    else
        echo "  ✅ AWS credentials configured"
    fi
fi

# Check if docker-compose services are running
echo ""
echo "  Checking Docker services..."
if ! docker-compose ps | grep -q "Up"; then
    echo "  ⚠️  Docker services not running"
    read -p "     Start services now? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        echo "  🚀 Starting Docker services..."
        docker-compose up -d
        echo "  ⏳ Waiting for services to be ready..."
        sleep 10
    else
        echo "  ℹ️  Please start services: docker-compose up -d"
        exit 1
    fi
fi
echo "  ✅ Docker services running"

echo ""

# Step 2: Run database migrations
echo "📋 Step 2: Running database migrations..."
echo ""

"$SCRIPT_DIR/run_migrations.sh"

echo ""

# Step 3: Setup DynamoDB
if [ "$SKIP_DYNAMODB" != "true" ]; then
    echo "📋 Step 3: Setting up DynamoDB..."
    echo ""
    
    read -p "Create DynamoDB table now? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        "$SCRIPT_DIR/setup_dynamodb.sh"
    else
        echo "  ℹ️  Skipped DynamoDB setup"
        echo "     Run later: ./scripts/setup_dynamodb.sh"
    fi
else
    echo "📋 Step 3: DynamoDB setup skipped"
fi

echo ""

# Step 4: Update .env file
echo "📋 Step 4: Updating .env file..."
echo ""

ENV_FILE="$PROJECT_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "  ⚠️  .env file not found, creating from template..."
    if [ -f "$PROJECT_ROOT/.env.example" ]; then
        cp "$PROJECT_ROOT/.env.example" "$ENV_FILE"
    else
        touch "$ENV_FILE"
    fi
fi

# Add DynamoDB config if not present
if ! grep -q "DYNAMODB_TABLE_NAME" "$ENV_FILE"; then
    echo ""  >> "$ENV_FILE"
    echo "# DynamoDB Configuration" >> "$ENV_FILE"
    echo "DYNAMODB_TABLE_NAME=raw_telegram_messages" >> "$ENV_FILE"
    echo "AWS_REGION=ap-south-1" >> "$ENV_FILE"
    echo "# AWS_ACCESS_KEY_ID=your-key-here" >> "$ENV_FILE"
    echo "# AWS_SECRET_ACCESS_KEY=your-secret-here" >> "$ENV_FILE"
    echo "  ✅ Added DynamoDB config to .env"
    echo "     Please update AWS credentials!"
else
    echo "  ℹ️  DynamoDB config already in .env"
fi

# Add Job Processor config if not present
if ! grep -q "JOB_PROCESSOR_POLL_INTERVAL" "$ENV_FILE"; then
    echo "" >> "$ENV_FILE"
    echo "# Job Processor Configuration" >> "$ENV_FILE"
    echo "JOB_PROCESSOR_POLL_INTERVAL=60" >> "$ENV_FILE"
    echo "JOB_PROCESSOR_BATCH_SIZE=10" >> "$ENV_FILE"
    echo "JOB_PROCESSOR_MAX_RETRIES=3" >> "$ENV_FILE"
    echo "  ✅ Added Job Processor config to .env"
else
    echo "  ℹ️  Job Processor config already in .env"
fi

echo ""

# Step 5: Test connections
echo "📋 Step 5: Testing connections..."
echo ""

if [ "$SKIP_DYNAMODB" != "true" ]; then
    read -p "Test DynamoDB connection? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        echo "  🧪 Running DynamoDB tests..."
        cd "$PROJECT_ROOT"
        python scripts/test_dynamodb.py
    else
        echo "  ℹ️  Skipped DynamoDB test"
        echo "     Run later: python scripts/test_dynamodb.py"
    fi
fi

echo ""

# Summary
echo "=" * 60
echo "🎉 Setup Complete!"
echo "=" * 60
echo ""
echo "✅ Database migrations applied"
echo "✅ job_scraping_preferences table created"

if [ "$SKIP_DYNAMODB" != "true" ]; then
    echo "✅ DynamoDB table configured"
else
    echo "⚠️  DynamoDB setup skipped (run ./scripts/setup_dynamodb.sh later)"
fi

echo ""
echo "📝 Next Steps:"
echo ""
echo "1️⃣  Configure AWS credentials in .env:"
echo "   AWS_ACCESS_KEY_ID=your-key"
echo "   AWS_SECRET_ACCESS_KEY=your-secret"
echo ""
echo "2️⃣  Test the API:"
echo "   curl http://localhost:8000/api/v1/admin/job-preferences"
echo ""
echo "3️⃣  Continue implementation:"
echo "   - Update Lambda 2 with filtering logic"
echo "   - Create EC2 background worker"
echo "   - Add admin API endpoints"
echo ""
echo "📚 Documentation:"
echo "   - JOB_PROCESSING_IMPLEMENTATION.md"
echo "   - API_DOCS.md"
echo ""
echo "🔍 Monitor:"
echo "   - DynamoDB: aws dynamodb describe-table --table-name raw_telegram_messages"
echo "   - Logs: docker-compose logs -f backend"
echo ""
