#!/bin/bash

# Setup script for Placement Dashboard Backend
# Uses Docker for development with external PostgreSQL database

set -e

echo "🚀 Setting up Placement Dashboard Backend (Docker-based)..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check Docker
echo -e "${YELLOW}Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is required but not found. Please install Docker.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker found: $(docker --version)${NC}"

# Check Docker Compose
echo -e "${YELLOW}Checking Docker Compose...${NC}"
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is required but not found.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose found: $(docker-compose --version)${NC}"

# Create .env file if not exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file...${NC}"
    cp .env.example .env
    echo -e "${GREEN}✓ .env file created${NC}"
    echo -e "${BLUE}📝 Please update .env with your external PostgreSQL credentials:${NC}"
    echo -e "   - DATABASE_URL (your external PostgreSQL URL)"
    echo -e "   - AWS credentials (if using S3/DynamoDB)"
    echo -e "   - Telegram API credentials"
    echo ""
    read -p "Press Enter after updating .env file to continue..."
else
    echo -e "${GREEN}✓ .env file already exists${NC}"
fi

# Create necessary directories
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p logs
mkdir -p uploads
mkdir -p models
echo -e "${GREEN}✓ Directories created${NC}"

# Load environment variables (excluding comments and inline comments)
if [ -f .env ]; then
    set -a
    source <(grep -v '^#' .env | sed 's/#.*$//' | grep -v '^$')
    set +a
fi

# Check PostgreSQL connection
echo ""
echo -e "${YELLOW}Checking external PostgreSQL connection...${NC}"
if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}Warning: DATABASE_URL not set in .env file${NC}"
else
    # Extract connection details from DATABASE_URL
    DB_HOST=$(echo $DATABASE_URL | sed -n 's/.*@\([^:]*\).*/\1/p')
    if [ ! -z "$DB_HOST" ]; then
        echo -e "${GREEN}✓ Database host configured: $DB_HOST${NC}"
    fi
fi

# Build Docker images
echo ""
echo -e "${YELLOW}Building Docker images...${NC}"
docker-compose build
echo -e "${GREEN}✓ Docker images built${NC}"

# Start services
echo ""
echo -e "${YELLOW}Starting services (Redis only, using external PostgreSQL)...${NC}"
docker-compose up -d redis
echo -e "${GREEN}✓ Redis started${NC}"

# Wait for Redis
echo -e "${YELLOW}Waiting for Redis to be ready...${NC}"
sleep 3
echo -e "${GREEN}✓ Redis is ready${NC}"

# Note: ML models are already downloaded during Docker image build
echo ""
echo -e "${GREEN}✓ ML models are ready (included in Docker image)${NC}"

# Run database migrations
echo ""
echo -e "${YELLOW}Running database migrations...${NC}"
echo -e "${BLUE}Note: Make sure your external PostgreSQL database is accessible${NC}"
read -p "Have you created the database and installed pgvector extension? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker-compose run --rm backend alembic upgrade head
    echo -e "${GREEN}✓ Migrations completed${NC}"
else
    echo -e "${YELLOW}⚠ Skipping migrations. Run manually with:${NC}"
    echo -e "   docker-compose run --rm backend alembic upgrade head"
fi

# Start all services
echo ""
echo -e "${YELLOW}Starting all application services...${NC}"
docker-compose up -d
echo -e "${GREEN}✓ All services started${NC}"

# Show status
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Setup completed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}📊 Running Services:${NC}"
docker-compose ps
echo ""
echo -e "${BLUE}🔗 Access URLs:${NC}"
echo -e "   API Documentation: ${GREEN}http://localhost:8000/docs${NC}"
echo -e "   API Health Check:  ${GREEN}http://localhost:8000/health${NC}"
echo -e "   Celery Monitor:    ${GREEN}http://localhost:5555${NC}"
echo ""
echo -e "${BLUE}📝 Useful Commands:${NC}"
echo -e "   View logs:         ${YELLOW}docker-compose logs -f${NC}"
echo -e "   Stop services:     ${YELLOW}docker-compose down${NC}"
echo -e "   Restart services:  ${YELLOW}docker-compose restart${NC}"
echo -e "   Run migrations:    ${YELLOW}docker-compose run --rm backend alembic upgrade head${NC}"
echo -e "   Access shell:      ${YELLOW}docker-compose exec backend bash${NC}"
echo -e "   Run tests:         ${YELLOW}docker-compose run --rm backend pytest${NC}"
echo ""
echo -e "${BLUE}📚 Important:${NC}"
echo -e "   1. External PostgreSQL must be accessible from Docker containers"
echo -e "   2. Update DATABASE_URL in .env with correct host/credentials"
echo -e "   3. Create database: CREATE DATABASE placement_db;"
echo -e "   4. Install extension: CREATE EXTENSION vector;"
echo ""
echo -e "${GREEN}Happy coding! 🎉${NC}"
