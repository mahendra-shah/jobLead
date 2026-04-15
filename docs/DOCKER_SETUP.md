# 🚀 Getting Started - Docker Development Setup

Welcome! This guide will help you set up the Placement Dashboard Backend using Docker with an external PostgreSQL database.

---

## 🎯 Quick Overview

**What you'll have:**
- ✅ Backend API running in Docker (auto-reload on code changes)
- ✅ Redis running in Docker (caching, task queue)
- ✅ Celery workers running in Docker (background jobs)
- ✅ PostgreSQL running on your host machine (persistent data)
- ✅ ML models pre-loaded (Sentence Transformers, spaCy)

**Why this setup?**
- 🔄 **Fast development**: Code changes reflect instantly
- 💾 **Data persistence**: Database survives container restarts
- 🚀 **Production-like**: Same setup as production environment
- 🛠️ **Easy debugging**: Access database directly from host

---

## 📋 Prerequisites (Install These First)

### 1. Docker & Docker Compose
```bash
# macOS
brew install docker docker-compose

# Or download Docker Desktop:
# https://www.docker.com/products/docker-desktop

# Verify
docker --version
docker-compose --version
```

### 2. PostgreSQL 15+
```bash
# macOS
brew install postgresql@15
brew services start postgresql@15

# Linux (Ubuntu/Debian)
sudo apt install postgresql-15 postgresql-contrib-15
sudo systemctl start postgresql

# Verify
psql --version
```

### 3. pgvector Extension
```bash
# macOS
brew install pgvector

# Linux - see POSTGRES_SETUP.md for detailed instructions
```

---

## 🏃 Quick Start (3 Steps)

### Step 1: Setup PostgreSQL Database

```bash
# Create database
createdb placement_db

# Enable pgvector extension
psql placement_db -c "CREATE EXTENSION vector;"

# Verify
psql placement_db -c "\dx vector"
```

### Step 2: Run Setup Script

```bash
# Clone the repo (if you haven't)
cd placementdashboard-be

# Make script executable
chmod +x setup.sh

# Run setup
./setup.sh
```

**The script will:**
1. Check Docker installation ✅
2. Create `.env` file from template
3. Build Docker images
4. Download ML models (~200MB)
5. Start Redis container
6. Run database migrations
7. Start all services

### Step 3: Start Developing!

```bash
# API is now running at:
open http://localhost:8000/docs

# Check health
curl http://localhost:8000/health
```

---

## 🔧 Configuration

### Update .env File

Edit `.env` with your PostgreSQL credentials:

```bash
# For local PostgreSQL accessed from Docker containers
DATABASE_URL=postgresql+asyncpg://your_username@host.docker.internal:5432/placement_db

# Find your username
whoami  # Use this as your PostgreSQL username

# Example:
# DATABASE_URL=postgresql+asyncpg://john@host.docker.internal:5432/placement_db
```

### Other Important Settings

```bash
# JWT Secret (change this!)
SECRET_KEY=your-super-secret-key-change-in-production

# AWS (if using S3/DynamoDB)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
S3_BUCKET_NAME=placement-resumes
DYNAMODB_TABLE_NAME=telegram-raw-jobs

# Telegram (for scraping)
TELEGRAM_API_ID=your-api-id
TELEGRAM_API_HASH=your-api-hash
```

---

## 📦 What's Running?

After `./setup.sh` or `make dev`:

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| **Backend API** | `placement_backend` | 8000 | FastAPI REST API |
| **Celery Worker** | `placement_celery_worker` | - | Background tasks |
| **Celery Beat** | `placement_celery_beat` | - | Task scheduler |
| **Flower** | `placement_flower` | 5555 | Celery monitoring |
| **Redis** | `placement_redis` | 6379 | Cache & queue |
| **PostgreSQL** | (host machine) | 5432 | Database |

---

## 🎨 Development Workflow

### Daily Development

```bash
# Start all services
make dev

# View all logs
make logs

# View backend logs only
make logs-backend

# Stop services
make stop

# Restart services
make restart
```

### Making Code Changes

1. **Edit files** - Changes are auto-reloaded (hot reload enabled)
2. **No rebuild needed** - Code is mounted as volume
3. **Check logs** - `make logs-backend` to see changes

### Database Operations

```bash
# Create new migration
make migrate
# Enter: "add student profile fields"

# Apply migrations
make upgrade

# Rollback
make downgrade

# Access database directly
psql placement_db
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test
docker-compose run --rm backend pytest tests/test_auth.py -v

# With coverage
docker-compose run --rm backend pytest --cov=app --cov-report=html
```

### Access Container Shell

```bash
# Backend container
make shell
# or
docker-compose exec backend bash

# Inside container you can:
python manage.py shell  # If you add this
alembic current         # Check migration status
pytest                  # Run tests
```

---

## 🧪 Testing the API

### 1. Register a User

```bash
curl -X POST "http://localhost:8000/api/v1/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "TestPassword123",
    "role": "student"
  }'
```

### 2. Login

```bash
curl -X POST "http://localhost:8000/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "password": "TestPassword123"
  }'
```

### 3. Get Current User

```bash
# Save token from login response
TOKEN="your_access_token_here"

curl -X GET "http://localhost:8000/api/v1/auth/me" \
  -H "Authorization: Bearer $TOKEN"
```

### 4. Use Swagger UI (Easier!)

Open http://localhost:8000/docs and use the interactive interface!

---

## 🐛 Troubleshooting

### Issue: "Connection refused" to PostgreSQL

**Check PostgreSQL is running:**
```bash
# macOS
brew services list | grep postgresql

# Linux
sudo systemctl status postgresql
```

**Check DATABASE_URL in .env:**
```bash
# Should use host.docker.internal for Docker to access host
DATABASE_URL=postgresql+asyncpg://username@host.docker.internal:5432/placement_db
```

**Linux users:** Add to `/etc/hosts`:
```bash
echo "127.0.0.1 host.docker.internal" | sudo tee -a /etc/hosts
```

### Issue: "Port 8000 already in use"

```bash
# Find and kill process
lsof -ti:8000 | xargs kill -9

# Or change port in docker-compose.yml
ports:
  - "8001:8000"  # Host:Container
```

### Issue: "Permission denied" running setup.sh

```bash
chmod +x setup.sh
./setup.sh
```

### Issue: Docker build fails

```bash
# Clean everything and rebuild
make clean
docker system prune -a
make build
```

### Issue: ML models not found

```bash
# Re-download models
docker-compose run --rm backend python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
docker-compose run --rm backend python -m spacy download en_core_web_sm
```

---

## 📊 Monitoring & Logs

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f celery_worker

# Last 100 lines
docker-compose logs --tail=100 backend
```

### Check Service Status

```bash
make ps
# or
docker-compose ps
```

### Celery Task Monitoring

Open Flower UI: http://localhost:5555

- View active tasks
- Monitor worker status
- Check task history
- Retry failed tasks

---

## 🔐 Important Security Notes

### Before Production:

1. **Change SECRET_KEY** in .env
2. **Use strong passwords** for database
3. **Enable SSL** for PostgreSQL
4. **Use environment variables** (not hardcoded)
5. **Set DEBUG=False** in production
6. **Configure CORS** properly in `main.py`

---

## 📚 Next Steps

### Learn the Codebase

1. **Read**: [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) - Understand the architecture
2. **Read**: [ARCHITECTURE.md](ARCHITECTURE.md) - See data flows and diagrams
3. **Read**: [API_DOCS.md](API_DOCS.md) - Available endpoints

### Start Building

1. **Implement Student Profile API** - `app/api/v1/students.py`
2. **Implement Jobs Listing** - `app/api/v1/jobs.py`
3. **Add Resume Parser** - `app/services/resume_parser.py`
4. **Add ML Matching** - `app/services/ml_service.py`

### Testing

1. Write tests in `tests/`
2. Run with `make test`
3. Aim for 80% coverage

### Deploy

1. Read [DEPLOYMENT.md](DEPLOYMENT.md) for AWS setup
2. Configure CI/CD pipeline
3. Set up monitoring

---

## 💡 Pro Tips

### Speed Up Development

```bash
# Use Makefile commands (faster)
make dev         # Instead of docker-compose up -d
make logs        # Instead of docker-compose logs -f
make shell       # Instead of docker-compose exec backend bash
make test        # Instead of docker-compose run --rm backend pytest
```

### Code Quality

```bash
# Format code
docker-compose run --rm backend black app/
docker-compose run --rm backend isort app/

# Or add to Makefile
make format
```

### Database Management

```bash
# Quick database access
psql placement_db

# Inside psql:
\dt              # List tables
\d users         # Describe users table
SELECT * FROM users LIMIT 10;
```

---

## 🆘 Getting Help

**Have issues?**

1. Check [POSTGRES_SETUP.md](POSTGRES_SETUP.md) for database issues
2. Check [QUICKSTART.md](QUICKSTART.md) for detailed instructions
3. Check Docker logs: `make logs`
4. Check database connection: `psql placement_db`

**Common commands:**

```bash
make help        # See all available commands
make ps          # Check service status
make logs        # View logs
make shell       # Access container
make test        # Run tests
```

---

## ✅ Success Checklist

Before you start coding, ensure:

- [ ] Docker and Docker Compose installed
- [ ] PostgreSQL running on host
- [ ] Database `placement_db` created
- [ ] pgvector extension installed
- [ ] `.env` file configured with correct DATABASE_URL
- [ ] `./setup.sh` completed successfully
- [ ] Services running: `make ps` shows all services up
- [ ] API accessible: http://localhost:8000/docs loads
- [ ] Can register/login user via API
- [ ] Database has tables: `psql placement_db -c "\dt"`

---

**🎉 You're all set! Happy coding!**

For detailed guides, see:
- [QUICKSTART.md](QUICKSTART.md) - Detailed setup
- [POSTGRES_SETUP.md](POSTGRES_SETUP.md) - PostgreSQL configuration
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design
- [API_DOCS.md](API_DOCS.md) - API reference
