# Placement Dashboard Backend

A job aggregation and matching platform for placement management, built with FastAPI and ML-based job recommendations.

## Features

- 🤖 **Automated Job Scraping** from Telegram channels
- 🎯 **ML-Based Job Matching** using Sentence Transformers
- 📄 **Resume Parsing** with skill extraction
- 🔐 **Role-Based Access Control** (SuperAdmin, Admin, Placement, Student)
- 📊 **Analytics Dashboard** with detailed reporting
- 🔄 **Bulk Operations** for students, jobs, and channels

## Tech Stack

- **Framework**: FastAPI 0.109+
- **Database**: PostgreSQL 15+ with pgvector (RDS)
- **Cache**: Redis 7+
- **Task Queue**: Celery
- **ML**: Sentence Transformers, spaCy
- **Storage**: AWS S3 (resumes), DynamoDB (raw data)

## Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Redis 7+
- AWS Account (for S3 and DynamoDB)

## Setup

### 1. Clone and Install Dependencies

```bash
cd placementdashboard-be
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 3. Setup Database

```bash
# Install pgvector extension
psql -U postgres -d placement_db -c "CREATE EXTENSION vector;"

# Run migrations
alembic upgrade head
```

### 4. Download ML Models

```bash
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
python -m spacy download en_core_web_sm
```

### 5. Start Services

```bash
# Terminal 1: Redis
redis-server

# Terminal 2: Celery Worker
celery -A app.workers.celery_app worker --loglevel=info

# Terminal 3: Celery Beat (scheduled tasks)
celery -A app.workers.celery_app beat --loglevel=info

# Terminal 4: FastAPI
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Documentation

Once running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Project Structure

```
app/
├── api/v1/          # API endpoints
├── models/          # SQLAlchemy models
├── schemas/         # Pydantic schemas
├── services/        # Business logic
├── workers/         # Celery tasks
├── core/            # Security, logging
└── utils/           # Helpers
```

## Development

```bash
# Run tests
pytest

# Format code
black app/
isort app/

# Type checking
mypy app/

# Linting
ruff check app/
```

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for AWS deployment instructions.

## License

MIT
