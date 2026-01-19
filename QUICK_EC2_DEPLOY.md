# 🚀 Quick Start - EC2 Deployment

## ✅ What You've Done So Far
- ✅ Created Neon PostgreSQL database
- ✅ Updated `.env` with Neon connection string
- ✅ Celery is configured and ready

---

## 📌 Answer to Your Questions

### **Q: Do I need to create tables in the database first?**
**A: NO!** ❌ Docker + Alembic will automatically create all tables when you run migrations.

### **Q: Will it work automatically when I run Docker?**
**A: Almost!** You need to run migrations ONCE after starting Docker. Here's the complete flow:

---

## 🎯 Complete Deployment Process (3 Simple Steps)

### **Step 1: Prepare Your EC2 Instance**

```bash
# SSH into EC2
ssh -i your-key.pem ubuntu@YOUR_EC2_IP

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Logout and login again
exit
# SSH back in
```

---

### **Step 2: Deploy Your Application**

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/placementdashboard-be.git
cd placementdashboard-be

# Create production env file
nano .env.production
```

**Copy this into `.env.production`:**

```bash
# Application
APP_NAME="Placement Dashboard API"
APP_VERSION="1.0.0"
DEBUG=False
ENVIRONMENT=production

# Server
HOST=0.0.0.0
PORT=8000
RELOAD=False

# Database (Neon PostgreSQL)
DATABASE_URL=postgresql+asyncpg://neondb_owner:npg_F7s9MjvawfBn@ep-bitter-snow-ahon59hy-pooler.c-3.us-east-1.aws.neon.tech/placement_db?sslmode=require
DATABASE_POOL_SIZE=20
DATABASE_MAX_OVERFLOW=10

# JWT - GENERATE A RANDOM SECRET!
SECRET_KEY=CHANGE_THIS_TO_RANDOM_STRING_MIN_32_CHARS
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=1440
REFRESH_TOKEN_EXPIRE_DAYS=30

# Redis & Celery
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/1
CELERY_RESULT_BACKEND=redis://redis:6379/2

# Telegram
TELEGRAM_API_ID=25218676
TELEGRAM_API_HASH=b763a5290336347353dea4e88829d6c3
TELEGRAM_PHONE=+919329796819

# MongoDB
MONGODB_USERNAME=assi
MONGODB_PASSWORD=Upo55HF6EzKdKQYV
MONGODB_URI=mongodb+srv://assi:Upo55HF6EzKdKQYV@cluster0.apufdpu.mongodb.net/placement_db?retryWrites=true&w=majority
STORAGE_TYPE=mongodb

# AI Provider
AI_PROVIDER=gemini
GEMINI_API_KEY=your-gemini-api-key

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
```

**Save:** `Ctrl + X`, `Y`, `Enter`

---

### **Step 3: Start Everything**

```bash
# Build and start all services
docker-compose -f docker-compose.prod.yml up -d

# Wait 10 seconds for containers to start
sleep 10

# Run database migrations (THIS CREATES ALL TABLES!)
docker-compose -f docker-compose.prod.yml exec backend alembic upgrade head

# Check if everything is running
docker-compose -f docker-compose.prod.yml ps
```

**That's it!** ✅ Your application is now running with:
- ✅ All database tables created automatically
- ✅ FastAPI backend running on port 8000
- ✅ Celery worker processing background tasks
- ✅ Redis for caching and task queue
- ✅ Nginx reverse proxy

---

## 🧪 Test Your Deployment

```bash
# Test from EC2 instance
curl http://localhost:8000/health

# Test from your computer (replace with your EC2 public IP)
curl http://YOUR_EC2_IP:8000/health

# Should return:
# {"status":"healthy","database":"connected","version":"1.0.0"}
```

---

## 🔐 Create First Admin User

```bash
# Option 1: Register via API
curl -X POST http://YOUR_EC2_IP:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@placement.com",
    "password": "SecurePassword123!",
    "full_name": "Admin User",
    "role": "admin"
  }'

# Option 2: Using Python in Docker
docker-compose -f docker-compose.prod.yml exec backend python -c "
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.user import User
from app.core.security import get_password_hash
import os

# Sync database URL (replace +asyncpg with nothing)
db_url = os.getenv('DATABASE_URL').replace('+asyncpg', '')
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()

admin = User(
    email='admin@placement.com',
    hashed_password=get_password_hash('SecurePassword123!'),
    full_name='Admin User',
    role='admin',
    is_active=True
)
session.add(admin)
session.commit()
print('✅ Admin user created!')
session.close()
"
```

---

## 📊 What Happens Automatically?

### **When you run Docker Compose:**
1. ✅ Builds your application container
2. ✅ Starts PostgreSQL connection to Neon
3. ✅ Starts Redis for caching
4. ✅ Starts Celery worker for background tasks
5. ✅ Starts Nginx for reverse proxy

### **When you run migrations:**
1. ✅ Connects to your Neon database
2. ✅ Creates `alembic_version` table
3. ✅ Creates all application tables:
   - `users` (admins, students)
   - `jobs` (job postings)
   - `telegram_channels`
   - `telegram_messages`
   - `student_profiles`
   - `saved_jobs`
   - `job_applications`
4. ✅ Creates indexes for performance
5. ✅ Creates vector extension for ML recommendations

### **After migrations complete:**
- ✅ Your database is ready with empty tables
- ✅ No manual table creation needed
- ✅ Can start using API immediately

---

## 🔄 Daily Operations

### **View Logs**
```bash
# Backend logs
docker-compose -f docker-compose.prod.yml logs -f backend

# Celery logs (background tasks)
docker-compose -f docker-compose.prod.yml logs -f celery

# All logs
docker-compose -f docker-compose.prod.yml logs -f
```

### **Restart Services**
```bash
# Restart everything
docker-compose -f docker-compose.prod.yml restart

# Restart specific service
docker-compose -f docker-compose.prod.yml restart backend
```

### **Stop Services**
```bash
docker-compose -f docker-compose.prod.yml down
```

### **Update Application**
```bash
# Pull latest code
git pull origin main

# Rebuild and restart
docker-compose -f docker-compose.prod.yml build
docker-compose -f docker-compose.prod.yml up -d

# Run new migrations (if any)
docker-compose -f docker-compose.prod.yml exec backend alembic upgrade head
```

---

## 🚨 Common Issues

### **Issue: "Cannot connect to database"**
**Solution:** Check if Neon database is active (not paused)

```bash
# Test connection
docker-compose -f docker-compose.prod.yml exec backend python -c "
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine

async def test():
    engine = create_async_engine('postgresql+asyncpg://neondb_owner:npg_F7s9MjvawfBn@ep-bitter-snow-ahon59hy-pooler.c-3.us-east-1.aws.neon.tech/placement_db?sslmode=require')
    async with engine.connect() as conn:
        result = await conn.execute('SELECT 1')
        print('✅ Database connected!')

asyncio.run(test())
"
```

### **Issue: "Port 8000 already in use"**
**Solution:** Stop conflicting service

```bash
# Find process using port 8000
sudo lsof -i :8000

# Kill the process
sudo kill -9 <PID>
```

### **Issue: "Celery not running tasks"**
**Solution:** Check Redis connection

```bash
# Test Redis
docker exec -it placement_redis redis-cli ping
# Should return: PONG

# Restart Celery
docker-compose -f docker-compose.prod.yml restart celery
```

---

## 🎉 Success Checklist

After deployment, verify:
- [ ] `curl http://localhost:8000/health` returns healthy status
- [ ] Database has tables: `docker-compose -f docker-compose.prod.yml exec backend python -c "from app.database import engine; print('OK')"`
- [ ] Celery is running: `docker-compose -f docker-compose.prod.yml logs celery | grep "ready"`
- [ ] Can access API docs: `http://YOUR_EC2_IP:8000/docs`
- [ ] Can register/login users via API
- [ ] Background tasks are processing (check celery logs)

---

## 📚 Reference

**Key Files:**
- `docker-compose.prod.yml` - Production Docker config
- `.env.production` - Production environment variables
- `alembic/versions/` - Database migration files
- `gunicorn.conf.py` - Gunicorn server config

**Key Commands:**
```bash
# Start
docker-compose -f docker-compose.prod.yml up -d

# Migrations
docker-compose -f docker-compose.prod.yml exec backend alembic upgrade head

# Logs
docker-compose -f docker-compose.prod.yml logs -f

# Stop
docker-compose -f docker-compose.prod.yml down
```

---

## 🔗 Next Steps

1. ✅ Configure EC2 Security Group (allow ports 80, 443, 8000)
2. ✅ Set up domain name and SSL certificate
3. ✅ Configure monitoring (CloudWatch, Sentry)
4. ✅ Set up automated backups
5. ✅ Configure CI/CD pipeline

---

**Need help?** Check `EC2_DEPLOYMENT_GUIDE.md` for detailed troubleshooting!
