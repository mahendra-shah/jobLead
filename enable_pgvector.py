"""
Quick script to enable pgvector extension in Neon PostgreSQL
Run this BEFORE starting the application
"""
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.config import settings

async def enable_pgvector():
    """Enable pgvector extension in the database"""
    print("🔌 Connecting to Neon PostgreSQL...")
    engine = create_async_engine(str(settings.DATABASE_URL))
    
    async with engine.begin() as conn:
        print("✅ Connected!")
        print("📦 Enabling pgvector extension...")
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
        print("✅ pgvector extension enabled!")
        
        # Verify
        result = await conn.execute(text("SELECT extname FROM pg_extension WHERE extname = 'vector';"))
        if result.fetchone():
            print("✅ Verification successful: pgvector is installed")
        else:
            print("❌ Verification failed: pgvector not found")
    
    await engine.dispose()
    print("🎉 Done!")

if __name__ == "__main__":
    asyncio.run(enable_pgvector())
