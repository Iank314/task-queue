from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from broker.models import Base
import os

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # create_all only creates missing tables -- it won't ALTER an existing
        # one. These idempotent statements bring an older `jobs` table up to
        # date with the lease columns/index without a full migration tool.
        await conn.execute(
            text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ")
        )
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_jobs_expired_lease "
                "ON jobs (lease_expires_at) WHERE status = 'running'"
            )
        )


@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session
