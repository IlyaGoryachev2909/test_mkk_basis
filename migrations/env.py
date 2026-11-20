import asyncio
import os

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.db import Base


def migrate(connection=None):
    context.configure(
        connection=connection, url=os.environ["DATABASE_URL"], target_metadata=Base.metadata,
        literal_binds=connection is None,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_online():
    engine = create_async_engine(os.environ["DATABASE_URL"], poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(migrate)
    await engine.dispose()


if context.is_offline_mode():
    migrate()
else:
    asyncio.run(run_online())
