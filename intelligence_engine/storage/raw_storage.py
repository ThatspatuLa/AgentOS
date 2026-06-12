"""
Raw Storage — Staging table for collected data before processing.
"""

from datetime import datetime
from uuid import UUID
from typing import AsyncIterator
import asyncpg
import json


class RawStorage:
    """Store raw collection results before processing."""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
    
    async def write(
        self,
        source_id: UUID,
        source_record_id: str,
        raw_data: dict,
        metadata: dict,
        collector_version: str = "1.0"
    ):
        """Write collection result to raw storage."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO raw_collection_events (
                    source_id, source_record_id, raw_data, collected_at,
                    collector_version, metadata, status
                ) VALUES ($1, $2, $3, $4, $5, $6, 'pending')
                ON CONFLICT (source_id, source_record_id, collected_at) DO NOTHING
            """,
                source_id,
                source_record_id,
                json.dumps(raw_data),
                datetime.utcnow(),
                collector_version,
                json.dumps(metadata)
            )
    
    async def read_unprocessed(
        self, source_id: UUID, batch_size: int = 100
    ) -> AsyncIterator[list[dict]]:
        """Read unprocessed records in batches."""
        async with self.pool.acquire() as conn:
            while True:
                rows = await conn.fetch("""
                    SELECT id, source_id, source_record_id, raw_data, collected_at,
                           collector_version, metadata
                    FROM raw_collection_events
                    WHERE source_id = $1 AND status = 'pending'
                    ORDER BY collected_at ASC
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                """, source_id, batch_size)
                
                if not rows:
                    break
                
                yield [dict(r) for r in rows]
    
    async def mark_processed(self, ids: list[int]):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE raw_collection_events
                SET status = 'processed', processed_at = NOW()
                WHERE id = ANY($1)
            """, ids)
    
    async def mark_failed(self, id: int, error: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE raw_collection_events
                SET status = 'failed', error = $1, failed_at = NOW()
                WHERE id = $2
            """, error, id)