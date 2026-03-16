"""Lakebase session memory for conversation persistence."""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Connection pool — initialized lazily
_pool = None


async def _get_pool():
    """Get or create the asyncpg connection pool."""
    global _pool
    if _pool is None:
        import asyncpg
        import ssl

        # Lakebase requires SSL
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        _pool = await asyncpg.create_pool(
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            database=os.environ.get("PGDATABASE", "uc_advisor_sessions"),
            user=os.environ.get("PGUSER", ""),
            password=os.environ.get("PGPASSWORD", ""),
            ssl=ssl_ctx,
            min_size=1,
            max_size=5,
        )
    return _pool


class SessionMemory:
    """Manages conversation history in Lakebase (Postgres)."""

    @staticmethod
    async def load_history(session_id: str, limit: int = 10) -> list[dict]:
        """Load the most recent messages for a session."""
        try:
            pool = await _get_pool()
            rows = await pool.fetch(
                """
                SELECT role, content, agent
                FROM conversations
                WHERE session_id = $1
                ORDER BY message_index DESC
                LIMIT $2
                """,
                session_id,
                limit,
            )
            # Reverse to chronological order
            messages = []
            for row in reversed(rows):
                msg = {"role": row["role"], "content": row["content"]}
                if row["agent"]:
                    msg["agent"] = row["agent"]
                messages.append(msg)
            return messages
        except Exception as e:
            logger.warning(f"Failed to load history for {session_id}: {e}")
            return []

    @staticmethod
    async def save_exchange(
        session_id: str,
        user_message: str,
        assistant_message: str,
        agent: Optional[str] = None,
    ):
        """Save a user/assistant exchange to the session."""
        try:
            pool = await _get_pool()

            # Get next message index
            max_idx = await pool.fetchval(
                "SELECT COALESCE(MAX(message_index), -1) FROM conversations WHERE session_id = $1",
                session_id,
            )
            next_idx = max_idx + 1

            await pool.executemany(
                """
                INSERT INTO conversations (session_id, message_index, role, content, agent)
                VALUES ($1, $2, $3, $4, $5)
                """,
                [
                    (session_id, next_idx, "user", user_message, None),
                    (session_id, next_idx + 1, "assistant", assistant_message, agent),
                ],
            )
        except Exception as e:
            logger.warning(f"Failed to save exchange for {session_id}: {e}")

    @staticmethod
    async def is_available() -> bool:
        """Check if Lakebase connection is configured and reachable."""
        if not os.environ.get("PGHOST"):
            return False
        try:
            pool = await _get_pool()
            await pool.fetchval("SELECT 1")
            return True
        except Exception:
            return False
