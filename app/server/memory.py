"""Lakebase session memory and feedback storage."""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Connection pool — initialized lazily
_pool = None
_tables_ensured = False


def _get_lakebase_token() -> str:
    """Generate a fresh Lakebase JWT token via Databricks SDK."""
    from .config import get_workspace_client
    client = get_workspace_client()
    instance_name = os.environ.get("LAKEBASE_INSTANCE", "uc-advisor-sessions")
    cred = client.database.generate_database_credential(instance_names=[instance_name])
    return cred.token


async def _get_pool():
    """Get or create the asyncpg connection pool."""
    global _pool, _tables_ensured
    if _pool is None:
        import asyncpg
        import ssl

        # Lakebase requires SSL
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        token = _get_lakebase_token()
        # PGUSER is the SP client ID when running as a Databricks App
        user = os.environ.get("PGUSER", os.environ.get("DATABRICKS_CLIENT_ID", ""))

        _pool = await asyncpg.create_pool(
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            database=os.environ.get("PGDATABASE", "uc_advisor_sessions"),
            user=user,
            password=token,
            ssl=ssl_ctx,
            min_size=1,
            max_size=5,
        )

    if not _tables_ensured:
        try:
            await _ensure_tables(_pool)
        except Exception as e:
            logger.warning(f"Failed to ensure tables: {e}")
        _tables_ensured = True

    return _pool


async def _ensure_tables(pool):
    """Create tables if they don't exist."""
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            session_id TEXT NOT NULL,
            message_index INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            agent TEXT,
            PRIMARY KEY (session_id, message_index)
        )
    """)
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            message_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            rating SMALLINT NOT NULL,
            comment TEXT,
            agent TEXT,
            question TEXT,
            answer TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)


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
    async def save_feedback(
        message_id: str,
        session_id: str,
        rating: int,
        comment: Optional[str] = None,
        agent: Optional[str] = None,
        question: Optional[str] = None,
        answer: Optional[str] = None,
    ):
        """Save or update feedback for a message."""
        pool = await _get_pool()
        await pool.execute(
            """
            INSERT INTO feedback (message_id, session_id, rating, comment, agent, question, answer)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (message_id)
            DO UPDATE SET rating = $3, comment = $4, updated_at = NOW()
            """,
            message_id, session_id, rating, comment, agent, question, answer,
        )

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
