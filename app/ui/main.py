"""UC Data Advisor — Databricks App chat UI.

Thin FastAPI proxy in front of the orchestrator serving endpoint. The Databricks
Apps runtime injects the app's service-principal credentials via env vars, so
WorkspaceClient() picks them up automatically with no further auth plumbing.

Routes:
  GET  /                  → static chat UI
  POST /api/chat          → invoke orchestrator endpoint with the user's message
  GET  /api/health        → liveness probe
"""

import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORCHESTRATOR_ENDPOINT = os.environ.get("ORCHESTRATOR_ENDPOINT", "")
if not ORCHESTRATOR_ENDPOINT:
    logger.warning("ORCHESTRATOR_ENDPOINT env var is empty — /api/chat will 503 until set")

# Auto-auth via the app's service principal (Databricks Apps runtime sets env vars).
_workspace: WorkspaceClient | None = None


def _ws() -> WorkspaceClient:
    global _workspace
    if _workspace is None:
        _workspace = WorkspaceClient()
    return _workspace


app = FastAPI(title="UC Data Advisor")


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


@app.get("/api/health")
async def health() -> dict:
    return {
        "status": "ok",
        "orchestrator_endpoint": ORCHESTRATOR_ENDPOINT or "<unset>",
    }


@app.post("/api/chat")
async def chat(req: ChatRequest, request: Request) -> dict:
    if not ORCHESTRATOR_ENDPOINT:
        raise HTTPException(status_code=503, detail="ORCHESTRATOR_ENDPOINT not configured")

    user_email = request.headers.get("X-Forwarded-Email", "anonymous")
    logger.info("chat from %s: %s", user_email, req.message[:120])

    try:
        resp = _ws().api_client.do(
            "POST",
            f"/serving-endpoints/{ORCHESTRATOR_ENDPOINT}/invocations",
            body={
                "input": [{"role": "user", "content": req.message}],
                "context": {
                    "conversation_id": req.conversation_id or "",
                    "user_id": user_email,
                },
            },
        )
    except Exception as e:
        logger.exception("orchestrator call failed")
        raise HTTPException(status_code=502, detail=f"orchestrator: {e}") from e

    text = ""
    for item in (resp.get("output") or []):
        if item.get("type") == "message":
            for c in (item.get("content") or []):
                if c.get("type") == "output_text":
                    text = c.get("text", "")

    return {
        "response": text or "(no response)",
        "user_email": user_email,
    }


# Static UI lives at /static/, but we want / to return the chat page directly.
_STATIC = Path(__file__).parent / "static"


@app.get("/")
async def root() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


app.mount("/static", StaticFiles(directory=_STATIC), name="static")
