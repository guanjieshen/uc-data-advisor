"""Chat API route with multi-agent orchestration and session memory."""

import logging
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from ..agents import Orchestrator
from ..memory import SessionMemory

logger = logging.getLogger(__name__)
router = APIRouter()
orchestrator = Orchestrator()


class ChatRequest(BaseModel):
    messages: list[dict]
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    agent: Optional[str] = None


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Process a chat message through the orchestrator."""
    messages = request.messages

    # Load conversation history from Lakebase if session_id provided
    if request.session_id and await SessionMemory.is_available():
        history = await SessionMemory.load_history(request.session_id)
        if history:
            # Prepend history, but cap total to last 10 exchanges (20 messages)
            messages = (history + messages)[-20:]

    try:
        response, agent_name = await orchestrator.route(messages)
    except Exception as e:
        logger.error(f"Orchestrator error: {e}", exc_info=True)
        response = "I'm sorry, I encountered an error processing your request. Please try rephrasing your question."
        agent_name = "error"

    # Save to Lakebase if session_id provided
    if request.session_id and await SessionMemory.is_available():
        last_user_msg = ""
        for msg in reversed(request.messages):
            if msg.get("role") == "user":
                last_user_msg = msg.get("content", "")
                break
        if last_user_msg:
            await SessionMemory.save_exchange(
                request.session_id, last_user_msg, response, agent=agent_name
            )

    return ChatResponse(response=response, agent=agent_name)


@router.get("/history/{session_id}")
async def get_history(session_id: str):
    """Retrieve conversation history for a session."""
    if not await SessionMemory.is_available():
        return {"messages": [], "available": False}

    messages = await SessionMemory.load_history(session_id, limit=50)
    return {"messages": messages, "available": True}
