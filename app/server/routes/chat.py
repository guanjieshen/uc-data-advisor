"""Chat API route with multi-agent orchestration, session memory, and feedback."""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
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
    message_id: str = ""


class FeedbackRequest(BaseModel):
    message_id: str
    rating: int
    comment: Optional[str] = None
    session_id: Optional[str] = None
    agent: Optional[str] = None
    question: Optional[str] = None
    answer: Optional[str] = None


class FeedbackResponse(BaseModel):
    status: str


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Process a chat message through the orchestrator."""
    message_id = str(uuid.uuid4())
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

    return ChatResponse(response=response, agent=agent_name, message_id=message_id)


@router.post("/feedback", response_model=FeedbackResponse)
async def submit_feedback(request: FeedbackRequest):
    """Submit thumbs up/down feedback on an assistant response."""
    if request.rating not in (1, -1):
        raise HTTPException(status_code=400, detail="rating must be 1 or -1")

    try:
        await SessionMemory.save_feedback(
            message_id=request.message_id,
            session_id=request.session_id or "",
            rating=request.rating,
            comment=request.comment,
            agent=request.agent,
            question=request.question,
            answer=request.answer,
        )
    except Exception as e:
        logger.error(f"Feedback save failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return FeedbackResponse(status="ok")


@router.get("/ui-config")
async def get_ui_config():
    """Return UI configuration (header subtitle, suggestion questions)."""
    from ..advisor_config import get_ui
    ui = get_ui()
    return {
        "header_subtitle": ui.get("header_subtitle", "Unity Catalog"),
        "suggestions": ui.get("suggestions", ["What catalogs are available?"]),
    }


@router.get("/history/{session_id}")
async def get_history(session_id: str):
    """Retrieve conversation history for a session."""
    if not await SessionMemory.is_available():
        return {"messages": [], "available": False}

    messages = await SessionMemory.load_history(session_id, limit=50)
    return {"messages": messages, "available": True}
