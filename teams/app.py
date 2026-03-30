"""UC Data Advisor Teams Bot.

Receives messages from Microsoft Teams via Azure Bot Service,
forwards them to the Databricks orchestrator serving endpoint,
and returns the response.
"""

import logging
from os import environ

from microsoft_agents.hosting.core import (
    AgentApplication,
    TurnState,
    TurnContext,
    MemoryStorage,
)
from microsoft_agents.hosting.aiohttp import CloudAdapter
from aiohttp.web import Request, Response, Application, run_app
from microsoft_agents.hosting.aiohttp import (
    start_agent_process,
    jwt_authorization_middleware,
)
from microsoft_agents.hosting.core import AgentAuthConfiguration

from databricks_client import query_orchestrator
from config import PORT

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# --- Bot application ---

AGENT_APP = AgentApplication[TurnState](
    storage=MemoryStorage(), adapter=CloudAdapter()
)


@AGENT_APP.conversation_update("membersAdded")
async def on_members_added(context: TurnContext, _: TurnState):
    """Send welcome message when the bot is added to a conversation."""
    for member in context.activity.members_added:
        if member.id != context.activity.recipient.id:
            await context.send_activity(
                "Hello! I'm the **UC Data Advisor**. Ask me about datasets, "
                "metrics, or data governance in Unity Catalog.\n\n"
                "Try: *What catalogs are available?*"
            )
    return True


@AGENT_APP.activity("message")
async def on_message(context: TurnContext, _: TurnState):
    """Forward user messages to the Databricks orchestrator endpoint."""
    # Strip @mention text from Teams messages
    text = context.activity.text or ""
    if context.activity.recipient and context.activity.recipient.id:
        cleaned = context.activity.remove_mention_text(context.activity.recipient.id)
        if cleaned:
            text = cleaned
    text = text.strip()

    if not text:
        return

    # Show typing indicator while processing
    await context.send_activity({"type": "typing"})

    logger.info(f"User message: {text[:100]}")

    # Call the Databricks orchestrator
    response = query_orchestrator(text)

    logger.info(f"Response: {response[:100]}")
    await context.send_activity(response)


# --- Web server ---

def start_server():
    async def entry_point(req: Request) -> Response:
        agent: AgentApplication = req.app["agent_app"]
        adapter: CloudAdapter = req.app["adapter"]
        return await start_agent_process(req, agent, adapter)

    app = Application(middlewares=[jwt_authorization_middleware])
    app.router.add_post("/api/messages", entry_point)
    app.router.add_get("/api/messages", lambda _: Response(status=200, text="OK"))

    # Auth configuration (None = anonymous for local testing)
    auth_config = None
    client_id = environ.get("CONNECTIONS__SERVICE_CONNECTION__SETTINGS__CLIENTID", "")
    if client_id:
        auth_config = AgentAuthConfiguration()

    app["agent_configuration"] = auth_config
    app["agent_app"] = AGENT_APP
    app["adapter"] = AGENT_APP.adapter

    logger.info(f"Starting Teams bot on port {PORT}")
    logger.info(f"Auth: {'configured' if client_id else 'anonymous (local testing)'}")
    run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    start_server()
