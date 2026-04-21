"""Deploy the UC Data Advisor Teams bot to Azure.

Creates all required Azure resources, configures the bot, and deploys the code.

Usage:
  python teams/deploy.py --config teams/teams_config.yaml
  python teams/deploy.py --config teams/teams_config.yaml --step teardown
"""

import argparse
import json
import os
import subprocess
import sys
import time
import yaml


def _az(args: list[str], description: str = "", check: bool = True) -> subprocess.CompletedProcess:
    """Run an Azure CLI command."""
    cmd = ["az"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        err = result.stderr[:500]
        if description:
            print(f"  FAILED: {description} — {err}")
        else:
            print(f"  FAILED: {err}")
    return result


def deploy(config: dict) -> None:
    """Deploy the Teams bot end-to-end."""
    azure = config.get("azure", {})
    bot = config.get("bot", {})
    ad = config.get("azure_ad", {})
    db = config.get("databricks", {})

    sub_id = azure["subscription_id"]
    rg = azure["resource_group"]
    location = azure.get("location", "canadacentral")
    owner_tag = azure.get("owner_tag", "")

    bot_name = bot["name"]
    web_app_name = bot.get("web_app_name", bot_name)
    plan_name = bot.get("app_service_plan", f"{bot_name}-plan")
    sku = bot.get("sku", "B1")
    runtime = bot.get("runtime", "PYTHON:3.13")

    tags = f"owner={owner_tag}" if owner_tag else ""

    print("=" * 60)
    print("UC Data Advisor — Teams Bot Deployment")
    print("=" * 60)
    print(f"  Resource group: {rg} ({location})")
    print(f"  Bot name:       {bot_name}")
    print(f"  Web app:        {web_app_name}.azurewebsites.net")
    print()

    # Step 1: Resource group
    print("[1/8] Resource group...", end=" ", flush=True)
    r = _az(["group", "show", "--name", rg], check=False)
    if r.returncode != 0:
        args = ["group", "create", "--name", rg, "--location", location]
        if tags:
            args += ["--tags", tags]
        _az(args, "create resource group")
        print("created")
    else:
        print("exists")

    # Step 2: App Service Plan
    print("[2/8] App Service Plan...", end=" ", flush=True)
    r = _az(["appservice", "plan", "show", "--name", plan_name, "--resource-group", rg], check=False)
    if r.returncode != 0:
        args = ["appservice", "plan", "create", "--name", plan_name, "--resource-group", rg,
                "--location", location, "--sku", sku, "--is-linux"]
        if tags:
            args += ["--tags", tags]
        _az(args, "create app service plan")
        print("created")
    else:
        print("exists")

    # Step 3: Web App
    print("[3/8] Web App...", end=" ", flush=True)
    r = _az(["webapp", "show", "--name", web_app_name, "--resource-group", rg], check=False)
    if r.returncode != 0:
        args = ["webapp", "create", "--name", web_app_name, "--resource-group", rg,
                "--plan", plan_name, "--runtime", runtime]
        if tags:
            args += ["--tags", tags]
        _az(args, "create web app")
        print("created")
    else:
        print("exists")

    # Step 4: App Registration
    print("[4/8] App Registration...", end=" ", flush=True)
    app_id = ad.get("app_id", "")
    tenant_id = ad.get("tenant_id", "")
    client_secret = ad.get("client_secret", "")

    if not app_id:
        r = _az(["ad", "app", "create", "--display-name", f"{bot_name} Bot",
                  "--sign-in-audience", "AzureADMyOrg", "-o", "json"])
        if r.returncode == 0:
            app_data = json.loads(r.stdout)
            app_id = app_data["appId"]
            print(f"created ({app_id})")
        else:
            return
    else:
        print(f"using {app_id[:20]}...")

    if not tenant_id:
        r = _az(["account", "show", "-o", "json"], check=False)
        if r.returncode == 0:
            tenant_id = json.loads(r.stdout)["tenantId"]

    if not client_secret:
        r = _az(["ad", "app", "credential", "reset", "--id", app_id, "--append",
                  "--query", "password", "-o", "tsv"])
        if r.returncode == 0:
            client_secret = r.stdout.strip()

    # Create service principal for the app
    _az(["ad", "sp", "create", "--id", app_id], check=False)

    # Save back to config
    config.setdefault("azure_ad", {})
    config["azure_ad"]["app_id"] = app_id
    config["azure_ad"]["tenant_id"] = tenant_id
    config["azure_ad"]["client_secret"] = client_secret

    # Step 5: Azure Bot
    print("[5/8] Azure Bot...", end=" ", flush=True)
    r = _az(["resource", "show", "--resource-group", rg,
             "--resource-type", "Microsoft.BotService/botServices", "--name", bot_name], check=False)
    if r.returncode != 0:
        props = json.dumps({
            "location": "global",
            "sku": {"name": "F0"},
            "kind": "azurebot",
            "tags": {"owner": owner_tag} if owner_tag else {},
            "properties": {
                "displayName": f"UC Data Advisor Bot",
                "endpoint": f"https://{web_app_name}.azurewebsites.net/api/messages",
                "msaAppId": app_id,
                "msaAppTenantId": tenant_id,
                "msaAppType": "SingleTenant",
            }
        })
        _az(["resource", "create", "--resource-group", rg,
             "--resource-type", "Microsoft.BotService/botServices",
             "--name", bot_name, "--is-full-object", "--properties", props], "create bot")
        print("created")
    else:
        print("exists")

    # Step 6: Teams Channel
    print("[6/8] Teams Channel...", end=" ", flush=True)
    channel_body = json.dumps({
        "location": "global",
        "properties": {
            "channelName": "MsTeamsChannel",
            "properties": {"isEnabled": True}
        }
    })
    _az(["rest", "--method", "PUT",
         "--uri", f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.BotService/botServices/{bot_name}/channels/MsTeamsChannel?api-version=2022-09-15",
         "--body", channel_body], check=False)
    print("enabled")

    # Step 7: Web App Environment Variables
    print("[7/8] Environment variables...", end=" ", flush=True)
    _az(["webapp", "config", "appsettings", "set",
         "--name", web_app_name, "--resource-group", rg,
         "--settings",
         f"DATABRICKS_HOST={db['host']}",
         f"DATABRICKS_SP_CLIENT_ID={db.get('sp_client_id', '')}",
         f"DATABRICKS_SP_CLIENT_SECRET={db.get('sp_client_secret', '')}",
         f"SERVING_ENDPOINT_NAME={db['orchestrator_endpoint']}",
         f"ADVISOR_CATALOG={db.get('advisor_catalog', '')}",
         f"WAREHOUSE_ID={db.get('warehouse_id', '')}",
         f"VS_INDEX_KNOWLEDGE={db.get('vs_index_knowledge', '')}",
         f"MicrosoftAppId={app_id}",
         f"MicrosoftAppPassword={client_secret}",
         "MicrosoftAppType=singletenant",
         f"MicrosoftTenantId={tenant_id}",
         "SCM_DO_BUILD_DURING_DEPLOYMENT=True",
         ], check=False)
    _az(["webapp", "config", "set", "--name", web_app_name, "--resource-group", rg,
         "--startup-file", "python3 app.py"], check=False)
    print("set")

    # Step 8: Deploy bot code
    print("[8/8] Deploying bot code...", end=" ", flush=True)

    # Clone the Databricks bot repo if not already present
    bot_code_dir = os.path.join(os.path.dirname(__file__), ".bot-code")
    if not os.path.exists(bot_code_dir):
        subprocess.run(["git", "clone", "https://github.com/databricks-solutions/teams-databricks-bot-service.git",
                         bot_code_dir], capture_output=True)

    # Write our simplified app.py
    teams_bot_dir = os.path.join(bot_code_dir, "teams-bot")
    _write_bot_app(teams_bot_dir)

    # Add databricks-sdk to requirements
    req_path = os.path.join(teams_bot_dir, "requirements.txt")
    with open(req_path, "r") as f:
        reqs = f.read()
    if "databricks-sdk" not in reqs:
        with open(req_path, "a") as f:
            f.write("\ndatabricks-sdk>=0.50.0\n")

    # Zip and deploy
    import zipfile
    zip_path = os.path.join(os.path.dirname(__file__), ".bot-deploy.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(teams_bot_dir):
            dirs[:] = [d for d in dirs if d not in (".git", "__pycache__", "bots", "dialogs")]
            for file in files:
                if file.endswith(".pyc"):
                    continue
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, teams_bot_dir)
                zf.write(filepath, arcname)

    r = _az(["webapp", "deployment", "source", "config-zip",
             "--name", web_app_name, "--resource-group", rg,
             "--src", zip_path], check=False)
    if r.returncode == 0 or "Timeout" in r.stderr:
        print("deployed (build may still be in progress)")
    else:
        print(f"failed: {r.stderr[:200]}")

    print()
    print("=" * 60)
    print("Teams Bot Deployment Complete")
    print("=" * 60)
    print(f"  Bot URL: https://{web_app_name}.azurewebsites.net")
    print(f"  Test: Azure Portal → Azure Bot '{bot_name}' → Test in Web Chat")
    print(f"  Teams: Azure Portal → Azure Bot '{bot_name}' → Channels → Teams → Open in Teams")


def teardown(config: dict) -> None:
    """Delete all Teams bot Azure resources."""
    azure = config.get("azure", {})
    bot = config.get("bot", {})
    ad = config.get("azure_ad", {})

    rg = azure["resource_group"]
    bot_name = bot["name"]
    web_app_name = bot.get("web_app_name", bot_name)
    plan_name = bot.get("app_service_plan", f"{bot_name}-plan")
    app_id = ad.get("app_id", "")

    print("=" * 60)
    print("Teams Bot Teardown")
    print("=" * 60)

    print("[1/4] Azure Bot...", end=" ", flush=True)
    _az(["resource", "delete", "--resource-group", rg,
         "--resource-type", "Microsoft.BotService/botServices",
         "--name", bot_name], check=False)
    print("deleted")

    print("[2/4] Web App...", end=" ", flush=True)
    _az(["webapp", "delete", "--name", web_app_name, "--resource-group", rg], check=False)
    print("deleted")

    print("[3/4] App Service Plan...", end=" ", flush=True)
    _az(["appservice", "plan", "delete", "--name", plan_name,
         "--resource-group", rg, "--yes"], check=False)
    print("deleted")

    if app_id:
        print("[4/4] App Registration...", end=" ", flush=True)
        _az(["ad", "app", "delete", "--id", app_id], check=False)
        print("deleted")

    print()
    print("Teardown complete")


def _write_bot_app(bot_dir: str) -> None:
    """Write the bot app.py with Adaptive Card feedback and auto-KB updates."""
    app_code = '''"""UC Data Advisor Teams Bot — feedback-enabled with Adaptive Cards."""

import os
import sys
import json
import asyncio
import logging
import traceback
import time as _time
from http import HTTPStatus
from uuid import uuid4

from aiohttp import web
from botbuilder.core import TurnContext, ActivityHandler
from botbuilder.integration.aiohttp import CloudAdapter, ConfigurationBotFrameworkAuthentication
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity, ActivityTypes, Attachment

from databricks.sdk import WorkspaceClient
from config import DefaultConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = DefaultConfig()
ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

# In-memory state
_conversations = {}   # teams_conversation_id -> conversation_id (UUID)
_messages = {}        # message_id -> {user_message, agent_response}
_kb_updates = {"count": 0, "reset_time": 0}

_db_client = None


def get_db_client():
    global _db_client
    if _db_client is None:
        host = os.environ.get("DATABRICKS_HOST", "")
        client_id = os.environ.get("DATABRICKS_SP_CLIENT_ID", "")
        client_secret = os.environ.get("DATABRICKS_SP_CLIENT_SECRET", "")
        if host and client_id and client_secret:
            _db_client = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        else:
            _db_client = WorkspaceClient()
    return _db_client


def _esc(s):
    """Escape strings for SQL single-quoted literals."""
    if s is None:
        return ""
    return str(s).replace("\\\\", "\\\\\\\\").replace("'", "''")


def _run_sql(statement):
    """Execute SQL via Statements API. Returns None if WAREHOUSE_ID not set."""
    wh = os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        return None
    w = get_db_client()
    return w.statement_execution.execute_statement(
        warehouse_id=wh, statement=statement, wait_timeout="30s",
    )


def query_orchestrator(message):
    """Call the orchestrator endpoint. Returns (response_text, request_id)."""
    w = get_db_client()
    endpoint = os.environ.get("SERVING_ENDPOINT_NAME", "")
    if not endpoint:
        return "SERVING_ENDPOINT_NAME not configured.", None
    try:
        resp = w.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint}/invocations",
            body={"input": [{"role": "user", "content": message}]},
        )
    except Exception as e:
        logger.error(f"Orchestrator call failed: {e}")
        return f"Sorry, I encountered an error: {str(e)[:200]}", None

    request_id = resp.get("request_id")
    for item in resp.get("output", []):
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    return c.get("text", ""), request_id
    return "I received your message but could not generate a response.", request_id


def _resolve_conversation_id(teams_conv_id):
    """Get or create a stable conversation ID for a Teams conversation."""
    if teams_conv_id not in _conversations:
        _conversations[teams_conv_id] = str(uuid4())
    return _conversations[teams_conv_id]


def build_response_card(text, message_id):
    """Build Adaptive Card with response text and feedback buttons."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {"type": "TextBlock", "text": text, "wrap": True, "size": "Default"}
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "👍 Helpful",
                "data": {"action": "feedback", "message_id": message_id, "rating": "positive"},
            },
            {
                "type": "Action.Submit",
                "title": "👎 Not helpful",
                "data": {"action": "feedback", "message_id": message_id, "rating": "negative"},
            },
        ],
    }


def build_comment_card(message_id, conversation_id):
    """Build Adaptive Card asking for correction text after negative feedback."""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "Thanks for the feedback. What would have been a better answer?",
                "wrap": True,
            },
            {
                "type": "Input.Text",
                "id": "comment_text",
                "placeholder": "Your correction or suggestion...",
                "isMultiline": True,
            },
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "Submit",
                "data": {
                    "action": "comment",
                    "message_id": message_id,
                    "conversation_id": conversation_id,
                },
            },
        ],
    }


# ---- Async logging (fire-and-forget, never blocks response) ----

async def _log_message(message_id, conversation_id, user_message, agent_response,
                       response_time_ms, teams_activity_id=None, request_id=None,
                       error_text=None):
    catalog = os.environ.get("ADVISOR_CATALOG", "")
    if not catalog:
        return
    try:
        t = f"{catalog}.default"
        err = "NULL" if not error_text else f"\\'{_esc(error_text)}\\'"
        req = "NULL" if not request_id else f"\\'{_esc(request_id)}\\'"
        act = "NULL" if not teams_activity_id else f"\\'{_esc(teams_activity_id)}\\'"
        sql = (
            f"INSERT INTO {t}.messages VALUES ("
            f"\\'{_esc(message_id)}\\', \\'{_esc(conversation_id)}\\', "
            f"\\'{_esc(user_message)}\\', \\'{_esc(agent_response)}\\', "
            f"NULL, {response_time_ms}, current_timestamp(), "
            f"{act}, {req}, {err})"
        )
        await asyncio.to_thread(_run_sql, sql)
    except Exception as e:
        logger.error(f"Failed to log message: {e}")


async def _log_conversation(conversation_id, teams_conversation_id,
                            teams_user_id, teams_user_name):
    catalog = os.environ.get("ADVISOR_CATALOG", "")
    if not catalog:
        return
    try:
        t = f"{catalog}.default"
        sql = (
            f"MERGE INTO {t}.conversations AS target "
            f"USING (SELECT \\'{_esc(conversation_id)}\\' AS conversation_id) AS source "
            f"ON target.conversation_id = source.conversation_id "
            f"WHEN MATCHED THEN UPDATE SET "
            f"  last_activity_at = current_timestamp(), "
            f"  message_count = message_count + 1 "
            f"WHEN NOT MATCHED THEN INSERT "
            f"  (conversation_id, teams_conversation_id, teams_user_id, "
            f"   teams_user_name, started_at, last_activity_at, message_count) "
            f"VALUES (\\'{_esc(conversation_id)}\\', \\'{_esc(teams_conversation_id)}\\', "
            f"\\'{_esc(teams_user_id)}\\', \\'{_esc(teams_user_name)}\\', "
            f"current_timestamp(), current_timestamp(), 1)"
        )
        await asyncio.to_thread(_run_sql, sql)
    except Exception as e:
        logger.error(f"Failed to log conversation: {e}")


async def _log_feedback(feedback_id, message_id, conversation_id, rating,
                        teams_user_id, comment=None):
    catalog = os.environ.get("ADVISOR_CATALOG", "")
    if not catalog:
        return
    try:
        t = f"{catalog}.default"
        cmt = "NULL" if not comment else f"\\'{_esc(comment)}\\'"
        sql = (
            f"INSERT INTO {t}.feedback VALUES ("
            f"\\'{_esc(feedback_id)}\\', \\'{_esc(message_id)}\\', "
            f"\\'{_esc(conversation_id)}\\', \\'{_esc(rating)}\\', "
            f"{cmt}, \\'{_esc(teams_user_id)}\\', current_timestamp())"
        )
        await asyncio.to_thread(_run_sql, sql)
    except Exception as e:
        logger.error(f"Failed to log feedback: {e}")


# ---- Auto-KB update (Phase 2: feedback loop closure) ----

async def _auto_update_knowledge_base(user_message, agent_response, correction):
    """Validate correction via LLM and auto-insert/update knowledge base."""
    now = _time.time()
    if now - _kb_updates["reset_time"] > 3600:
        _kb_updates["count"] = 0
        _kb_updates["reset_time"] = now
    if _kb_updates["count"] >= 10:
        logger.info("KB auto-update rate limit reached (10/hr)")
        return

    catalog = os.environ.get("ADVISOR_CATALOG", "")
    vs_index = os.environ.get("VS_INDEX_KNOWLEDGE", "")
    if not catalog:
        return

    prompt = (
        "You are a data quality validator. Given the following interaction, "
        "determine if the user correction is valid and generate a knowledge base entry.\\n\\n"
        f"User question: {user_message}\\n"
        f"Agent response: {agent_response}\\n"
        f"User correction: {correction}\\n\\n"
        "Return ONLY a JSON object: "
        '{"question": "...", "answer": "...", "category": "...", '
        '"is_valid": true, "confidence": 0.85}'
    )

    validation_text, _ = query_orchestrator(prompt)
    try:
        clean = validation_text.strip()
        if clean.startswith("```"):
            clean = clean.split("\\n", 1)[1].rsplit("```", 1)[0].strip()
        result = json.loads(clean)
    except (json.JSONDecodeError, IndexError, ValueError):
        logger.warning(f"KB validation parse failed: {validation_text[:200]}")
        return

    if not result.get("is_valid") or result.get("confidence", 0) < 0.8:
        logger.info(f"KB correction rejected: valid={result.get('is_valid')}, conf={result.get('confidence')}")
        return

    question = result["question"]
    answer = result["answer"]
    category = result.get("category", "user_feedback")
    t = f"{catalog}.default"

    def _do_update():
        w = get_db_client()
        # Dedup: check for similar existing entry via VS similarity search
        if vs_index:
            try:
                vs_resp = w.vector_search_indexes.query_index(
                    index_name=vs_index, query_text=question,
                    columns=["id", "question"], num_results=1,
                    score_threshold=0.9,
                )
                rows = getattr(vs_resp.result, "data_array", None) or []
                if rows:
                    existing_id = rows[0][0]
                    _run_sql(
                        f"UPDATE {t}.knowledge_base SET answer = \\'{_esc(answer)}\\', "
                        f"source = \\'auto_feedback\\' WHERE id = {existing_id}"
                    )
                    logger.info(f"KB entry updated (id={existing_id})")
                    return
            except Exception as e:
                logger.warning(f"VS dedup check failed: {e}")

        # Insert new entry with auto-incremented ID
        search_text = f"{question} {answer}"
        _run_sql(
            f"INSERT INTO {t}.knowledge_base "
            f"SELECT COALESCE(MAX(id), 0) + 1, "
            f"\\'{_esc(question)}\\', \\'{_esc(answer)}\\', "
            f"\\'{_esc(category)}\\', \\'auto_feedback\\', "
            f"\\'{_esc(search_text)}\\' "
            f"FROM {t}.knowledge_base"
        )
        logger.info(f"KB entry inserted: {question[:80]}")

    try:
        await asyncio.to_thread(_do_update)
        _kb_updates["count"] += 1
    except Exception as e:
        logger.error(f"KB auto-update failed: {e}")


# ---- Bot handler ----

class AdvisorBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        # Handle Adaptive Card button clicks (feedback / comment submissions)
        if turn_context.activity.value:
            await self._handle_card_action(turn_context)
            return

        text = (turn_context.activity.text or "").strip()
        if not text:
            return

        await turn_context.send_activity(Activity(type=ActivityTypes.typing))

        message_id = str(uuid4())
        teams_conv_id = turn_context.activity.conversation.id if turn_context.activity.conversation else ""
        conversation_id = _resolve_conversation_id(teams_conv_id)
        user_id = ""
        user_name = ""
        if turn_context.activity.from_property:
            user_id = getattr(turn_context.activity.from_property, "aad_object_id", "") or turn_context.activity.from_property.id or ""
            user_name = turn_context.activity.from_property.name or ""

        logger.info(f"User ({user_name}): {text[:100]}")

        start = _time.time()
        response, request_id = query_orchestrator(text)
        elapsed_ms = int((_time.time() - start) * 1000)

        logger.info(f"Response ({elapsed_ms}ms): {response[:100]}")

        # Cache for potential KB update on negative feedback
        _messages[message_id] = {"user_message": text, "agent_response": response}

        # Send Adaptive Card with feedback buttons
        card = build_response_card(response, message_id)
        attachment = Attachment(
            content_type="application/vnd.microsoft.card.adaptive",
            content=card,
        )
        await turn_context.send_activity(
            Activity(type=ActivityTypes.message, attachments=[attachment])
        )

        # Fire-and-forget: log to Delta tables
        activity_id = turn_context.activity.id or ""
        asyncio.create_task(_log_message(
            message_id, conversation_id, text, response,
            elapsed_ms, activity_id, request_id,
        ))
        asyncio.create_task(_log_conversation(
            conversation_id, teams_conv_id, user_id, user_name,
        ))

    async def _handle_card_action(self, turn_context):
        """Handle feedback button clicks and comment submissions."""
        value = turn_context.activity.value or {}
        action = value.get("action", "")
        user_id = ""
        if turn_context.activity.from_property:
            user_id = getattr(turn_context.activity.from_property, "aad_object_id", "") or turn_context.activity.from_property.id or ""

        if action == "feedback":
            message_id = value.get("message_id", "")
            rating = value.get("rating", "")
            teams_conv_id = turn_context.activity.conversation.id if turn_context.activity.conversation else ""
            conversation_id = _resolve_conversation_id(teams_conv_id)

            asyncio.create_task(_log_feedback(
                str(uuid4()), message_id, conversation_id, rating, user_id,
            ))

            if rating == "negative":
                # Ask for correction details
                card = build_comment_card(message_id, conversation_id)
                attachment = Attachment(
                    content_type="application/vnd.microsoft.card.adaptive",
                    content=card,
                )
                await turn_context.send_activity(
                    Activity(type=ActivityTypes.message, attachments=[attachment])
                )
            else:
                await turn_context.send_activity("Thanks for your feedback!")

        elif action == "comment":
            message_id = value.get("message_id", "")
            conversation_id = value.get("conversation_id", "")
            comment = value.get("comment_text", "").strip()

            if comment:
                # Log feedback with comment
                asyncio.create_task(_log_feedback(
                    str(uuid4()), message_id, conversation_id,
                    "negative_with_comment", user_id, comment,
                ))
                # Auto-update knowledge base from correction
                msg_data = _messages.get(message_id, {})
                if msg_data:
                    asyncio.create_task(_auto_update_knowledge_base(
                        msg_data["user_message"], msg_data["agent_response"], comment,
                    ))

            await turn_context.send_activity(
                "Thanks for your feedback! Your correction helps improve future responses."
            )

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    "Hello! I am the **UC Data Advisor**. Ask me about datasets, "
                    "metrics, or data governance in Unity Catalog."
                )


BOT = AdvisorBot()


async def on_error(context: TurnContext, error: Exception):
    print(f"[on_turn_error] {error}", file=sys.stderr)
    traceback.print_exc()
    await context.send_activity("Sorry, something went wrong.")

ADAPTER.on_turn_error = on_error


async def messages(req):
    if "application/json" not in req.headers.get("Content-Type", ""):
        return web.Response(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
    try:
        response = await ADAPTER.process(req, BOT)
        if response:
            return response
        return web.Response(status=HTTPStatus.OK)
    except Exception as e:
        logger.error(f"Error: {e}")
        traceback.print_exc()
        return web.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, text="Internal error")


APP = web.Application(middlewares=[aiohttp_error_middleware])
APP.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    web.run_app(APP, host="0.0.0.0", port=CONFIG.PORT)
'''
    with open(os.path.join(bot_dir, "app.py"), "w") as f:
        f.write(app_code)


def main():
    parser = argparse.ArgumentParser(description="Deploy UC Data Advisor Teams Bot")
    parser.add_argument("--config", required=True, help="Path to teams_config.yaml")
    parser.add_argument("--step", choices=["deploy", "teardown"], default="deploy")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f) or {}

    if args.step == "teardown":
        teardown(config)
    else:
        deploy(config)

    # Save updated config (with app_id, tenant_id, etc.)
    with open(args.config, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    main()
