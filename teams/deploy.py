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


_AZ_EXE = "az.cmd" if sys.platform == "win32" else "az"


def _az(args: list[str], description: str = "", check: bool = True) -> subprocess.CompletedProcess:
    """Run an Azure CLI command."""
    cmd = [_AZ_EXE] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        prefix = f"FAILED: {description} — " if description else "FAILED: "
        print(f"\n  {prefix}{result.stderr.strip()}")
        _explain_policy_error(result.stderr)
    return result


def _explain_policy_error(stderr: str) -> None:
    """If stderr contains a RequestDisallowedByPolicy error, look up the policy
    definitions that blocked the request and print the required parameters."""
    if "RequestDisallowedByPolicy" not in stderr and "disallowed by policy" not in stderr.lower():
        return

    # Policy definition IDs look like:
    #   /providers/Microsoft.Authorization/policyDefinitions/<guid>
    #   /providers/Microsoft.Management/managementGroups/<mg>/providers/Microsoft.Authorization/policyDefinitions/<name>
    # Extract them from the error text.
    import re
    def_ids = list(dict.fromkeys(re.findall(
        r"(/providers/Microsoft\.Authorization/policyDefinitions/[A-Za-z0-9\-_]+|"
        r"/providers/Microsoft\.Management/managementGroups/[^\"',\s]+/providers/Microsoft\.Authorization/policyDefinitions/[A-Za-z0-9\-_]+)",
        stderr,
    )))
    if not def_ids:
        print("\n  (Could not parse policy definition IDs from error — see full stderr above.)")
        return

    print("\n  Policy definitions that blocked this request:")
    for def_id in def_ids:
        r = subprocess.run([_AZ_EXE, "policy", "definition", "show", "--name", def_id.rsplit("/", 1)[-1],
                            "-o", "json"], capture_output=True, text=True)
        if r.returncode != 0:
            # Fallback — try resolving via management group path
            r = subprocess.run([_AZ_EXE, "rest", "--method", "GET",
                                "--uri", f"https://management.azure.com{def_id}?api-version=2021-06-01"],
                                capture_output=True, text=True)
        if r.returncode != 0 or not r.stdout.strip():
            print(f"    - {def_id} (could not fetch definition)")
            continue
        try:
            pd = json.loads(r.stdout)
        except Exception:
            print(f"    - {def_id} (unparseable response)")
            continue
        name = pd.get("displayName") or pd.get("name") or def_id
        params = pd.get("parameters") or (pd.get("properties", {}) or {}).get("parameters") or {}
        print(f"    - {name}")
        if params:
            for pname, pmeta in params.items():
                meta = pmeta.get("metadata") or {}
                desc = meta.get("description") or pmeta.get("defaultValue") or ""
                print(f"        param `{pname}`: {desc}")


def deploy(config: dict) -> None:
    """Deploy the Teams bot end-to-end."""
    azure = config.get("azure", {})
    bot = config.get("bot", {})
    ad = config.get("azure_ad", {})
    db = config.get("databricks", {})

    sub_id = azure["subscription_id"]
    rg = azure["resource_group"]
    location = azure.get("location", "canadacentral")
    tags = azure.get("tags", {}) or {}

    bot_name = bot["name"]
    web_app_name = bot.get("web_app_name", bot_name)
    plan_name = bot.get("app_service_plan", f"{bot_name}-plan")
    sku = bot.get("sku", "B1")
    runtime = bot.get("runtime", "PYTHON:3.13")

    # Tag pairs passed to --tags on RG, Plan, and Web App creation.
    tag_pairs = [f"{k}={v}" for k, v in tags.items()]

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
        if tag_pairs:
            args += ["--tags", *tag_pairs]
        cr = _az(args, "create resource group")
        if cr.returncode != 0:
            print("\nABORTING: resource group could not be created. If your subscription requires")
            print("mandatory tags, add them under `azure.tags` in the config. Example:")
            print("  azure:\n    tags:\n      Project: my-project\n      CostCenter: 12345")
            sys.exit(1)
        print("created")
    else:
        print("exists")

    # Step 2: App Service Plan
    print("[2/8] App Service Plan...", end=" ", flush=True)
    r = _az(["appservice", "plan", "show", "--name", plan_name, "--resource-group", rg], check=False)
    if r.returncode != 0:
        args = ["appservice", "plan", "create", "--name", plan_name, "--resource-group", rg,
                "--location", location, "--sku", sku, "--is-linux"]
        if tag_pairs:
            args += ["--tags", *tag_pairs]
        cr = _az(args, "create app service plan")
        if cr.returncode != 0:
            sys.exit(1)
        print("created")
    else:
        print("exists")

    # Step 3: Web App
    print("[3/8] Web App...", end=" ", flush=True)
    r = _az(["webapp", "show", "--name", web_app_name, "--resource-group", rg], check=False)
    if r.returncode != 0:
        args = ["webapp", "create", "--name", web_app_name, "--resource-group", rg,
                "--plan", plan_name, "--runtime", runtime]
        if tag_pairs:
            args += ["--tags", *tag_pairs]
        cr = _az(args, "create web app")
        if cr.returncode != 0:
            sys.exit(1)
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
            "tags": tags,
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
    """Write the simplified bot app.py (no OAuth, uses SP)."""
    app_code = '''"""UC Data Advisor Teams Bot — uses SP credentials directly."""

import os
import sys
import logging
import traceback
from http import HTTPStatus

from aiohttp import web
from botbuilder.core import TurnContext, ActivityHandler
from botbuilder.integration.aiohttp import CloudAdapter, ConfigurationBotFrameworkAuthentication
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity, ActivityTypes

from databricks.sdk import WorkspaceClient
from config import DefaultConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = DefaultConfig()
ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

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


def query_orchestrator(message: str) -> str:
    w = get_db_client()
    endpoint = os.environ.get("SERVING_ENDPOINT_NAME", "")
    if not endpoint:
        return "SERVING_ENDPOINT_NAME not configured."
    try:
        resp = w.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint}/invocations",
            body={"input": [{"role": "user", "content": message}]},
        )
    except Exception as e:
        logger.error(f"Orchestrator call failed: {e}")
        return f"Sorry, I encountered an error: {str(e)[:200]}"

    for item in resp.get("output", []):
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    return c.get("text", "")
    return "I received your message but couldn\\'t generate a response."


class AdvisorBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        text = (turn_context.activity.text or "").strip()
        if not text:
            return
        await turn_context.send_activity(Activity(type=ActivityTypes.typing))
        logger.info(f"User: {text[:100]}")
        response = query_orchestrator(text)
        logger.info(f"Response: {response[:100]}")
        await turn_context.send_activity(response)

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    "Hello! I\\'m the **UC Data Advisor**. Ask me about datasets, "
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
