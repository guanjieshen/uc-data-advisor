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
_AZ_SUBSCRIPTION: str = ""  # Set at start of deploy()/teardown() so every az call targets the configured subscription.

# Command groups that operate at tenant scope (not subscription) and reject --subscription.
_AZ_NO_SUBSCRIPTION_PREFIXES: tuple[str, ...] = ("ad", "account")


def _az(args: list[str], description: str = "", check: bool = True) -> subprocess.CompletedProcess:
    """Run an Azure CLI command. If _AZ_SUBSCRIPTION is set, append --subscription
    so every call targets the subscription declared in the config rather than the
    active CLI context. Skipped for tenant-scoped groups (`ad`, `account`) which
    reject that flag."""
    cmd = [_AZ_EXE] + args
    first = args[0] if args else ""
    supports_sub = first not in _AZ_NO_SUBSCRIPTION_PREFIXES
    if _AZ_SUBSCRIPTION and supports_sub and "--subscription" not in args:
        cmd += ["--subscription", _AZ_SUBSCRIPTION]
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


def _configure_network(config: dict, s, web_app_name: str, bot_rg: str, sub_id: str) -> None:
    """Apply VNet integration, private DNS, and ingress restrictions to the Web App.

    Assumes the Databricks workspace is already behind Private Link with a PE + private DNS zone.
    This function wires the Web App's outbound egress through a VNet that can reach the PE, and
    optionally links the existing private DNS zone to that VNet for FQDN resolution.
    """
    net = config.get("network", {}) or {}
    vnet_cfg = net.get("vnet", {}) or {}
    subnet_cfg = net.get("subnet", {}) or {}
    dns_cfg = net.get("private_dns_zone", {}) or {}

    vnet_name = vnet_cfg.get("name", "")
    vnet_rg = vnet_cfg.get("resource_group") or bot_rg
    subnet_name = subnet_cfg.get("name", "bot-integration-subnet")
    subnet_prefix = subnet_cfg.get("address_prefix", "")

    print(f"{s(4)} Network...", flush=True)

    if not vnet_name:
        print("  SKIPPED: network.vnet.name is required when network.enabled=true")
        return

    # Ensure the integration subnet exists and is delegated to Microsoft.Web/serverFarms.
    r = _az(["network", "vnet", "subnet", "show",
             "--resource-group", vnet_rg, "--vnet-name", vnet_name,
             "--name", subnet_name, "-o", "json"], check=False)
    if r.returncode != 0:
        if not subnet_prefix:
            print(f"  FAILED: subnet '{subnet_name}' not found in {vnet_rg}/{vnet_name} and "
                  f"no network.subnet.address_prefix provided to create it.")
            return
        print(f"  Creating subnet {subnet_name} ({subnet_prefix})...", flush=True)
        _az(["network", "vnet", "subnet", "create",
             "--resource-group", vnet_rg, "--vnet-name", vnet_name,
             "--name", subnet_name, "--address-prefix", subnet_prefix,
             "--delegations", "Microsoft.Web/serverFarms"], "create integration subnet")
    else:
        try:
            sdata = json.loads(r.stdout)
            delegations = sdata.get("delegations") or []
            has_delegation = any(
                (d.get("serviceName") or (d.get("properties") or {}).get("serviceName"))
                == "Microsoft.Web/serverFarms" for d in delegations
            )
        except Exception:
            has_delegation = False
        if not has_delegation:
            print(f"  Delegating {subnet_name} to Microsoft.Web/serverFarms...", flush=True)
            _az(["network", "vnet", "subnet", "update",
                 "--resource-group", vnet_rg, "--vnet-name", vnet_name,
                 "--name", subnet_name, "--delegations", "Microsoft.Web/serverFarms"],
                "delegate subnet")

    # VNet-integrate the Web App.
    subnet_id = (f"/subscriptions/{sub_id}/resourceGroups/{vnet_rg}"
                 f"/providers/Microsoft.Network/virtualNetworks/{vnet_name}/subnets/{subnet_name}")
    print(f"  Integrating Web App into {vnet_rg}/{vnet_name}/{subnet_name}...", flush=True)
    _az(["webapp", "vnet-integration", "add",
         "--resource-group", bot_rg, "--name", web_app_name,
         "--vnet", vnet_name, "--subnet", subnet_id], "add vnet integration", check=False)

    # Link the existing private DNS zone to the VNet so the workspace FQDN resolves privately.
    pdns_name = dns_cfg.get("name", "privatelink.azuredatabricks.net")
    pdns_rg = dns_cfg.get("resource_group", "")
    if pdns_rg and dns_cfg.get("link_to_vnet", True):
        link_name = f"{web_app_name}-link"
        r = _az(["network", "private-dns", "link", "vnet", "show",
                 "--resource-group", pdns_rg, "--zone-name", pdns_name,
                 "--name", link_name], check=False)
        if r.returncode != 0:
            vnet_id = (f"/subscriptions/{sub_id}/resourceGroups/{vnet_rg}"
                       f"/providers/Microsoft.Network/virtualNetworks/{vnet_name}")
            print(f"  Linking private DNS zone {pdns_name} to {vnet_name}...", flush=True)
            _az(["network", "private-dns", "link", "vnet", "create",
                 "--resource-group", pdns_rg, "--zone-name", pdns_name,
                 "--name", link_name, "--virtual-network", vnet_id,
                 "--registration-enabled", "false"], "link private DNS zone")
        else:
            print(f"  DNS zone {pdns_name} already linked.", flush=True)

    # Restrict inbound to Bot Service service tag.
    if net.get("restrict_ingress_to_bot_service", True):
        print(f"  Restricting inbound to AzureBotService service tag...", flush=True)
        _az(["webapp", "config", "access-restriction", "add",
             "--resource-group", bot_rg, "--name", web_app_name,
             "--rule-name", "AllowBotService", "--action", "Allow", "--priority", "100",
             "--service-tag", "AzureBotService"], "add access restriction", check=False)


def deploy(config: dict) -> None:
    """Deploy the Teams bot end-to-end."""
    global _AZ_SUBSCRIPTION

    azure = config.get("azure", {})
    bot = config.get("bot", {})
    ad = config.get("azure_ad", {})
    db = config.get("databricks", {})
    net = config.get("network", {}) or {}
    network_enabled = bool(net.get("enabled"))

    sub_id = azure["subscription_id"]
    _AZ_SUBSCRIPTION = sub_id
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

    total = 9 if network_enabled else 8
    s = lambda i: f"[{i}/{total}]"

    print("=" * 60)
    print("UC Data Advisor — Teams Bot Deployment")
    print("=" * 60)
    print(f"  Subscription:   {sub_id}")
    print(f"  Resource group: {rg} ({location})")
    print(f"  Bot name:       {bot_name}")
    print(f"  Web app:        {web_app_name}.azurewebsites.net")
    if network_enabled:
        print(f"  Network:        private (VNet integration + private DNS)")
        if sku.upper() in ("B1", "B2", "B3", "F1"):
            print(f"  WARNING: sku={sku} does not support VNet integration. Use S1 or higher.")
    print()

    # Step 1: Resource group
    print(f"{s(1)} Resource group...", end=" ", flush=True)
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
    print(f"{s(2)} App Service Plan...", end=" ", flush=True)
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
    print(f"{s(3)} Web App...", end=" ", flush=True)
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

    # Step 4 (conditional): Network — VNet integration, private DNS, ingress restriction.
    if network_enabled:
        _configure_network(config, s, web_app_name, rg, sub_id)

    # Next step: App Registration
    step_idx = 5 if network_enabled else 4
    print(f"{s(step_idx)} App Registration...", end=" ", flush=True)
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

    # Azure Bot
    step_idx += 1
    print(f"{s(step_idx)} Azure Bot...", end=" ", flush=True)
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

    # Teams Channel
    step_idx += 1
    print(f"{s(step_idx)} Teams Channel...", end=" ", flush=True)
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

    # Web App Environment Variables
    step_idx += 1
    print(f"{s(step_idx)} Environment variables...", end=" ", flush=True)
    env_settings = [
        f"DATABRICKS_HOST={db['host']}",
        f"DATABRICKS_SP_CLIENT_ID={db.get('sp_client_id', '')}",
        f"DATABRICKS_SP_CLIENT_SECRET={db.get('sp_client_secret', '')}",
        f"SERVING_ENDPOINT_NAME={db['orchestrator_endpoint']}",
        f"MicrosoftAppId={app_id}",
        f"MicrosoftAppPassword={client_secret}",
        "MicrosoftAppType=singletenant",
        f"MicrosoftTenantId={tenant_id}",
        "SCM_DO_BUILD_DURING_DEPLOYMENT=True",
    ]
    if network_enabled and net.get("route_all_traffic", True):
        # Force all Web App egress through the integrated VNet so private DNS applies.
        env_settings += [
            "WEBSITE_VNET_ROUTE_ALL=1",
            f"WEBSITE_DNS_SERVER={net.get('dns_server', '168.63.129.16')}",
        ]
    _az(["webapp", "config", "appsettings", "set",
         "--name", web_app_name, "--resource-group", rg,
         "--settings", *env_settings], check=False)
    _az(["webapp", "config", "set", "--name", web_app_name, "--resource-group", rg,
         "--startup-file", "python3 app.py"], check=False)
    print("set")

    # Deploy bot code
    step_idx += 1
    print(f"{s(step_idx)} Deploying bot code...", end=" ", flush=True)

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
    global _AZ_SUBSCRIPTION

    azure = config.get("azure", {})
    bot = config.get("bot", {})
    ad = config.get("azure_ad", {})
    net = config.get("network", {}) or {}
    network_enabled = bool(net.get("enabled"))

    _AZ_SUBSCRIPTION = azure["subscription_id"]
    rg = azure["resource_group"]
    bot_name = bot["name"]
    web_app_name = bot.get("web_app_name", bot_name)
    plan_name = bot.get("app_service_plan", f"{bot_name}-plan")
    app_id = ad.get("app_id", "")

    total = 5 if network_enabled else 4
    s = lambda i: f"[{i}/{total}]"

    print("=" * 60)
    print("Teams Bot Teardown")
    print("=" * 60)

    # Private DNS zone link is removed first (if we created one).
    if network_enabled:
        dns_cfg = net.get("private_dns_zone", {}) or {}
        pdns_rg = dns_cfg.get("resource_group", "")
        pdns_name = dns_cfg.get("name", "privatelink.azuredatabricks.net")
        if pdns_rg and dns_cfg.get("link_to_vnet", True):
            print(f"{s(1)} Private DNS zone link...", end=" ", flush=True)
            _az(["network", "private-dns", "link", "vnet", "delete",
                 "--resource-group", pdns_rg, "--zone-name", pdns_name,
                 "--name", f"{web_app_name}-link", "--yes"], check=False)
            print("deleted")
        else:
            print(f"{s(1)} Private DNS zone link... skipped (no zone RG configured)")

    offset = 1 if network_enabled else 0
    print(f"{s(1 + offset)} Azure Bot...", end=" ", flush=True)
    _az(["resource", "delete", "--resource-group", rg,
         "--resource-type", "Microsoft.BotService/botServices",
         "--name", bot_name], check=False)
    print("deleted")

    print(f"{s(2 + offset)} Web App...", end=" ", flush=True)
    _az(["webapp", "delete", "--name", web_app_name, "--resource-group", rg], check=False)
    print("deleted")

    print(f"{s(3 + offset)} App Service Plan...", end=" ", flush=True)
    _az(["appservice", "plan", "delete", "--name", plan_name,
         "--resource-group", rg, "--yes"], check=False)
    print("deleted")

    if app_id:
        print(f"{s(4 + offset)} App Registration...", end=" ", flush=True)
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
