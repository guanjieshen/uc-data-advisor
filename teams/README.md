# UC Data Advisor — Teams Bot

Microsoft Teams bot that forwards user messages to the UC Data Advisor orchestrator serving endpoint. Users chat with the bot in Teams and get responses from the full agent pipeline (discovery, metrics, Q&A) — no Databricks account required for end users.

## Architecture

```
Teams User → Microsoft Bot Connector → Azure Web App (/api/messages, Python aiohttp bot)
                                              │
                                              ▼  (SP OAuth)
                                       Databricks Workspace
                                       Orchestrator Serving Endpoint
                                              │
                                              ▼
                              Discovery / Metrics / Q&A agents → Unity Catalog
```

The bot authenticates to Databricks using a single workspace service principal — all Teams users share that SP's permissions. There is no per-user OAuth.

## Deployment Patterns

There are exactly two supported patterns. Pick the one that matches your workspace:

- **[Pattern A — Public workspace](#pattern-a--public-workspace)** — workspace is reachable from the public internet (with or without IP allowlist). Bot uses public egress. Simplest setup.
- **[Pattern B — Private workspace, isolated bot RG](#pattern-b--private-workspace-isolated-bot-rg)** — workspace is behind Private Link / NCC / no public access. Bot lives in its own VNet with its own Private Endpoint to the workspace. **Customer creates only new resources** — no modifications to the existing workspace, workspace VNet, NCC, or DNS zones.

Both patterns share the same `teams/deploy.py` script. The difference is just whether `network.enabled` is `true` and what you provision before running it.

### Config files

The config lives in two files side by side:

- **`<name>_config.yaml`** — user-authored. You fill it in. Never written by the deploy script.
- **`<name>_config.generated.yaml`** — auto-populated by the deploy script with the Entra App Registration's `app_id`, `tenant_id`, and `client_secret`. Safe to delete to force the script to recreate the App Registration on the next run. Do not hand-edit.

`teams/deploy.py` loads both files and merges them transparently. You only ever edit the input file.

## Resources Created (Both Patterns)

`teams/deploy.py` always provisions these:

| # | Resource | Type | Why |
|---|---|---|---|
| 1 | **Resource group** | `Microsoft.Resources/resourceGroups` | Container for the bot. Created if missing; reused if exists. |
| 2 | **App Service Plan** | `Microsoft.Web/serverFarms` | Compute for the Web App. `B1` (~$13/mo) for Pattern A, `S1` (~$73/mo) for Pattern B (B-series doesn't support VNet integration). |
| 3 | **Web App** | `Microsoft.Web/sites` | Hosts the Python bot. Bot Connector POSTs activities to `<name>.azurewebsites.net/api/messages`. |
| 4 | **App Registration** | Entra ID app | Bot's identity for Bot Framework auth. Provides `MicrosoftAppId/Password/TenantId`. |
| 5 | **Tenant Service Principal** | Entra ID SP for the App Reg | Required by Bot Framework auth. |
| 6 | **Azure Bot** | `Microsoft.BotService/botServices` (F0 SKU) | Bot Framework registration; routes channel traffic to the Web App. |
| 7 | **Teams channel** | `botServices/channels/MsTeamsChannel` | Enables the Teams channel on the Bot Service. |
| 8 | **App settings** | env vars on Web App | `DATABRICKS_HOST/SP_CLIENT_ID/SP_CLIENT_SECRET`, `SERVING_ENDPOINT_NAME`, `MicrosoftApp{Id,Password,TenantId,Type}`, `SCM_DO_BUILD_DURING_DEPLOYMENT`. |
| 9 | **Bot code** | OneDeploy zip push | Bundles `app.py` + `requirements.txt` (with `databricks-sdk` injected) and uploads via `az webapp deploy --type zip` (uses Azure AD auth — works even when SCM basic auth is disabled). |

**Pattern B additionally** wires up VNet integration on the Web App, links the bot's private DNS zone, sets `WEBSITE_VNET_ROUTE_ALL=1`, and restricts inbound to the `AzureBotService` service tag. See [Pattern B](#pattern-b--private-workspace-isolated-bot-rg) for the per-resource list.

## Prerequisites

- **UC Data Advisor pipeline already deployed** in the target workspace (creates the orchestrator serving endpoint and the SP OAuth secret).
- **Azure CLI** installed and authenticated (`az login`).
- **Python 3.12+** with `pyyaml`.
- **Globally-unique bot name** (becomes part of `<name>.azurewebsites.net`).
- The bot's runtime SP — typically the same agent SP the advisor pipeline provisioned. Retrieve its OAuth secret from the workspace secret scope:
  ```fish
  databricks secrets get-secret <advisor-app-name> sp-client-secret --profile <ws> \
      --output json | jq -r .value | base64 -d
  ```

---

## Pattern A — Public workspace

For workspaces reachable from the public internet (with or without IP allowlist).

### Network requirements

None. Bot's outbound HTTPS to `<workspace>.azuredatabricks.net` goes over public network. If the workspace has an IP allowlist, add the Web App's outbound IP ranges to it (`az webapp show -g <bot-rg> -n <bot-name> --query possibleOutboundIpAddresses` — the list can be large; consider attaching a NAT Gateway for a stable single IP).

### Config

```yaml
azure:
  subscription_id: "..."
  resource_group: "<bot-rg>"     # new or existing
  location: "..."
  tags:
    owner: "you@company.com"

bot:
  name: "..."                    # globally unique
  sku: "B1"                      # B1 fine for Pattern A
  runtime: "PYTHON:3.13"

azure_ad:
  app_id: ""                     # auto-populated on first deploy
  tenant_id: ""
  client_secret: ""

databricks:
  host: "https://<workspace>.azuredatabricks.net"
  orchestrator_endpoint: "<advisor>-orchestrator-agent"
  sp_client_id: "<agent SP app id>"
  sp_client_secret: "<from advisor pipeline secret scope>"

# omit `network:` entirely (or set network.enabled: false)
```

### Deploy

```fish
cp teams/teams_config.example.yaml teams/teams_config.yaml
# edit teams/teams_config.yaml
python teams/deploy.py --config teams/teams_config.yaml
```

Then test in **Azure Portal → Azure Bot → Test in Web Chat**, or **Open in Teams**.

---

## Pattern B — Private workspace, isolated bot RG

For workspaces behind **Private Link**, with **NCC** for serverless egress, optionally with **public network access disabled**. The bot lives in a brand-new RG and VNet you create. **Nothing about the existing workspace, workspace VNet, NCC, or DNS zones is modified**, with one exception: a single PE-connection approval on the workspace resource (smallest possible touch).

### What the bot's runtime path looks like

```
Web App  ──VNet integration──►  bot-integration subnet
                                       │
                                       ▼  (intra-VNet routing)
                                private-endpoints subnet
                                       │
                                       ▼
                              ws-uiapi-pe ──► Customer's Workspace REST API
                                                    │
                                                    ▼
                                          orchestrator + sub-agents
```

DNS resolution: bot-vnet has a **private DNS zone** named `privatelink.azuredatabricks.net` linked only to bot-vnet. The workspace FQDN resolves to the bot-side PE's private IP from inside the Web App. No traffic leaves the customer's VNets.

### What the customer must create (all new, all in their own bot RG)

| # | Resource | Purpose |
|---|---|---|
| B1 | **bot-rg** | Container. |
| B2 | **bot-vnet** with non-overlapping address space | Hosts the bot. Address space must not overlap with the workspace VNet (no peering exists). |
| B3 | **`bot-integration` subnet** (`/27`+, delegated to `Microsoft.Web/serverFarms`) | Web App Regional VNet Integration target. |
| B4 | **`private-endpoints` subnet** (`/26` is plenty, network policies disabled) | Holds the PE for the workspace. |
| B5 | **PE for the workspace** (`databricks_ui_api` group) in `private-endpoints` subnet | Private path from bot-vnet to the workspace REST API. |
| B6 | **Private DNS zone** `privatelink.azuredatabricks.net` (NEW, in bot-rg), linked to bot-vnet | Resolves the workspace FQDN to the bot-side PE's private IP. |
| B7 | All **Pattern A bot resources** (#1–9 in the table above) | Created automatically by `teams/deploy.py` with `network.enabled: true`. |

### What is NOT created or modified

- ❌ The customer's existing workspace VNet — no new subnet, no peering, no UDR, no NSG changes.
- ❌ The customer's existing workspace PEs — left alone.
- ❌ The customer's existing private DNS zone (the workspace's copy of `privatelink.azuredatabricks.net`) — left alone. The bot uses its own zone in its own RG. (Azure private DNS zones are RG-scoped and per-VNet-linked; two same-named zones coexist fine in different RGs as long as a given VNet only links one.)
- ❌ NCC — only governs serverless *egress from* the workspace; irrelevant to the bot's path *into* the workspace.
- ❌ Managed storage / catalog — bot doesn't touch it.
- ❌ Workspace IP allowlist — doesn't apply to PE traffic.

### The one unavoidable workspace-side touch

Creating a PE that targets the workspace creates a `privateEndpointConnection` sub-resource on the workspace, status `Pending` until approved. Two ways to handle:

- **Customer is workspace owner / has Contributor on the workspace** → approves during creation (or `az network private-endpoint-connection approve` if the auto-approval doesn't trigger).
- **Customer is data-plane only** → asks workspace owner to approve once. One command on the owner's side; no other access required.

### Pre-deploy commands (customer runs once)

```fish
set BOT_RG bot-rg
set BOT_VNET bot-vnet
set LOC <region matching workspace>
set SUB <subscription>
set TAGS owner=... <other-mandatory-tags>
set WS_ID "/subscriptions/.../resourceGroups/<ws-rg>/providers/Microsoft.Databricks/workspaces/<ws-name>"

# 1. RG + VNet + subnets (NEW resources, no overlap with workspace)
az group create -n $BOT_RG -l $LOC --tags $TAGS
az network vnet create -g $BOT_RG -n $BOT_VNET --address-prefixes 10.100.0.0/16 -l $LOC --tags $TAGS
az network vnet subnet create -g $BOT_RG --vnet-name $BOT_VNET -n bot-integration  \
    --address-prefixes 10.100.1.0/27 --delegations Microsoft.Web/serverFarms
az network vnet subnet create -g $BOT_RG --vnet-name $BOT_VNET -n private-endpoints \
    --address-prefixes 10.100.2.0/26 --private-endpoint-network-policies Disabled

# 2. PE pointing at the workspace (the one workspace touch — needs approval)
az network private-endpoint create -g $BOT_RG -n ws-uiapi-pe \
    --vnet-name $BOT_VNET --subnet private-endpoints \
    --private-connection-resource-id $WS_ID --connection-name uiapi \
    --group-id databricks_ui_api --tags $TAGS

# If approval doesn't auto-fire (customer not workspace owner), have the
# workspace owner run:
#   az network private-endpoint-connection approve --id <pe-conn-id> --description "approved for bot"

# 3. Private DNS zone (NEW, customer-owned) + VNet link
az network private-dns zone create -g $BOT_RG -n privatelink.azuredatabricks.net --tags $TAGS
az network private-dns link vnet create -g $BOT_RG --zone-name privatelink.azuredatabricks.net \
    --name $BOT_VNET-link \
    --virtual-network "/subscriptions/$SUB/resourceGroups/$BOT_RG/providers/Microsoft.Network/virtualNetworks/$BOT_VNET" \
    --registration-enabled false

# 4. Bind the PE to the DNS zone so resolution works
az network private-endpoint dns-zone-group create -g $BOT_RG \
    --endpoint-name ws-uiapi-pe --name uiapi-zg \
    --private-dns-zone privatelink.azuredatabricks.net \
    --zone-name privatelink.azuredatabricks.net
```

### Config

```yaml
azure:
  subscription_id: "..."
  resource_group: "bot-rg"       # the new RG you just created
  location: "..."
  tags:
    owner: "you@company.com"

bot:
  name: "..."
  sku: "S1"                      # MUST be S1+ for VNet integration
  runtime: "PYTHON:3.13"

azure_ad:
  app_id: ""
  tenant_id: ""
  client_secret: ""

databricks:
  host: "https://<workspace>.azuredatabricks.net"
  orchestrator_endpoint: "<advisor>-orchestrator-agent"
  sp_client_id: "<agent SP app id>"
  sp_client_secret: "<from advisor pipeline secret scope>"

network:
  enabled: true
  vnet:
    name: "bot-vnet"
    resource_group: "bot-rg"
  subnet:
    name: "bot-integration"
    # address_prefix: not needed since the subnet already exists from pre-deploy commands
  private_dns_zone:
    name: "privatelink.azuredatabricks.net"
    resource_group: "bot-rg"     # the customer's NEW zone
    link_to_vnet: true           # idempotent — already linked above
  route_all_traffic: true        # WEBSITE_VNET_ROUTE_ALL=1 on Web App
  dns_server: "168.63.129.16"    # Azure resolver; override if VNet uses custom DNS
  restrict_ingress_to_bot_service: true
```

### Deploy

```fish
python teams/deploy.py --config teams/teams_config.yaml
```

Test in **Azure Portal → Azure Bot → Test in Web Chat**, or **Open in Teams**.

### Permissions the customer needs

- **Subscription**: `Contributor` on the bot RG.
- **Workspace resource**: ability to approve a PE connection (or coordinate with workspace owner — one-time).
- **Entra ID**: `Application Developer` directory role (or `Application.ReadWrite.OwnedBy`) to create the App Registration.
- **Workspace secret scope**: ability to read the agent SP's `sp-client-secret` (or have it from when the advisor pipeline was originally run).

---

## Teardown

```fish
python teams/deploy.py --config teams/teams_config.yaml --step teardown
```

Deletes Azure Bot, Web App, App Service Plan, App Registration, and (Pattern B) the private DNS zone link the script created. The customer's pre-created resources (bot-rg, bot-vnet, subnets, PE, DNS zone) are NOT deleted by the teardown — remove them manually if needed:

```fish
# Pattern B teardown of pre-deploy resources
az group delete -n bot-rg --yes  # cascades VNet, subnets, PE, DNS zone, link
# The PE-connection on the workspace becomes "Disconnected" — workspace owner can clean up:
az network private-endpoint-connection list --id $WS_ID -o table
az network private-endpoint-connection delete --id <connection-id> --yes
```

## Troubleshooting

**Bot unresponsive in Web Chat / Teams**: Tail the Web App logs (`az webapp log tail -g <bot-rg> -n <bot>`). If logs return 403, your IP isn't allowed on the SCM site — see *SCM lockdown* below.

**Env var casing**: must be `MicrosoftAppId` (camelCase), not `MicrosoftAppID`.

**Cold-start delays (~5 min on first request)**: Agent serving endpoints scale to zero by default. The orchestrator → sub-agent → SQL warehouse chain can take 3–5 min cold. Set `scale_to_zero: false` in the advisor config (the workspace pipeline) to keep agents warm.

**`Cannot resolve hostname: <workspace>.azuredatabricks.net`** (Pattern B): The Web App's VNet doesn't have the private DNS zone linked. Verify with `az network private-dns link vnet list -g <bot-rg> --zone-name privatelink.azuredatabricks.net` — the bot-vnet should be listed.

**`Container creation failed`** during agent endpoint deploy (advisor pipeline, not bot): NCC PE rule for the workspace's managed storage isn't `ESTABLISHED`. This is a workspace-side fix, not a bot issue.

**SCM 401/403 during deploy**: Either SCM basic auth is disabled (OneDeploy now handles this — verify your script is current) or the SCM site has IP restrictions blocking your IP. Add your IP via `az webapp config access-restriction add --scm-site true --rule-name AllowAdmin --action Allow --priority 200 --ip-address <your-ip>/32`.

**Mandatory-tag policy** blocking RG/Web App creation: add the required tags under `azure.tags` in the config. The deploy script's `_explain_policy_error` helper prints the blocking policy's required parameters when this fails.

**`publicNetworkAccess: Disabled` on the Bot resource greys out Test in Web Chat**: Expected — the Portal proxy uses public Direct Line. Either re-enable PNA on the Bot resource for ad-hoc testing, or test via Teams (Teams traffic uses Microsoft's internal mesh and works regardless).
