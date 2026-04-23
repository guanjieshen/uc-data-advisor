# UC Data Advisor — Teams Bot

Microsoft Teams bot that forwards messages to the UC Data Advisor orchestrator serving endpoint. Users chat with the bot in Teams and get responses from the full agent pipeline (discovery, metrics, Q&A) — no Databricks account required.

## Architecture

```
Teams User → Azure Bot Service → Azure Web App (Python bot)
                                        ↓
                              Databricks SP credentials
                                        ↓
                              Orchestrator Serving Endpoint
                                        ↓
                              Discovery / Metrics / Q&A agents
                                        ↓
                                  Unity Catalog
```

All users share the configured service principal's permissions. No per-user OAuth — the bot authenticates to Databricks using SP credentials.

## Prerequisites

- **UC Data Advisor deployed** with the orchestrator endpoint running
- **Azure CLI** installed and authenticated (`az login`)
- **Azure subscription** with permissions to create resources
- **Python 3.12+** with `pyyaml` installed

## Quick Start

```bash
# 1. Create config from template
cp teams/teams_config.example.yaml teams/teams_config.yaml
```

Edit `teams/teams_config.yaml`:

```yaml
azure:
  subscription_id: "your-subscription-id"
  resource_group: "your-rg"
  location: "canadacentral"
  tags:
    owner: "you@company.com"
    # Add any tags your subscription's Azure Policy requires, e.g.:
    # Project: "my-project"
    # CostCenter: "12345"

bot:
  name: "my-advisor-bot"        # Must be globally unique

databricks:
  host: "https://your-workspace.cloud.databricks.com"
  orchestrator_endpoint: "your-app-orchestrator-agent"
  sp_client_id: "your-sp-client-id"
  sp_client_secret: "your-sp-secret"
```

```bash
# 2. Deploy everything
python teams/deploy.py --config teams/teams_config.yaml

# 3. Test
# Azure Portal → Azure Bot → Test in Web Chat
# Or: Azure Portal → Azure Bot → Channels → Teams → Open in Teams
```

The deploy script creates all Azure resources automatically:
- Resource group (if not exists)
- App Service Plan (Linux, B1)
- Web App (Python 3.13)
- App Registration + Service Principal
- Azure Bot (F0 free tier) with Teams channel
- Environment variables
- Bot code deployment

## Config Reference

```yaml
azure:
  subscription_id: ""        # Azure subscription ID
  resource_group: ""         # Resource group name
  location: "canadacentral"  # Azure region
  tags:                      # Tags for RG/plan/web app/bot; add any your Azure Policy requires
    owner: "you@company.com"

bot:
  name: ""                   # Azure Bot name (globally unique)
  web_app_name: ""           # Web App name (defaults to bot name)
  app_service_plan: ""       # Plan name (defaults to {bot_name}-plan)
  sku: "B1"                  # App Service tier (B1 = ~$13/mo)
  runtime: "PYTHON:3.13"     # Python runtime

azure_ad:
  app_id: ""                 # Auto-populated on first deploy
  tenant_id: ""              # Auto-populated on first deploy
  client_secret: ""          # Auto-populated on first deploy

databricks:
  host: ""                   # Workspace URL
  orchestrator_endpoint: ""  # Orchestrator serving endpoint name
  sp_client_id: ""           # SP application ID for Databricks auth
  sp_client_secret: ""       # SP OAuth secret for Databricks auth
```

## Teardown

```bash
python teams/deploy.py --config teams/teams_config.yaml --step teardown
```

Deletes: Azure Bot, Web App, App Service Plan, and App Registration.

## How It Works

1. User sends a message in Teams (DM or @mention in channel)
2. Azure Bot Service forwards the message to the Web App's `/api/messages` endpoint
3. Bot extracts the user's text
4. Bot calls the Databricks orchestrator serving endpoint via the SDK using SP credentials
5. Orchestrator classifies intent and routes to the appropriate agent
6. Response is sent back to Teams

## Troubleshooting

**Bot unresponsive in Web Chat**: Check Web App logs in Azure Portal → Web App → Log stream. Common issues:
- Env var casing (`MicrosoftAppId` not `MicrosoftAppID`)
- Missing service principal for app registration (`az ad sp create --id <app_id>`)
- Build timeout — the first deployment can take 10-15 minutes

**Empty sign-in page**: This bot doesn't use OAuth sign-in. If you see a sign-in prompt, the old OBO bot code is still deployed. Redeploy.

**Cold start delays**: Agent endpoints scale to zero. First request takes up to 5 minutes. Set `scale_to_zero: false` in the advisor config to keep endpoints warm.

**"Container did not start" errors**: The App Service may need more time for the initial build. Check the deployment status in Azure Portal → Web App → Deployment Center.
