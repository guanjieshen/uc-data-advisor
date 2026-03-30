# UC Data Advisor — Teams Integration

Microsoft Teams bot that forwards messages to the UC Data Advisor orchestrator serving endpoint. Users chat with the bot in Teams and get responses from the full agent pipeline (discovery, metrics, Q&A).

## Architecture

```
Teams User → Azure Bot Service → This Bot (aiohttp) → Databricks Orchestrator Endpoint
                                                              ↓
                                                    Discovery / Metrics / Q&A agents
                                                              ↓
                                                        Unity Catalog
```

## Prerequisites

- **UC Data Advisor deployed** with the orchestrator endpoint (run `--step all` from the main project first)
- **Azure subscription** for Azure Bot Service registration
- **Microsoft Entra ID** app registration (single-tenant)
- **Python 3.12+**

## Setup

### 1. Deploy the UC Data Advisor

Ensure the orchestrator endpoint is deployed and READY:

```bash
# From the project root
uv run python -m src.setup.run --config config/my_config.yaml --step all
```

Note the orchestrator endpoint name from the output (e.g., `my-app-orchestrator-agent`).

### 2. Azure App Registration

1. Go to **Azure Portal > Microsoft Entra ID > App registrations > New registration**
2. Name: `UC Data Advisor Bot`
3. Supported account types: **Single tenant**
4. Register, then copy:
   - **Application (client) ID**
   - **Directory (tenant) ID**
5. Go to **Certificates & secrets > New client secret** — copy the value

### 3. Azure Bot Resource

1. Go to **Azure Portal > Create a resource > Azure Bot**
2. Bot handle: `uc-data-advisor-bot`
3. Type of App: **Single Tenant**
4. App ID: paste the client ID from step 2
5. After creation, go to **Channels > Microsoft Teams** and enable it
6. Go to **Configuration** and set the messaging endpoint to:
   ```
   https://your-bot-host.com/api/messages
   ```

### 4. Configure & Run the Bot

```bash
cd teams
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Databricks and Azure credentials
```

```bash
python app.py
```

The bot starts on port 3978.

### 5. Expose the Bot (for development)

For local development, use a tunnel to expose port 3978:

```bash
# Using ngrok
ngrok http 3978

# Or using devtunnel
devtunnel host -p 3978
```

Update the Azure Bot messaging endpoint to the tunnel URL + `/api/messages`.

### 6. Install in Teams

1. Go to **Azure Portal > Azure Bot > Channels > Microsoft Teams > Open in Teams**
2. Or create a Teams app package and sideload it

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_HOST` | Yes | Workspace URL |
| `DATABRICKS_CLIENT_ID` | Yes* | Agent SP client ID |
| `DATABRICKS_CLIENT_SECRET` | Yes* | Agent SP client secret |
| `DATABRICKS_TOKEN` | Alt* | PAT (alternative to SP auth) |
| `ORCHESTRATOR_ENDPOINT` | Yes | Orchestrator serving endpoint name |
| `CONNECTIONS__SERVICE_CONNECTION__SETTINGS__CLIENTID` | Prod | Entra app client ID |
| `CONNECTIONS__SERVICE_CONNECTION__SETTINGS__CLIENTSECRET` | Prod | Entra app client secret |
| `CONNECTIONS__SERVICE_CONNECTION__SETTINGS__TENANTID` | Prod | Entra tenant ID |
| `PORT` | No | Bot server port (default: 3978) |

*Use either SP credentials (CLIENT_ID + CLIENT_SECRET) or TOKEN, not both.

## How It Works

1. User sends a message in Teams (DM or @mention in channel)
2. Azure Bot Service forwards the message to this bot's `/api/messages` endpoint
3. Bot strips @mention text and extracts the user's question
4. Bot calls the Databricks orchestrator serving endpoint via the SDK
5. Orchestrator classifies intent and routes to the appropriate agent (discovery/metrics/qa/general)
6. Response is sent back to Teams

## Limitations

- **Shared identity**: All Teams users share the agent SP's Databricks permissions (no per-user UC access control)
- **No conversation history**: Each message is independent (no multi-turn context). To add history, implement conversation state storage
- **Cold starts**: First request after endpoint scale-to-zero may take several minutes
- **Markdown rendering**: Teams supports basic markdown but not tables or code blocks in all contexts
