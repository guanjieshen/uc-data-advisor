# Databricks notebook source
# MAGIC %md
# MAGIC # UC Data Advisor — Benchmark Suite
# MAGIC
# MAGIC Runs benchmarks directly against agent serving endpoints via the Databricks SDK.
# MAGIC No app URL or external auth needed — uses the notebook's built-in credentials.
# MAGIC
# MAGIC **Setup:** Set the `config_path` widget to your deployed config file, then Run All.

# COMMAND ----------

dbutils.widgets.text("config_path", "/Workspace/Users/allan.cao@databricks.com/uc-data-advisor/config/advisor_config.yaml", "Config Path")
CONFIG_PATH = dbutils.widgets.get("config_path").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Config

# COMMAND ----------

import json, time, yaml
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f) or {}

infra = config.get("infrastructure", {})
generated = config.get("generated", {})
app_name = infra.get("app_name", "")
agent_endpoints = infra.get("agent_endpoints", {})
serving_endpoint = infra.get("serving_endpoint", "databricks-claude-opus-4-6")

assert agent_endpoints, "No agent_endpoints in config — run the setup pipeline first"

print(f"App name: {app_name}")
print(f"LLM endpoint: {serving_endpoint}")
print(f"Agent endpoints: {json.dumps(agent_endpoints, indent=2)}")

BENCHMARKS = generated.get("benchmarks", [
    {"question": "What catalogs are available in the workspace?", "expected_agent": "discovery", "expect_contains": ["catalog"], "category": "discovery"},
    {"question": "Show me the columns in the media_gold_reviews_chunked table", "expected_agent": "discovery", "expect_contains": [], "category": "discovery"},
    {"question": "What is the average franchiseID across all records?", "expected_agent": "metrics", "expect_contains": [], "category": "metrics"},
    {"question": "How do I request access to a dataset?", "expected_agent": "qa", "expect_contains": [], "category": "qa"},
    {"question": "What is Unity Catalog?", "expected_agent": "qa", "expect_contains": [], "category": "qa"},
    {"question": "Hello, what can you help me with?", "expected_agent": "general", "expect_contains": [], "category": "general"},
])

print(f"Benchmarks: {len(BENCHMARKS)} questions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: Classify & Call Agents

# COMMAND ----------

CLASSIFY_PROMPT = generated.get("prompts", {}).get("classify", """You are an intent classifier for the UC Data Advisor.
Classify the user's latest message into exactly one category:
- discovery: Questions about finding datasets, browsing catalogs/schemas/tables, understanding table structures, or checking what data exists.
- metrics: Questions asking for specific numbers, aggregations, counts, trends, or analytical queries about the data.
- qa: Questions about data governance, access policies, how to request data, FAQs about the data catalog, or general knowledge questions.
- general: Greetings, small talk, clarifications, or anything that doesn't fit the above categories.
Respond with ONLY the category name, nothing else.""")

GENERAL_PROMPT = generated.get("prompts", {}).get("general",
    "You are the UC Data Advisor, a helpful assistant for discovering and understanding datasets in Unity Catalog. Respond warmly and briefly.")


def classify_intent(question: str) -> str:
    """Classify a question into an agent category via the LLM endpoint."""
    resp = w.serving_endpoints.query(
        name=serving_endpoint,
        messages=[
            {"role": "system", "content": CLASSIFY_PROMPT},
            {"role": "user", "content": question},
        ],
        max_tokens=16,
        temperature=0,
    )
    intent = (resp.choices[0].message.content or "").strip().lower()
    if intent not in ("discovery", "metrics", "qa", "general"):
        intent = "discovery"
    return intent


def call_agent(endpoint_name: str, question: str) -> str:
    """Call an agent serving endpoint and extract the text response."""
    payload = {"input": [{"role": "user", "content": question}]}
    resp = w.api_client.do(
        "POST",
        f"/serving-endpoints/{endpoint_name}/invocations",
        body=payload,
    )
    # Extract text from ResponsesAgent output format
    for item in resp.get("output", []):
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    return content.get("text", "")
    return str(resp.get("output", ""))


def general_response(question: str) -> str:
    """Handle general/greeting messages without tools."""
    resp = w.serving_endpoints.query(
        name=serving_endpoint,
        messages=[
            {"role": "system", "content": GENERAL_PROMPT},
            {"role": "user", "content": question},
        ],
        max_tokens=512,
        temperature=0.5,
    )
    return resp.choices[0].message.content or ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health Check

# COMMAND ----------

# Verify all agent endpoints are READY
for name, ep_name in agent_endpoints.items():
    ep = w.serving_endpoints.get(ep_name)
    state = ep.state
    print(f"  {ep_name}: ready={state.ready}, config_update={state.config_update}")
    assert "READY" in str(state.ready), f"Endpoint {ep_name} not ready: {state.ready}"

print("\nAll agent endpoints READY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Benchmarks

# COMMAND ----------

ERROR_PHRASES = ["unavailable", "not properly configured", "misconfigured",
                 "unable to", "couldn't be completed", "error processing"]

results = []

for i, bench in enumerate(BENCHMARKS, 1):
    question = bench["question"]
    expected = bench["expected_agent"]
    category = bench["category"]

    print(f"\n[{i}/{len(BENCHMARKS)}] ({category.upper()}) {question}")

    start = time.time()
    try:
        # Classify intent
        actual_agent = classify_intent(question)

        # Route to agent or handle general
        if actual_agent in agent_endpoints:
            response_text = call_agent(agent_endpoints[actual_agent], question)
        else:
            response_text = general_response(question)
            actual_agent = "general"

        elapsed = time.time() - start

        routing_ok = actual_agent == expected
        contains_ok = all(kw.lower() in response_text.lower() for kw in bench.get("expect_contains", []))
        has_content = len(response_text.strip()) > 10
        has_error = any(p in response_text.lower() for p in ERROR_PHRASES)

        if not has_content:
            status = "FAIL"
        elif not routing_ok or not contains_ok:
            status = "WARN"
        elif has_error:
            status = "WARN"
        else:
            status = "PASS"

        icon = {"PASS": "\u2705", "WARN": "\u26a0\ufe0f", "FAIL": "\u274c"}[status]
        print(f"  {icon} {status} | Agent: {actual_agent} (expected {expected}) | {elapsed:.1f}s")
        print(f"  {response_text[:200]}")
        if not routing_ok:
            print(f"  \u26a0\ufe0f ROUTING MISMATCH: got {actual_agent}")
        if has_error:
            print(f"  \u26a0\ufe0f RESPONSE ERROR detected")

        results.append({**bench, "status": status, "elapsed": elapsed,
                        "actual_agent": actual_agent, "response": response_text,
                        "routing_ok": routing_ok, "contains_ok": contains_ok})

    except Exception as e:
        elapsed = time.time() - start
        print(f"  \u274c FAIL ({e})")
        results.append({**bench, "status": "FAIL", "elapsed": elapsed,
                        "actual_agent": "", "response": str(e)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

pass_count = sum(1 for r in results if r["status"] == "PASS")
warn_count = sum(1 for r in results if r["status"] == "WARN")
fail_count = sum(1 for r in results if r["status"] == "FAIL")
routing_correct = sum(1 for r in results if r.get("routing_ok"))
total_time = sum(r["elapsed"] for r in results)
times = [r["elapsed"] for r in results]

print(f"Results: {pass_count} PASS / {warn_count} WARN / {fail_count} FAIL  (total {len(results)})")
print(f"Routing accuracy: {routing_correct}/{len(results)} ({100*routing_correct/len(results):.0f}%)")
if times:
    print(f"Total time: {total_time:.1f}s | Avg: {total_time/len(results):.1f}s | Min: {min(times):.1f}s | Max: {max(times):.1f}s")

print("\nPer-category breakdown:")
for cat in ["discovery", "metrics", "qa", "general"]:
    cat_results = [r for r in results if r["category"] == cat]
    if not cat_results:
        continue
    cat_pass = sum(1 for r in cat_results if r["status"] == "PASS")
    cat_avg = sum(r["elapsed"] for r in cat_results) / len(cat_results)
    print(f"  {cat:12s}: {cat_pass}/{len(cat_results)} pass | avg {cat_avg:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Results

# COMMAND ----------

import pandas as pd

df = pd.DataFrame([{
    "Category": r["category"],
    "Question": r["question"][:60],
    "Expected": r["expected_agent"],
    "Actual": r.get("actual_agent", ""),
    "Status": r["status"],
    "Time (s)": round(r["elapsed"], 1),
    "Response": r.get("response", "")[:100],
} for r in results])

display(df)
