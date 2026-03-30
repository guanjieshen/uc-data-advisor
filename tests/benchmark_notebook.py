# Databricks notebook source
# MAGIC %md
# MAGIC # UC Data Advisor — Benchmark Suite
# MAGIC
# MAGIC Run this notebook directly in a Databricks workspace to benchmark the deployed UC Data Advisor app.
# MAGIC
# MAGIC **Setup:** Set the two widgets below (`app_url` and `config_path`), then Run All.

# COMMAND ----------

dbutils.widgets.text("app_url", "", "App URL")
dbutils.widgets.text("config_path", "/Workspace/Users/allan.cao@databricks.com/uc-data-advisor/config/advisor_config.yaml", "Config Path")

APP_URL = dbutils.widgets.get("app_url").strip().rstrip("/")
CONFIG_PATH = dbutils.widgets.get("config_path").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Config & Auth

# COMMAND ----------

import json, time, yaml, requests
from databricks.sdk import WorkspaceClient

# Auth — uses notebook's built-in credentials
w = WorkspaceClient()
token = w.config.authenticate()["Authorization"].replace("Bearer ", "")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Resolve app URL from config if not provided
if not APP_URL:
    with open(CONFIG_PATH) as f:
        _config = yaml.safe_load(f) or {}
    app_name = _config.get("infrastructure", {}).get("app_name", "")
    if app_name:
        try:
            app_info = w.apps.get(app_name)
            APP_URL = app_info.url.rstrip("/") if app_info.url else ""
        except Exception:
            pass

if not APP_URL:
    raise ValueError("Set app_url widget or ensure config has infrastructure.app_name")

print(f"App URL: {APP_URL}")

# Load benchmarks from config
try:
    with open(CONFIG_PATH) as f:
        _config = yaml.safe_load(f) or {}
except FileNotFoundError:
    _config = {}

BENCHMARKS = _config.get("generated", {}).get("benchmarks", [
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
# MAGIC ## Health Check

# COMMAND ----------

CHAT_ENDPOINT = f"{APP_URL}/api/chat"

health = requests.get(f"{APP_URL}/api/health", headers=headers, timeout=10)
print(f"Health: {health.status_code} — {health.text[:200]}")
assert health.status_code == 200, f"Health check failed: {health.status_code}"

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

    payload = {"messages": [{"role": "user", "content": question}]}
    start = time.time()

    try:
        resp = requests.post(CHAT_ENDPOINT, json=payload, headers=headers, timeout=300)
        elapsed = time.time() - start

        if resp.status_code != 200:
            print(f"  FAIL (HTTP {resp.status_code})")
            results.append({**bench, "status": "FAIL", "elapsed": elapsed,
                            "actual_agent": "", "response": resp.text[:200]})
            continue

        data = resp.json()
        response_text = data.get("response", "")
        actual_agent = data.get("agent", "unknown")
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

        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[status]
        print(f"  {icon} {status} | Agent: {actual_agent} (expected {expected}) | {elapsed:.1f}s")
        print(f"  {response_text[:200]}")
        if not routing_ok:
            print(f"  ⚠️ ROUTING MISMATCH: got {actual_agent}")
        if has_error:
            print(f"  ⚠️ RESPONSE ERROR detected")

        results.append({**bench, "status": status, "elapsed": elapsed,
                        "actual_agent": actual_agent, "response": response_text,
                        "routing_ok": routing_ok, "contains_ok": contains_ok})

    except requests.Timeout:
        elapsed = time.time() - start
        print(f"  ❌ FAIL (timeout after {elapsed:.1f}s)")
        results.append({**bench, "status": "FAIL", "elapsed": elapsed,
                        "actual_agent": "", "response": ""})
    except Exception as e:
        elapsed = time.time() - start
        print(f"  ❌ FAIL ({e})")
        results.append({**bench, "status": "FAIL", "elapsed": elapsed,
                        "actual_agent": "", "response": ""})

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
