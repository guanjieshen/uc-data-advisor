#!/usr/bin/env python3
"""Benchmark suite for UC Data Advisor deployed app."""

import json
import time
import subprocess
import requests

APP_URL = "https://uc-data-advisor-7474649275991072.aws.databricksapps.com"
CHAT_ENDPOINT = f"{APP_URL}/api/chat"

# Get token from Databricks CLI
def get_token():
    result = subprocess.run(
        ["databricks", "auth", "token", "-p", "enbridge", "-o", "json"],
        capture_output=True, text=True,
    )
    return json.loads(result.stdout)["access_token"]

BENCHMARKS = [
    # Discovery agent questions
    {
        "question": "What catalogs are available in the workspace?",
        "expected_agent": "discovery",
        "expect_contains": ["catalog"],
        "category": "discovery",
    },
    {
        "question": "Do we have any data about pipeline safety incidents?",
        "expected_agent": "discovery",
        "expect_contains": [],
        "category": "discovery",
    },
    {
        "question": "Show me the columns in the nominations table",
        "expected_agent": "discovery",
        "expect_contains": [],
        "category": "discovery",
    },
    # Metrics agent questions
    {
        "question": "How many safety incidents were recorded in total?",
        "expected_agent": "metrics",
        "expect_contains": [],
        "category": "metrics",
    },
    {
        "question": "What is the total pipeline throughput?",
        "expected_agent": "metrics",
        "expect_contains": [],
        "category": "metrics",
    },
    # QA agent questions
    {
        "question": "How do I request access to a dataset?",
        "expected_agent": "qa",
        "expect_contains": [],
        "category": "qa",
    },
    {
        "question": "What data quality checks are in place?",
        "expected_agent": "qa",
        "expect_contains": [],
        "category": "qa",
    },
    {
        "question": "What is Unity Catalog and how is data organized?",
        "expected_agent": "qa",
        "expect_contains": [],
        "category": "qa",
    },
    # General / greeting
    {
        "question": "Hello, what can you help me with?",
        "expected_agent": "general",
        "expect_contains": [],
        "category": "general",
    },
]


def run_benchmark():
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Health check
    print("=" * 80)
    print("UC DATA ADVISOR — BENCHMARK SUITE")
    print("=" * 80)
    try:
        health = requests.get(f"{APP_URL}/api/health", headers=headers, timeout=10)
        print(f"\nHealth check: {health.status_code} — {health.json()}")
    except Exception as e:
        print(f"\nHealth check FAILED: {e}")
        return

    results = []
    total_time = 0

    for i, bench in enumerate(BENCHMARKS, 1):
        print(f"\n{'—' * 80}")
        print(f"[{i}/{len(BENCHMARKS)}] ({bench['category'].upper()}) {bench['question']}")
        print(f"  Expected agent: {bench['expected_agent']}")

        payload = {
            "messages": [{"role": "user", "content": bench["question"]}],
        }

        start = time.time()
        try:
            resp = requests.post(
                CHAT_ENDPOINT, json=payload, headers=headers, timeout=120
            )
            elapsed = time.time() - start
            total_time += elapsed

            if resp.status_code != 200:
                print(f"  STATUS: FAIL (HTTP {resp.status_code})")
                print(f"  Body: {resp.text[:300]}")
                results.append({
                    **bench, "status": "FAIL", "elapsed": elapsed,
                    "error": f"HTTP {resp.status_code}", "response": "", "actual_agent": "",
                })
                continue

            data = resp.json()
            response_text = data.get("response", "")
            actual_agent = data.get("agent", "unknown")
            routing_ok = actual_agent == bench["expected_agent"]
            contains_ok = all(
                kw.lower() in response_text.lower() for kw in bench["expect_contains"]
            )
            has_content = len(response_text.strip()) > 10

            status = "PASS" if (routing_ok and contains_ok and has_content) else "WARN"
            if not has_content:
                status = "FAIL"

            print(f"  STATUS: {status} | Agent: {actual_agent} (expected {bench['expected_agent']}) | {elapsed:.1f}s")
            print(f"  Response ({len(response_text)} chars): {response_text[:200]}...")

            if not routing_ok:
                print(f"  ⚠ ROUTING MISMATCH: got {actual_agent}, expected {bench['expected_agent']}")

            results.append({
                **bench, "status": status, "elapsed": elapsed,
                "actual_agent": actual_agent, "response": response_text,
                "routing_ok": routing_ok, "contains_ok": contains_ok,
            })

        except requests.Timeout:
            elapsed = time.time() - start
            total_time += elapsed
            print(f"  STATUS: FAIL (timeout after {elapsed:.1f}s)")
            results.append({
                **bench, "status": "FAIL", "elapsed": elapsed,
                "error": "timeout", "response": "", "actual_agent": "",
            })
        except Exception as e:
            elapsed = time.time() - start
            total_time += elapsed
            print(f"  STATUS: FAIL ({e})")
            results.append({
                **bench, "status": "FAIL", "elapsed": elapsed,
                "error": str(e), "response": "", "actual_agent": "",
            })

    # Summary
    print(f"\n{'=' * 80}")
    print("BENCHMARK SUMMARY")
    print(f"{'=' * 80}")

    pass_count = sum(1 for r in results if r["status"] == "PASS")
    warn_count = sum(1 for r in results if r["status"] == "WARN")
    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    routing_correct = sum(1 for r in results if r.get("routing_ok"))
    times = [r["elapsed"] for r in results]

    print(f"\nResults: {pass_count} PASS / {warn_count} WARN / {fail_count} FAIL  (total {len(results)})")
    print(f"Routing accuracy: {routing_correct}/{len(results)} ({100*routing_correct/len(results):.0f}%)")
    print(f"Total time: {total_time:.1f}s | Avg: {total_time/len(results):.1f}s | Min: {min(times):.1f}s | Max: {max(times):.1f}s")

    print(f"\nPer-category breakdown:")
    for cat in ["discovery", "metrics", "qa", "general"]:
        cat_results = [r for r in results if r["category"] == cat]
        if not cat_results:
            continue
        cat_pass = sum(1 for r in cat_results if r["status"] == "PASS")
        cat_avg = sum(r["elapsed"] for r in cat_results) / len(cat_results)
        print(f"  {cat:12s}: {cat_pass}/{len(cat_results)} pass | avg {cat_avg:.1f}s")

    # Write detailed results
    output_path = "tests/benchmark_results.json"
    serializable = []
    for r in results:
        s = {k: v for k, v in r.items()}
        serializable.append(s)
    with open(output_path, "w") as f:
        json.dump(serializable, f, indent=2, default=str)
    print(f"\nDetailed results saved to {output_path}")


if __name__ == "__main__":
    run_benchmark()
