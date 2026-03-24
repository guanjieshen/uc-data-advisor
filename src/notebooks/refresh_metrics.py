"""Refresh Genie metric tables from Databricks Metric Views.

Metric Views in uc_data_advisor.default (mv_*) are the single source of truth.
Tables in enbridge_operations.metrics are materialized snapshots for Genie Space
(which cannot query metric views directly).

Each entry maps a metric view to a SELECT query using MEASURE() and the target table.

Run (fish shell):
  set -x DATABRICKS_CONFIG_PROFILE enbridge
  uv run --project /Users/allan.cao/Dev/uc-data-advisor \
    python src/notebooks/refresh_metrics.py
"""

import os
import time

os.environ["DATABRICKS_CONFIG_PROFILE"] = "enbridge"

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MV_CATALOG = "uc_data_advisor"
MV_SCHEMA = "default"
TABLE_CATALOG = "enbridge_operations"
TABLE_SCHEMA = "metrics"
WAREHOUSE_ID = "23bf2a865893d648"

# Each entry: (target_table, source_metric_view, SELECT query using MEASURE())
# All MEASURE() and dimension columns are aliased to snake_case for Delta compatibility.
REFRESHES = [
    (
        "safety_summary",
        "mv_safety",
        """SELECT
            MEASURE(`Total Incidents`) AS total_incidents,
            MEASURE(`Critical Incidents`) AS critical_incidents,
            MEASURE(`High Incidents`) AS high_incidents,
            MEASURE(`Total Injuries`) AS total_injuries,
            MEASURE(`Total Days Lost`) AS total_days_lost,
            MEASURE(`Avg Injuries Per Incident`) AS avg_injuries_per_incident,
            MEASURE(`Open Investigations`) AS open_investigations
        FROM {mv}""",
    ),
    (
        "safety_by_region",
        "mv_safety",
        """SELECT
            `Region` AS region,
            MEASURE(`Total Incidents`) AS incident_count,
            MEASURE(`Critical Incidents`) + MEASURE(`High Incidents`) AS critical_high_count,
            MEASURE(`Total Injuries`) AS total_injuries,
            MEASURE(`Total Days Lost`) AS total_days_lost
        FROM {mv}
        GROUP BY `Region`""",
    ),
    (
        "safety_by_type",
        "mv_safety",
        """SELECT
            `Incident Type` AS incident_type,
            MEASURE(`Total Incidents`) AS incident_count,
            MEASURE(`Total Injuries`) AS total_injuries,
            MEASURE(`Total Days Lost`) AS total_days_lost
        FROM {mv}
        GROUP BY `Incident Type`""",
    ),
    (
        "safety_monthly",
        "mv_safety",
        """SELECT
            `Incident Month` AS month,
            MEASURE(`Total Incidents`) AS incident_count,
            MEASURE(`Total Injuries`) AS injuries,
            MEASURE(`Total Days Lost`) AS days_lost,
            MEASURE(`Critical Incidents`) + MEASURE(`High Incidents`) AS critical_high_count
        FROM {mv}
        GROUP BY `Incident Month`""",
    ),
    (
        "pipeline_overview",
        "mv_pipeline",
        """SELECT
            MEASURE(`Total Pipelines`) AS total_pipelines,
            MEASURE(`Active Pipelines`) AS active_pipelines,
            MEASURE(`Total Length KM`) AS total_length_km,
            MEASURE(`Avg Length KM`) AS avg_length_km,
            MEASURE(`Avg Diameter Inches`) AS avg_diameter_inches,
            MEASURE(`Oldest Pipeline Year`) AS oldest_pipeline_year,
            MEASURE(`Newest Pipeline Year`) AS newest_pipeline_year
        FROM {mv}""",
    ),
    (
        "pipeline_by_region",
        "mv_pipeline",
        """SELECT
            `Region` AS region,
            MEASURE(`Total Pipelines`) AS pipeline_count,
            MEASURE(`Total Length KM`) AS total_length_km,
            MEASURE(`Avg Diameter Inches`) AS avg_diameter_inches,
            MEASURE(`Active Pipelines`) AS active_count
        FROM {mv}
        GROUP BY `Region`""",
    ),
    (
        "throughput_summary",
        "mv_throughput",
        """SELECT
            `Plant Name` AS plant_name,
            `Plant Region` AS region,
            `Plant Status` AS plant_status,
            MEASURE(`Avg Daily Inlet MMCFD`) AS avg_daily_inlet_mmcfd,
            MEASURE(`Avg Daily Outlet MMCFD`) AS avg_daily_outlet_mmcfd,
            MEASURE(`Total Inlet MMCFD`) AS total_inlet_mmcfd,
            MEASURE(`Avg Utilization Pct`) AS avg_utilization_pct,
            MEASURE(`Production Days`) AS total_production_days
        FROM {mv}
        GROUP BY `Plant Name`, `Plant Region`, `Plant Status`""",
    ),
    (
        "maintenance_summary",
        "mv_maintenance",
        """SELECT
            MEASURE(`Total Work Orders`) AS total_work_orders,
            MEASURE(`Open Orders`) AS open_orders,
            MEASURE(`In Progress Orders`) AS in_progress_orders,
            MEASURE(`Completed Orders`) AS completed_orders,
            MEASURE(`Emergency Orders`) AS emergency_orders,
            MEASURE(`Total Actual Cost CAD`) AS total_actual_cost_cad,
            MEASURE(`Avg Cost Per Order CAD`) AS avg_cost_per_order_cad,
            MEASURE(`Total Estimated Cost CAD`) AS total_estimated_cost_cad,
            MEASURE(`Cost Variance CAD`) AS cost_variance_cad
        FROM {mv}""",
    ),
    (
        "commercial_summary",
        "mv_commercial",
        """SELECT
            MEASURE(`Total Contracts`) AS total_contracts,
            MEASURE(`Active Contracts`) AS active_contracts,
            MEASURE(`Total Active Volume GJ Day`) AS total_active_volume_gj_day,
            MEASURE(`Avg Rate Per GJ`) AS avg_active_rate_per_gj,
            MEASURE(`Unique Shippers`) AS unique_shippers
        FROM {mv}""",
    ),
    (
        "nominations_daily",
        "mv_nominations",
        """SELECT
            `Gas Day` AS gas_day,
            `Shipper Name` AS shipper_name,
            `Pipeline Name` AS pipeline_name,
            MEASURE(`Total Nominated GJ`) AS total_nominated_gj,
            MEASURE(`Total Scheduled GJ`) AS total_scheduled_gj,
            MEASURE(`Total Actual GJ`) AS total_actual_gj,
            MEASURE(`Fulfillment Pct`) AS fulfillment_pct
        FROM {mv}
        GROUP BY `Gas Day`, `Shipper Name`, `Pipeline Name`""",
    ),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

w = WorkspaceClient()


def run_sql(statement: str, timeout: int = 60):
    """Execute SQL via the Statements API and wait for result."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    status = resp.status
    if status.state.value == "FAILED":
        raise RuntimeError(f"SQL failed: {status.error}")
    if status.state.value == "SUCCEEDED":
        return resp
    stmt_id = resp.statement_id
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(2)
        resp = w.statement_execution.get_statement(stmt_id)
        if resp.status.state.value == "SUCCEEDED":
            return resp
        if resp.status.state.value == "FAILED":
            raise RuntimeError(f"SQL failed: {resp.status.error}")
    raise TimeoutError(f"SQL statement {stmt_id} did not complete in {timeout}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Metric Table Refresh (from Databricks Metric Views)")
    print("=" * 60)
    print(f"Source metric views: {MV_CATALOG}.{MV_SCHEMA}.mv_*")
    print(f"Target tables:      {TABLE_CATALOG}.{TABLE_SCHEMA}.*")
    print(f"Warehouse:          {WAREHOUSE_ID}")
    print()

    run_sql(f"CREATE SCHEMA IF NOT EXISTS {TABLE_CATALOG}.{TABLE_SCHEMA}")

    succeeded = 0
    failed = 0

    for target_table, mv_name, query_template in REFRESHES:
        mv_fqn = f"{MV_CATALOG}.{MV_SCHEMA}.{mv_name}"
        table_fqn = f"{TABLE_CATALOG}.{TABLE_SCHEMA}.{target_table}"
        query = query_template.format(mv=mv_fqn)

        print(f"  {target_table}...", end=" ", flush=True)
        try:
            run_sql(f"CREATE OR REPLACE TABLE {table_fqn} AS {query}")

            resp = run_sql(f"SELECT COUNT(*) AS cnt FROM {table_fqn}")
            count = resp.result.data_array[0][0] if resp.result and resp.result.data_array else "?"
            print(f"OK ({count} rows)")
            succeeded += 1
        except Exception as e:
            print(f"FAILED: {e}")
            failed += 1

    print()
    print("=" * 60)
    print(f"Refresh complete: {succeeded} succeeded, {failed} failed")
    print("=" * 60)


if __name__ == "__main__":
    main()
