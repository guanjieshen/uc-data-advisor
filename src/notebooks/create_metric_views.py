"""Create Databricks Metric Views as single source of truth for Enbridge metrics.

Metric views define reusable dimensions and measures in YAML.
The materialized tables in enbridge_operations.metrics are refreshed from these.

Run (fish shell):
  set -x DATABRICKS_CONFIG_PROFILE enbridge
  uv run --project /Users/allan.cao/Dev/uc-data-advisor \
    python src/notebooks/create_metric_views.py
"""

import os
import time

os.environ["DATABRICKS_CONFIG_PROFILE"] = "enbridge"

from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "23bf2a865893d648"
CATALOG = "uc_data_advisor"
SCHEMA = "default"

w = WorkspaceClient()


def run_sql(statement: str, timeout: int = 60):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    if resp.status.state.value == "FAILED":
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    if resp.status.state.value == "SUCCEEDED":
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
    raise TimeoutError(f"SQL timed out after {timeout}s")


METRIC_VIEWS = {
    "mv_safety": f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_safety
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Safety incident metrics across Enbridge operations"
source: enbridge_operations.safety_compliance.incidents
dimensions:
  - name: Severity
    expr: severity
    comment: "Incident severity: Critical, High, Medium, Low"
  - name: Incident Type
    expr: incident_type
    comment: "Near Miss, First Aid, Medical Treatment, Environmental Release, Lost Time, Property Damage, Regulatory Violation"
  - name: Region
    expr: region
    comment: "Alberta, British Columbia, Saskatchewan, Ontario, Gulf Coast"
  - name: Root Cause
    expr: root_cause
  - name: Investigation Status
    expr: investigation_status
  - name: Incident Month
    expr: DATE_TRUNC('MONTH', incident_date)
  - name: Incident Year
    expr: DATE_TRUNC('YEAR', incident_date)
measures:
  - name: Total Incidents
    expr: COUNT(1)
  - name: Total Injuries
    expr: SUM(injuries)
  - name: Total Days Lost
    expr: SUM(days_lost)
  - name: Avg Injuries Per Incident
    expr: "ROUND(SUM(injuries) * 1.0 / COUNT(1), 2)"
  - name: Avg Days Lost Per Incident
    expr: "ROUND(SUM(days_lost) * 1.0 / COUNT(1), 2)"
  - name: Critical Incidents
    expr: COUNT(1) FILTER (WHERE severity = 'Critical')
  - name: High Incidents
    expr: COUNT(1) FILTER (WHERE severity = 'High')
  - name: Open Investigations
    expr: COUNT(1) FILTER (WHERE investigation_status = 'Open')
  - name: Total Corrective Actions
    expr: SUM(corrective_actions_count)
$$
""",

    "mv_pipeline": f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_pipeline
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Pipeline infrastructure metrics"
source: enbridge_operations.pipeline_monitoring.pipelines
dimensions:
  - name: Region
    expr: region
  - name: Pipeline Type
    expr: pipeline_type
  - name: Material
    expr: material
  - name: Status
    expr: status
    comment: "Active, Inactive, Decommissioned"
  - name: Installation Decade
    expr: "CONCAT(FLOOR(installation_year / 10) * 10, 's')"
measures:
  - name: Total Pipelines
    expr: COUNT(1)
  - name: Active Pipelines
    expr: COUNT(1) FILTER (WHERE status = 'Active')
  - name: Total Length KM
    expr: ROUND(SUM(length_km), 1)
  - name: Avg Length KM
    expr: ROUND(AVG(length_km), 1)
  - name: Avg Diameter Inches
    expr: ROUND(AVG(diameter_inches), 1)
  - name: Oldest Pipeline Year
    expr: MIN(installation_year)
  - name: Newest Pipeline Year
    expr: MAX(installation_year)
  - name: Avg Max Operating Pressure KPA
    expr: ROUND(AVG(max_operating_pressure_kpa), 0)
$$
""",

    "mv_throughput": f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_throughput
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Gas processing throughput metrics"
source: enbridge_operations.gas_processing.daily_throughput
joins:
  - name: processing_plants
    source: enbridge_operations.gas_processing.processing_plants
    using:
      - plant_id

dimensions:
  - name: Plant Name
    expr: processing_plants.plant_name
  - name: Plant Region
    expr: processing_plants.region
  - name: Plant Status
    expr: processing_plants.status
    comment: "Operational, Turnaround, Standby"
  - name: Production Month
    expr: DATE_TRUNC('MONTH', production_date)
  - name: Production Year
    expr: DATE_TRUNC('YEAR', production_date)
measures:
  - name: Total Inlet MMCFD
    expr: ROUND(SUM(inlet_volume_mmcfd), 1)
  - name: Total Outlet MMCFD
    expr: ROUND(SUM(outlet_volume_mmcfd), 1)
  - name: Avg Daily Inlet MMCFD
    expr: ROUND(AVG(inlet_volume_mmcfd), 1)
  - name: Avg Daily Outlet MMCFD
    expr: ROUND(AVG(outlet_volume_mmcfd), 1)
  - name: Avg Utilization Pct
    expr: "ROUND(AVG(100.0 * inlet_volume_mmcfd / processing_plants.capacity_mmcfd), 1)"
  - name: Avg Shrinkage Pct
    expr: ROUND(AVG(shrinkage_pct), 2)
  - name: Total Flare MMCFD
    expr: ROUND(SUM(flare_volume_mmcfd), 1)
  - name: Production Days
    expr: COUNT(1)
$$
""",

    "mv_maintenance": f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_maintenance
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Pipeline maintenance work order metrics"
source: enbridge_operations.pipeline_monitoring.maintenance_work_orders
dimensions:
  - name: Work Order Type
    expr: work_order_type
  - name: Priority
    expr: priority
    comment: "Emergency, High, Medium, Low"
  - name: Status
    expr: status
    comment: "Open, In Progress, Completed, Cancelled"
  - name: Created Month
    expr: DATE_TRUNC('MONTH', created_date)
measures:
  - name: Total Work Orders
    expr: COUNT(1)
  - name: Open Orders
    expr: COUNT(1) FILTER (WHERE status = 'Open')
  - name: In Progress Orders
    expr: COUNT(1) FILTER (WHERE status = 'In Progress')
  - name: Completed Orders
    expr: COUNT(1) FILTER (WHERE status = 'Completed')
  - name: Emergency Orders
    expr: COUNT(1) FILTER (WHERE priority = 'Emergency')
  - name: Total Actual Cost CAD
    expr: ROUND(SUM(actual_cost_cad), 2)
  - name: Avg Cost Per Order CAD
    expr: ROUND(AVG(actual_cost_cad), 2)
  - name: Total Estimated Cost CAD
    expr: ROUND(SUM(estimated_cost_cad), 2)
  - name: Cost Variance CAD
    expr: "ROUND(SUM(actual_cost_cad) - SUM(estimated_cost_cad), 2)"
$$
""",

    "mv_commercial": f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_commercial
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Transportation contract metrics"
source: enbridge_commercial.contracts.transportation_contracts
dimensions:
  - name: Contract Type
    expr: contract_type
    comment: "Firm, Seasonal, Interruptible"
  - name: Contract Status
    expr: status
    comment: "Active, Expired, Terminated"
measures:
  - name: Total Contracts
    expr: COUNT(1)
  - name: Active Contracts
    expr: COUNT(1) FILTER (WHERE status = 'Active')
  - name: Total Active Volume GJ Day
    expr: SUM(contracted_volume_gj_day) FILTER (WHERE status = 'Active')
  - name: Avg Rate Per GJ
    expr: ROUND(AVG(rate_per_gj) FILTER (WHERE status = 'Active'), 4)
  - name: Unique Shippers
    expr: COUNT(DISTINCT shipper_id)
  - name: Avg Take Or Pay Pct
    expr: ROUND(AVG(take_or_pay_pct), 1)
$$
""",

    "mv_nominations": f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_nominations
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Gas nomination and flow metrics"
source: enbridge_commercial.contracts.nominations
joins:
  - name: shippers
    source: enbridge_commercial.contracts.shippers
    using:
      - shipper_id

  - name: pipelines
    source: enbridge_operations.pipeline_monitoring.pipelines
    using:
      - pipeline_id

dimensions:
  - name: Shipper Name
    expr: shippers.shipper_name
  - name: Pipeline Name
    expr: pipelines.pipeline_name
  - name: Nomination Cycle
    expr: nomination_cycle
  - name: Nomination Status
    expr: status
  - name: Gas Day
    expr: gas_day
  - name: Gas Month
    expr: DATE_TRUNC('MONTH', gas_day)
measures:
  - name: Total Nominated GJ
    expr: SUM(nominated_volume_gj)
  - name: Total Scheduled GJ
    expr: SUM(scheduled_volume_gj)
  - name: Total Actual GJ
    expr: SUM(actual_volume_gj)
  - name: Fulfillment Pct
    expr: "ROUND(100.0 * SUM(actual_volume_gj) / NULLIF(SUM(nominated_volume_gj), 0), 1)"
  - name: Scheduling Pct
    expr: "ROUND(100.0 * SUM(scheduled_volume_gj) / NULLIF(SUM(nominated_volume_gj), 0), 1)"
  - name: Total Nominations
    expr: COUNT(1)
$$
""",
}


def main():
    print("Creating Databricks Metric Views")
    print("=" * 60)
    print(f"Target: {CATALOG}.{SCHEMA}")
    print()

    succeeded = 0
    failed = 0

    for name, sql in METRIC_VIEWS.items():
        print(f"  {name}...", end=" ", flush=True)
        try:
            run_sql(sql.strip())
            print("OK")
            succeeded += 1
        except Exception as e:
            print(f"FAILED: {e}")
            failed += 1

    print()
    print("=" * 60)
    print(f"Done: {succeeded} succeeded, {failed} failed")
    print("=" * 60)


if __name__ == "__main__":
    main()
