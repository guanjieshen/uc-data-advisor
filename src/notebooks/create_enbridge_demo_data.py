"""Enbridge Demo Data Generator — Midstream Oil & Gas Pipeline Operations.

Generates realistic Unity Catalog structure with demo data for a midstream
pipeline/gas processing company. Uses Polars + NumPy locally, writes to UC
via Databricks Connect (serverless).

Catalogs:
  - enbridge_operations: pipeline_monitoring, gas_processing, safety_compliance
  - enbridge_commercial: contracts, market_data

Run:
  DATABRICKS_CONFIG_PROFILE=enbridge uv run \
    --with polars --with numpy --with "databricks-connect>=16.4,<17.0" \
    src/notebooks/create_enbridge_demo_data.py
"""

import numpy as np
import polars as pl
from databricks.connect import DatabricksSession

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEED = 42
rng = np.random.default_rng(SEED)

CATALOGS = {
    "enbridge_operations": "Core operational data for pipeline and midstream operations",
    "enbridge_commercial": "Commercial, financial, and customer data",
}
SCHEMAS = {
    "enbridge_operations.pipeline_monitoring": "Pipeline asset monitoring and IoT sensor data",
    "enbridge_operations.gas_processing": "Gas processing plant operations and throughput",
    "enbridge_operations.safety_compliance": "Safety incidents, regulatory filings, and emissions",
    "enbridge_commercial.contracts": "Shipper contracts and daily nominations",
    "enbridge_commercial.market_data": "Commodity prices and supply/demand forecasts",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def date_range(start: str, end: str, size: int) -> np.ndarray:
    s = np.datetime64(start)
    span = (np.datetime64(end) - s).astype(int)
    return s + rng.integers(0, span + 1, size=size).astype("timedelta64[D]")


def ts_range(start: str, end: str, size: int) -> np.ndarray:
    s = np.datetime64(start)
    span = int((np.datetime64(end) - s) / np.timedelta64(1, "ms"))
    return s + rng.integers(0, span + 1, size=size).astype("timedelta64[ms]")


def weighted_choice(values: list, weights: list, size: int) -> np.ndarray:
    w = np.array(weights, dtype=np.float64)
    return rng.choice(values, size=size, p=w / w.sum())


def write_table(spark, df: pl.DataFrame, full_name: str, comment: str,
                col_comments: dict | None = None):
    """Write a Polars DataFrame to UC via Connect, then set table/column comments."""
    spark_df = spark.createDataFrame(df.to_pandas())
    (spark_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(full_name))
    count = spark.table(full_name).count()
    print(f"  {full_name}: {count:,} rows")

    # Set table comment
    safe_comment = comment.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{safe_comment}'")

    # Set column comments
    if col_comments:
        for col, desc in col_comments.items():
            safe_desc = desc.replace("'", "\\'")
            spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN {col} COMMENT '{safe_desc}'")


# ===========================================================================
# Data Generators
# ===========================================================================

# --- enbridge_operations.pipeline_monitoring ---

def gen_pipelines(n: int = 50) -> pl.DataFrame:
    pipeline_ids = np.arange(1, n + 1)
    regions = ["Alberta", "British Columbia", "Saskatchewan", "Ontario", "Gulf Coast"]
    types_ = ["Natural Gas", "Crude Oil", "NGL", "Diluent"]
    materials = ["Carbon Steel", "High-Strength Steel", "Composite"]
    statuses = ["Active", "Maintenance", "Decommissioned", "Under Construction"]

    names = [f"Line {chr(65 + i // 10)}-{100 + i}" for i in range(n)]

    return pl.DataFrame({
        "pipeline_id": pipeline_ids,
        "pipeline_name": names,
        "pipeline_type": weighted_choice(types_, [40, 30, 20, 10], n),
        "diameter_inches": rng.choice([12, 16, 20, 24, 30, 36, 42, 48], size=n),
        "length_km": np.round(rng.uniform(50, 1500, size=n), 1),
        "material": weighted_choice(materials, [60, 30, 10], n),
        "region": rng.choice(regions, size=n),
        "max_operating_pressure_kpa": np.round(rng.uniform(4000, 12000, size=n), 0).astype(int),
        "installation_year": rng.integers(1960, 2024, size=n),
        "status": weighted_choice(statuses, [80, 10, 5, 5], n),
    })

PIPELINES_COMMENTS = {
    "pipeline_id": "Unique pipeline identifier",
    "pipeline_name": "Human-readable pipeline designation",
    "pipeline_type": "Product transported: Natural Gas, Crude Oil, NGL, or Diluent",
    "diameter_inches": "Nominal pipe diameter in inches",
    "length_km": "Total pipeline length in kilometers",
    "material": "Pipe material composition",
    "region": "Geographic operating region",
    "max_operating_pressure_kpa": "Maximum allowable operating pressure in kilopascals",
    "installation_year": "Year the pipeline was installed",
    "status": "Current operational status",
}


def gen_pipeline_sensors(n: int = 5000, n_pipelines: int = 50) -> pl.DataFrame:
    sensor_types = ["Pressure", "Temperature", "Flow Rate", "Vibration"]
    units = {"Pressure": "kPa", "Temperature": "°C", "Flow Rate": "m³/h", "Vibration": "mm/s"}
    sensor_type_arr = weighted_choice(sensor_types, [35, 25, 25, 15], n)
    unit_arr = np.array([units[s] for s in sensor_type_arr])

    # Realistic value ranges per sensor type
    values = np.zeros(n)
    for st, (lo, hi) in [("Pressure", (3000, 10000)), ("Temperature", (-20, 80)),
                          ("Flow Rate", (100, 5000)), ("Vibration", (0.1, 15))]:
        mask = sensor_type_arr == st
        values[mask] = np.round(rng.uniform(lo, hi, size=mask.sum()), 2)

    return pl.DataFrame({
        "reading_id": np.arange(1, n + 1),
        "pipeline_id": rng.integers(1, n_pipelines + 1, size=n),
        "sensor_type": sensor_type_arr,
        "reading_value": values,
        "unit": unit_arr,
        "reading_timestamp": ts_range("2024-01-01", "2026-03-16", n),
        "quality_flag": weighted_choice(["Good", "Suspect", "Bad"], [92, 6, 2], n),
        "station_km": np.round(rng.uniform(0, 1500, size=n), 1),
    })

SENSORS_COMMENTS = {
    "reading_id": "Unique sensor reading identifier",
    "pipeline_id": "FK to pipelines table",
    "sensor_type": "Type of measurement: Pressure, Temperature, Flow Rate, or Vibration",
    "reading_value": "Measured value in the corresponding unit",
    "unit": "Unit of measurement for the reading",
    "reading_timestamp": "Timestamp when the reading was taken",
    "quality_flag": "Data quality indicator: Good, Suspect, or Bad",
    "station_km": "Distance along pipeline in kilometers where sensor is located",
}


def gen_maintenance_work_orders(n: int = 500, n_pipelines: int = 50) -> pl.DataFrame:
    wo_types = ["Preventive", "Corrective", "Emergency", "Regulatory"]
    priorities = ["Low", "Medium", "High", "Critical"]
    statuses = ["Open", "In Progress", "Completed", "Cancelled"]

    return pl.DataFrame({
        "work_order_id": np.arange(1, n + 1),
        "pipeline_id": rng.integers(1, n_pipelines + 1, size=n),
        "work_order_type": weighted_choice(wo_types, [40, 30, 15, 15], n),
        "priority": weighted_choice(priorities, [20, 40, 25, 15], n),
        "status": weighted_choice(statuses, [15, 20, 55, 10], n),
        "description": [f"WO-{i:04d}: Maintenance activity on pipeline segment" for i in range(1, n + 1)],
        "created_date": date_range("2023-01-01", "2026-03-16", n),
        "scheduled_date": date_range("2023-06-01", "2026-06-30", n),
        "completed_date": date_range("2023-06-01", "2026-03-16", n),
        "estimated_cost_cad": np.round(rng.lognormal(9, 1.2, size=n), 2),
        "actual_cost_cad": np.round(rng.lognormal(9, 1.3, size=n), 2),
    })

WO_COMMENTS = {
    "work_order_id": "Unique work order identifier",
    "pipeline_id": "FK to pipelines table",
    "work_order_type": "Preventive, Corrective, Emergency, or Regulatory",
    "priority": "Work order priority level",
    "status": "Current work order status",
    "description": "Work order description",
    "created_date": "Date the work order was created",
    "scheduled_date": "Planned execution date",
    "completed_date": "Actual completion date (null if not completed)",
    "estimated_cost_cad": "Estimated cost in Canadian dollars",
    "actual_cost_cad": "Actual cost in Canadian dollars",
}


def gen_inspection_reports(n: int = 200, n_pipelines: int = 50) -> pl.DataFrame:
    methods = ["Inline Inspection (ILI)", "Hydrostatic Test", "Visual Inspection",
               "Ultrasonic", "Magnetic Flux Leakage"]
    findings = ["No Defects", "Minor Corrosion", "Wall Thinning", "Dent",
                "Crack", "Coating Damage", "Weld Anomaly"]

    return pl.DataFrame({
        "inspection_id": np.arange(1, n + 1),
        "pipeline_id": rng.integers(1, n_pipelines + 1, size=n),
        "inspection_method": rng.choice(methods, size=n),
        "inspection_date": date_range("2020-01-01", "2026-03-16", n),
        "finding": weighted_choice(findings, [35, 20, 15, 10, 8, 7, 5], n),
        "severity_score": rng.integers(1, 11, size=n),
        "anomaly_count": rng.integers(0, 25, size=n),
        "wall_loss_pct": np.round(rng.uniform(0, 40, size=n), 1),
        "requires_repair": weighted_choice(["Yes", "No"], [25, 75], n),
        "station_start_km": np.round(rng.uniform(0, 1400, size=n), 1),
        "station_end_km": np.round(rng.uniform(10, 1500, size=n), 1),
    })

INSPECTION_COMMENTS = {
    "inspection_id": "Unique inspection report identifier",
    "pipeline_id": "FK to pipelines table",
    "inspection_method": "Inspection technique used",
    "inspection_date": "Date the inspection was conducted",
    "finding": "Primary finding category",
    "severity_score": "Severity score from 1 (minor) to 10 (critical)",
    "anomaly_count": "Number of anomalies detected in the segment",
    "wall_loss_pct": "Maximum wall thickness loss as a percentage",
    "requires_repair": "Whether immediate repair action is required",
    "station_start_km": "Start of inspected segment in kilometers",
    "station_end_km": "End of inspected segment in kilometers",
}


# --- enbridge_operations.gas_processing ---

def gen_processing_plants(n: int = 15) -> pl.DataFrame:
    regions = ["Alberta", "British Columbia", "Saskatchewan", "Gulf Coast", "Midwest"]
    plant_names = [
        "Empress", "Cochrane", "Straddle", "Channahon", "Aux Sable",
        "Younger", "Pine River", "Saturn", "Brazeau River", "Wild Rose",
        "Gold Creek", "Dawson Creek", "Aitken Creek", "Edson", "Ram River",
    ]
    statuses = ["Operational", "Turnaround", "Standby"]

    return pl.DataFrame({
        "plant_id": np.arange(1, n + 1),
        "plant_name": plant_names[:n],
        "region": rng.choice(regions, size=n),
        "capacity_mmcfd": rng.integers(100, 2000, size=n),
        "plant_type": weighted_choice(["Cryogenic", "Lean Oil", "Refrigeration"], [50, 30, 20], n),
        "status": weighted_choice(statuses, [85, 10, 5], n),
        "commissioning_year": rng.integers(1985, 2023, size=n),
        "latitude": np.round(rng.uniform(49.0, 57.0, size=n), 4),
        "longitude": np.round(rng.uniform(-120.0, -110.0, size=n), 4),
    })

PLANTS_COMMENTS = {
    "plant_id": "Unique plant identifier",
    "plant_name": "Gas processing plant name",
    "region": "Geographic operating region",
    "capacity_mmcfd": "Nameplate capacity in million cubic feet per day",
    "plant_type": "Processing technology: Cryogenic, Lean Oil, or Refrigeration",
    "status": "Current plant status",
    "commissioning_year": "Year the plant was commissioned",
    "latitude": "Plant latitude",
    "longitude": "Plant longitude",
}


def gen_daily_throughput(n: int = 2000, n_plants: int = 15) -> pl.DataFrame:
    # Generate sequential dates per plant
    plant_ids = np.repeat(np.arange(1, n_plants + 1), n // n_plants + 1)[:n]
    dates = date_range("2024-01-01", "2026-03-16", n)

    capacities = rng.integers(100, 2000, size=n)
    utilization = rng.uniform(0.55, 0.98, size=n)

    return pl.DataFrame({
        "record_id": np.arange(1, n + 1),
        "plant_id": plant_ids.astype(int),
        "production_date": dates,
        "inlet_volume_mmcfd": np.round(capacities * utilization, 1),
        "outlet_volume_mmcfd": np.round(capacities * utilization * rng.uniform(0.92, 0.99, size=n), 1),
        "shrinkage_pct": np.round(rng.uniform(1.0, 8.0, size=n), 2),
        "plant_fuel_mmcfd": np.round(rng.uniform(2, 20, size=n), 1),
        "flare_volume_mmcfd": np.round(rng.exponential(0.5, size=n), 2),
    })

THROUGHPUT_COMMENTS = {
    "record_id": "Unique daily throughput record identifier",
    "plant_id": "FK to processing_plants table",
    "production_date": "Date of production",
    "inlet_volume_mmcfd": "Inlet gas volume in MMcf/d",
    "outlet_volume_mmcfd": "Outlet (sales) gas volume in MMcf/d",
    "shrinkage_pct": "Volume reduction percentage from processing",
    "plant_fuel_mmcfd": "Gas consumed as plant fuel in MMcf/d",
    "flare_volume_mmcfd": "Volume of gas flared in MMcf/d",
}


def gen_product_yields(n: int = 3000, n_plants: int = 15) -> pl.DataFrame:
    products = ["Methane", "Ethane", "Propane", "Butane", "NGL Mix", "Condensate"]
    plant_ids = rng.integers(1, n_plants + 1, size=n)
    product_arr = rng.choice(products, size=n)

    # Volume varies by product type
    volumes = np.zeros(n)
    for prod, (lo, hi) in [("Methane", (500, 1500)), ("Ethane", (50, 300)),
                            ("Propane", (30, 200)), ("Butane", (10, 80)),
                            ("NGL Mix", (20, 150)), ("Condensate", (5, 60))]:
        mask = product_arr == prod
        volumes[mask] = np.round(rng.uniform(lo, hi, size=mask.sum()), 1)

    return pl.DataFrame({
        "yield_id": np.arange(1, n + 1),
        "plant_id": plant_ids,
        "production_date": date_range("2024-01-01", "2026-03-16", n),
        "product": product_arr,
        "volume_bpd": volumes,
        "purity_pct": np.round(rng.uniform(90, 99.9, size=n), 2),
        "spec_met": weighted_choice(["Yes", "No"], [95, 5], n),
    })

YIELDS_COMMENTS = {
    "yield_id": "Unique yield record identifier",
    "plant_id": "FK to processing_plants table",
    "production_date": "Date of production",
    "product": "Output product: Methane, Ethane, Propane, Butane, NGL Mix, or Condensate",
    "volume_bpd": "Production volume in barrels per day",
    "purity_pct": "Product purity as a percentage",
    "spec_met": "Whether the product met pipeline specifications",
}


# --- enbridge_operations.safety_compliance ---

def gen_incidents(n: int = 150) -> pl.DataFrame:
    types_ = ["Near Miss", "First Aid", "Medical Treatment", "Lost Time",
              "Environmental Release", "Property Damage", "Vehicle Incident"]
    root_causes = ["Human Error", "Equipment Failure", "Procedure Gap",
                   "Weather", "Third Party Damage", "Corrosion", "Design Issue"]

    return pl.DataFrame({
        "incident_id": np.arange(1, n + 1),
        "incident_date": date_range("2020-01-01", "2026-03-16", n),
        "incident_type": weighted_choice(types_, [35, 20, 15, 10, 8, 7, 5], n),
        "severity": weighted_choice(["Low", "Medium", "High", "Critical"], [30, 35, 25, 10], n),
        "root_cause": rng.choice(root_causes, size=n),
        "region": rng.choice(["Alberta", "British Columbia", "Saskatchewan",
                              "Ontario", "Gulf Coast"], size=n),
        "injuries": weighted_choice([0, 1, 2, 3], [70, 20, 7, 3], n).astype(int),
        "days_lost": weighted_choice([0, 1, 2, 3, 5, 10, 20], [50, 15, 10, 8, 7, 5, 5], n).astype(int),
        "investigation_status": weighted_choice(
            ["Open", "In Progress", "Closed"], [10, 25, 65], n),
        "corrective_actions_count": rng.integers(0, 8, size=n),
    })

INCIDENTS_COMMENTS = {
    "incident_id": "Unique incident identifier",
    "incident_date": "Date of the incident",
    "incident_type": "Classification of the incident",
    "severity": "Severity level: Low, Medium, High, or Critical",
    "root_cause": "Identified root cause of the incident",
    "region": "Region where the incident occurred",
    "injuries": "Number of injuries resulting from the incident",
    "days_lost": "Number of lost workdays due to the incident",
    "investigation_status": "Current investigation status",
    "corrective_actions_count": "Number of corrective actions assigned",
}


def gen_regulatory_filings(n: int = 100) -> pl.DataFrame:
    agencies = ["PHMSA", "NEB/CER", "AER", "TCEQ", "EPA"]
    filing_types = ["Annual Report", "Incident Report", "Integrity Management Plan",
                    "Emergency Response Plan", "Emissions Report", "Permit Renewal"]

    return pl.DataFrame({
        "filing_id": np.arange(1, n + 1),
        "regulatory_agency": rng.choice(agencies, size=n),
        "filing_type": rng.choice(filing_types, size=n),
        "filing_date": date_range("2020-01-01", "2026-03-16", n),
        "due_date": date_range("2020-01-01", "2026-06-30", n),
        "status": weighted_choice(["Submitted", "Accepted", "Under Review",
                                   "Revision Required"], [40, 35, 15, 10], n),
        "region": rng.choice(["Alberta", "British Columbia", "Saskatchewan",
                              "Ontario", "Gulf Coast"], size=n),
        "compliance_year": rng.integers(2020, 2027, size=n),
    })

FILINGS_COMMENTS = {
    "filing_id": "Unique filing identifier",
    "regulatory_agency": "Regulatory body: PHMSA, NEB/CER, AER, TCEQ, or EPA",
    "filing_type": "Type of regulatory filing",
    "filing_date": "Date the filing was submitted",
    "due_date": "Filing due date",
    "status": "Current filing status",
    "region": "Applicable regulatory region",
    "compliance_year": "Year the filing covers",
}


def gen_environmental_monitoring(n: int = 1000) -> pl.DataFrame:
    parameters = ["CO2", "CH4", "NOx", "SO2", "VOC", "Particulate Matter"]
    methods = ["CEMS", "LDAR", "OGI Camera", "Ambient Monitor", "Stack Test"]

    return pl.DataFrame({
        "monitoring_id": np.arange(1, n + 1),
        "monitoring_date": date_range("2023-01-01", "2026-03-16", n),
        "parameter": rng.choice(parameters, size=n),
        "measurement_value": np.round(rng.lognormal(2, 1.5, size=n), 3),
        "unit": weighted_choice(["tonnes", "kg", "ppm", "mg/m³"], [30, 25, 25, 20], n),
        "detection_method": rng.choice(methods, size=n),
        "exceedance": weighted_choice(["No", "Yes"], [92, 8], n),
        "facility_type": weighted_choice(
            ["Compressor Station", "Processing Plant", "Pipeline ROW", "Meter Station"],
            [35, 30, 20, 15], n),
        "region": rng.choice(["Alberta", "British Columbia", "Saskatchewan",
                              "Ontario", "Gulf Coast"], size=n),
    })

ENV_COMMENTS = {
    "monitoring_id": "Unique monitoring record identifier",
    "monitoring_date": "Date of the environmental reading",
    "parameter": "Measured emission parameter: CO2, CH4, NOx, SO2, VOC, or PM",
    "measurement_value": "Measured emission value",
    "unit": "Unit of measurement",
    "detection_method": "Monitoring method used",
    "exceedance": "Whether the reading exceeded regulatory limits",
    "facility_type": "Type of facility being monitored",
    "region": "Region where the monitoring occurred",
}


# --- enbridge_commercial.contracts ---

def gen_shippers(n: int = 30) -> pl.DataFrame:
    shipper_names = [
        "CNRL", "Suncor Energy", "Imperial Oil", "Husky Energy", "Cenovus",
        "MEG Energy", "Pengrowth", "Athabasca Oil", "Penn West", "Baytex Energy",
        "Encana", "Vermilion Energy", "Whitecap Resources", "Crescent Point",
        "Tourmaline Oil", "ARC Resources", "Seven Generations", "Peyto",
        "Birchcliff Energy", "NuVista Energy", "Kelt Exploration", "Bonterra Energy",
        "Obsidian Energy", "Gear Energy", "InPlay Oil", "Surge Energy",
        "Cardinal Energy", "Tamarack Valley", "Spartan Delta", "Headwater Exploration",
    ]
    types_ = ["Producer", "Marketer", "Utility", "Industrial"]

    return pl.DataFrame({
        "shipper_id": np.arange(1, n + 1),
        "shipper_name": shipper_names[:n],
        "shipper_type": weighted_choice(types_, [50, 25, 15, 10], n),
        "credit_rating": weighted_choice(["AAA", "AA", "A", "BBB", "BB"],
                                          [10, 25, 35, 20, 10], n),
        "province_state": rng.choice(["Alberta", "British Columbia", "Saskatchewan",
                                       "Ontario", "Texas"], size=n),
        "contract_start_date": date_range("2015-01-01", "2024-01-01", n),
        "is_active": weighted_choice(["Yes", "No"], [90, 10], n),
    })

SHIPPERS_COMMENTS = {
    "shipper_id": "Unique shipper identifier",
    "shipper_name": "Company name of the shipper/customer",
    "shipper_type": "Producer, Marketer, Utility, or Industrial",
    "credit_rating": "Shipper credit rating",
    "province_state": "Shipper headquarters province or state",
    "contract_start_date": "Date the shipper relationship began",
    "is_active": "Whether the shipper currently has active contracts",
}


def gen_transportation_contracts(n: int = 100, n_shippers: int = 30,
                                  n_pipelines: int = 50) -> pl.DataFrame:
    terms = ["Firm", "Interruptible", "Seasonal"]

    return pl.DataFrame({
        "contract_id": np.arange(1, n + 1),
        "shipper_id": rng.integers(1, n_shippers + 1, size=n),
        "pipeline_id": rng.integers(1, n_pipelines + 1, size=n),
        "contract_type": weighted_choice(terms, [60, 25, 15], n),
        "contracted_volume_gj_day": rng.integers(5000, 500000, size=n),
        "rate_per_gj": np.round(rng.uniform(0.50, 3.50, size=n), 4),
        "effective_date": date_range("2020-01-01", "2025-01-01", n),
        "expiry_date": date_range("2025-06-01", "2035-12-31", n),
        "status": weighted_choice(["Active", "Expired", "Pending"], [70, 20, 10], n),
        "take_or_pay_pct": rng.integers(70, 101, size=n),
    })

CONTRACTS_COMMENTS = {
    "contract_id": "Unique contract identifier",
    "shipper_id": "FK to shippers table",
    "pipeline_id": "FK to pipelines table (enbridge_operations.pipeline_monitoring)",
    "contract_type": "Firm, Interruptible, or Seasonal",
    "contracted_volume_gj_day": "Contracted daily volume in gigajoules",
    "rate_per_gj": "Transportation rate per gigajoule in CAD",
    "effective_date": "Contract start date",
    "expiry_date": "Contract end date",
    "status": "Current contract status",
    "take_or_pay_pct": "Minimum take-or-pay percentage obligation",
}


def gen_nominations(n: int = 3000, n_shippers: int = 30,
                     n_pipelines: int = 50) -> pl.DataFrame:
    return pl.DataFrame({
        "nomination_id": np.arange(1, n + 1),
        "shipper_id": rng.integers(1, n_shippers + 1, size=n),
        "pipeline_id": rng.integers(1, n_pipelines + 1, size=n),
        "gas_day": date_range("2024-01-01", "2026-03-16", n),
        "nominated_volume_gj": rng.integers(1000, 200000, size=n),
        "scheduled_volume_gj": rng.integers(1000, 200000, size=n),
        "actual_volume_gj": rng.integers(800, 200000, size=n),
        "nomination_cycle": weighted_choice(
            ["Timely", "Evening", "Intraday 1", "Intraday 2", "Intraday 3"],
            [40, 25, 15, 12, 8], n),
        "status": weighted_choice(["Confirmed", "Pending", "Rejected"], [80, 15, 5], n),
    })

NOMINATIONS_COMMENTS = {
    "nomination_id": "Unique nomination identifier",
    "shipper_id": "FK to shippers table",
    "pipeline_id": "FK to pipelines table (enbridge_operations.pipeline_monitoring)",
    "gas_day": "Gas day for the nomination",
    "nominated_volume_gj": "Volume nominated by the shipper in GJ",
    "scheduled_volume_gj": "Volume scheduled by the pipeline operator in GJ",
    "actual_volume_gj": "Actual metered volume in GJ",
    "nomination_cycle": "NAESB nomination cycle",
    "status": "Nomination confirmation status",
}


# --- enbridge_commercial.market_data ---

def gen_commodity_prices(n: int = 2000) -> pl.DataFrame:
    hubs = ["AECO", "Henry Hub", "Dawn", "Empress", "Chicago Citygate"]
    # Generate sequential-ish dates
    dates = np.sort(date_range("2020-01-01", "2026-03-16", n))

    hub_arr = rng.choice(hubs, size=n)
    # Base prices vary by hub
    base_prices = np.zeros(n)
    for hub, base in [("AECO", 2.50), ("Henry Hub", 3.50), ("Dawn", 3.20),
                       ("Empress", 2.80), ("Chicago Citygate", 3.30)]:
        mask = hub_arr == hub
        base_prices[mask] = base

    # Add seasonal variation and noise
    day_of_year = (dates - np.datetime64("2020-01-01")).astype(int) % 365
    seasonal = 1.5 * np.sin(2 * np.pi * (day_of_year - 30) / 365)  # winter premium
    prices = base_prices + seasonal + rng.normal(0, 0.5, size=n)
    prices = np.maximum(prices, 0.50)  # floor

    return pl.DataFrame({
        "price_id": np.arange(1, n + 1),
        "trade_date": dates,
        "hub": hub_arr,
        "commodity": weighted_choice(["Natural Gas", "NGL", "Crude Oil"], [60, 25, 15], n),
        "price_per_gj": np.round(prices, 4),
        "currency": weighted_choice(["CAD", "USD"], [55, 45], n),
        "volume_traded_gj": rng.integers(10000, 5000000, size=n),
    })

PRICES_COMMENTS = {
    "price_id": "Unique price record identifier",
    "trade_date": "Date of the price observation",
    "hub": "Trading hub: AECO, Henry Hub, Dawn, Empress, or Chicago Citygate",
    "commodity": "Commodity type",
    "price_per_gj": "Price per gigajoule",
    "currency": "Price currency (CAD or USD)",
    "volume_traded_gj": "Volume traded in gigajoules",
}


def gen_supply_demand_forecast(n: int = 500) -> pl.DataFrame:
    regions = ["Western Canada", "Eastern Canada", "US Midwest", "Gulf Coast", "Northeast US"]
    scenarios = ["Base", "High Growth", "Low Growth", "Constrained Infrastructure"]

    return pl.DataFrame({
        "forecast_id": np.arange(1, n + 1),
        "forecast_date": date_range("2025-01-01", "2030-12-31", n),
        "region": rng.choice(regions, size=n),
        "scenario": weighted_choice(scenarios, [40, 20, 20, 20], n),
        "supply_bcfd": np.round(rng.uniform(5, 25, size=n), 2),
        "demand_bcfd": np.round(rng.uniform(5, 25, size=n), 2),
        "export_capacity_bcfd": np.round(rng.uniform(2, 15, size=n), 2),
        "price_forecast_per_gj": np.round(rng.uniform(1.50, 8.00, size=n), 4),
    })

FORECAST_COMMENTS = {
    "forecast_id": "Unique forecast record identifier",
    "forecast_date": "Date the forecast applies to",
    "region": "Geographic region of the forecast",
    "scenario": "Planning scenario: Base, High Growth, Low Growth, or Constrained",
    "supply_bcfd": "Projected supply in billion cubic feet per day",
    "demand_bcfd": "Projected demand in billion cubic feet per day",
    "export_capacity_bcfd": "Available export pipeline capacity in Bcf/d",
    "price_forecast_per_gj": "Forecasted price per gigajoule",
}


# ===========================================================================
# Main
# ===========================================================================

def main():
    print("Connecting to Enbridge workspace via Databricks Connect (serverless)...")
    spark = DatabricksSession.builder.profile("enbridge").serverless().getOrCreate()

    # Catalogs must be created via CLI with --storage-root (Default Storage workspace).
    # Verify they exist:
    for catalog in CATALOGS:
        spark.sql(f"USE CATALOG {catalog}")
        print(f"  Catalog exists: {catalog}")

    # Create schemas
    for schema, comment in SCHEMAS.items():
        print(f"Creating schema: {schema}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comment}'")

    # --- enbridge_operations.pipeline_monitoring ---
    print("\n=== enbridge_operations.pipeline_monitoring ===")
    write_table(spark, gen_pipelines(),
                "enbridge_operations.pipeline_monitoring.pipelines",
                "Pipeline asset registry — physical characteristics, regions, and operational status",
                PIPELINES_COMMENTS)

    write_table(spark, gen_pipeline_sensors(),
                "enbridge_operations.pipeline_monitoring.pipeline_sensors",
                "IoT sensor readings from pipeline infrastructure — pressure, temperature, flow, vibration",
                SENSORS_COMMENTS)

    write_table(spark, gen_maintenance_work_orders(),
                "enbridge_operations.pipeline_monitoring.maintenance_work_orders",
                "Scheduled and reactive maintenance records for pipeline assets",
                WO_COMMENTS)

    write_table(spark, gen_inspection_reports(),
                "enbridge_operations.pipeline_monitoring.inspection_reports",
                "Inline inspection (ILI) and anomaly detection results",
                INSPECTION_COMMENTS)

    # --- enbridge_operations.gas_processing ---
    print("\n=== enbridge_operations.gas_processing ===")
    write_table(spark, gen_processing_plants(),
                "enbridge_operations.gas_processing.processing_plants",
                "Gas processing facility registry — capacity, type, and location",
                PLANTS_COMMENTS)

    write_table(spark, gen_daily_throughput(),
                "enbridge_operations.gas_processing.daily_throughput",
                "Daily gas volumes processed per plant — inlet, outlet, shrinkage, flare",
                THROUGHPUT_COMMENTS)

    write_table(spark, gen_product_yields(),
                "enbridge_operations.gas_processing.product_yields",
                "Output product breakdown by plant — methane, ethane, propane, butane, NGL, condensate",
                YIELDS_COMMENTS)

    # --- enbridge_operations.safety_compliance ---
    print("\n=== enbridge_operations.safety_compliance ===")
    write_table(spark, gen_incidents(),
                "enbridge_operations.safety_compliance.incidents",
                "Safety incidents and near-misses with root cause analysis",
                INCIDENTS_COMMENTS)

    write_table(spark, gen_regulatory_filings(),
                "enbridge_operations.safety_compliance.regulatory_filings",
                "PHMSA, NEB/CER, AER, and EPA compliance filings",
                FILINGS_COMMENTS)

    write_table(spark, gen_environmental_monitoring(),
                "enbridge_operations.safety_compliance.environmental_monitoring",
                "Emissions readings, leak detection, and environmental monitoring data",
                ENV_COMMENTS)

    # --- enbridge_commercial.contracts ---
    print("\n=== enbridge_commercial.contracts ===")
    write_table(spark, gen_shippers(),
                "enbridge_commercial.contracts.shippers",
                "Shipper and customer registry — producers, marketers, utilities",
                SHIPPERS_COMMENTS)

    write_table(spark, gen_transportation_contracts(),
                "enbridge_commercial.contracts.transportation_contracts",
                "Pipeline capacity transportation contracts — firm, interruptible, seasonal",
                CONTRACTS_COMMENTS)

    write_table(spark, gen_nominations(),
                "enbridge_commercial.contracts.nominations",
                "Daily gas flow nominations by shipper — nominated, scheduled, and actual volumes",
                NOMINATIONS_COMMENTS)

    # --- enbridge_commercial.market_data ---
    print("\n=== enbridge_commercial.market_data ===")
    write_table(spark, gen_commodity_prices(),
                "enbridge_commercial.market_data.commodity_prices",
                "Daily AECO, Henry Hub, Dawn, Empress, and Chicago Citygate commodity prices",
                PRICES_COMMENTS)

    write_table(spark, gen_supply_demand_forecast(),
                "enbridge_commercial.market_data.supply_demand_forecast",
                "Regional supply/demand projections under multiple planning scenarios",
                FORECAST_COMMENTS)

    print("\n✅ All 15 tables created successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
