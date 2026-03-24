# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Knowledge Base for Q&A Agent
# MAGIC
# MAGIC Populates a Delta table with FAQ entries and documentation about the Enbridge data catalog.
# MAGIC Creates a Vector Search index for semantic retrieval by the Q&A Agent.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

# Configuration
TARGET_CATALOG = "uc_data_advisor"
TARGET_SCHEMA = "default"
TARGET_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.knowledge_base"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# FAQ entries about the Enbridge data catalog
faq_entries = [
    {
        "question": "How do I request access to a dataset?",
        "answer": "Submit a data access request through the Unity Catalog access request portal. Your request will be routed to the data steward for the relevant catalog. Access is granted based on your role and business justification. Typical turnaround is 2-3 business days.",
        "category": "access",
        "source": "Data Governance Policy",
    },
    {
        "question": "What is Unity Catalog?",
        "answer": "Unity Catalog is Databricks' unified governance solution for all data and AI assets. It provides centralized access control, auditing, lineage, and discovery across all Enbridge workspaces. All datasets are organized in a three-level namespace: catalog.schema.table.",
        "category": "general",
        "source": "Platform Documentation",
    },
    {
        "question": "How is the data organized?",
        "answer": "Enbridge data in Unity Catalog follows a domain-based catalog structure. The main catalogs are: enbridge_operations (pipeline monitoring, gas processing, safety/compliance), enbridge_commercial (contracts, nominations, market data), and enbridge_analytics (derived datasets and ML features).",
        "category": "general",
        "source": "Data Architecture Guide",
    },
    {
        "question": "What is a data steward?",
        "answer": "A data steward is the designated owner responsible for data quality, access governance, and metadata accuracy within a specific catalog or schema. Data stewards review access requests and ensure compliance with Enbridge data policies.",
        "category": "governance",
        "source": "Data Governance Policy",
    },
    {
        "question": "How often is pipeline sensor data refreshed?",
        "answer": "Pipeline sensor data in enbridge_operations.pipeline_monitoring is refreshed every 15 minutes via streaming ingestion from SCADA systems. Historical data is available from January 2020 onwards.",
        "category": "data_freshness",
        "source": "Pipeline Monitoring SLA",
    },
    {
        "question": "What data quality checks are in place?",
        "answer": "All ingested datasets go through automated data quality checks including: null validation on required fields, range checks for sensor values, referential integrity across related tables, and timeliness checks. Quality metrics are published to the data quality dashboard.",
        "category": "quality",
        "source": "Data Quality Framework",
    },
    {
        "question": "Can I create my own tables in Unity Catalog?",
        "answer": "Users with the CREATE TABLE privilege can create tables in designated sandbox schemas. For production datasets, submit a request to the Data Platform team with your schema design and data source details. All production tables must have column descriptions and a data steward assigned.",
        "category": "access",
        "source": "Self-Service Data Guidelines",
    },
    {
        "question": "How do I report a data quality issue?",
        "answer": "Report data quality issues through the Data Quality portal or by contacting the data steward listed in the table's metadata. Include the fully qualified table name, specific columns affected, and a description of the issue. Critical issues affecting safety data should be escalated immediately.",
        "category": "quality",
        "source": "Data Incident Response",
    },
    {
        "question": "What is data lineage and how can I view it?",
        "answer": "Data lineage tracks how data flows from source to destination, including all transformations. In Unity Catalog, you can view lineage for any table by clicking the Lineage tab in the catalog explorer. This shows upstream sources and downstream consumers.",
        "category": "governance",
        "source": "Platform Documentation",
    },
    {
        "question": "What compliance requirements apply to our data?",
        "answer": "Enbridge data is subject to multiple regulatory frameworks including NERC CIP for critical infrastructure, SOX for financial reporting data, and provincial/state pipeline safety regulations. Access controls are configured to meet these requirements. Personal data is classified and subject to privacy policies.",
        "category": "compliance",
        "source": "Regulatory Compliance Guide",
    },
    {
        "question": "How do I connect to the data from my tools?",
        "answer": "You can access Unity Catalog data through: Databricks notebooks and SQL editor (direct), Power BI and Tableau (via Databricks SQL connector), Python/R (via Databricks Connect or SQL connector), and REST APIs. All connections require authentication through your Databricks workspace credentials.",
        "category": "access",
        "source": "Data Access Guide",
    },
    {
        "question": "What commercial data is available?",
        "answer": "The enbridge_commercial catalog contains: gas_contracts (active transportation agreements), daily_nominations (shipper volume nominations), market_pricing (commodity price data including AECO, Henry Hub, Dawn), and invoicing data. Access requires commercial team authorization.",
        "category": "data_catalog",
        "source": "Commercial Data Catalog",
    },
    {
        "question": "How is sensitive data protected?",
        "answer": "Sensitive data is protected through: column-level access controls in Unity Catalog, dynamic data masking for PII fields, encryption at rest and in transit, audit logging of all data access, and row-level security where required. Data classification labels are applied to all columns containing sensitive information.",
        "category": "security",
        "source": "Data Security Policy",
    },
    {
        "question": "What safety and compliance data do we track?",
        "answer": "Safety data is in enbridge_operations and includes: safety_incidents (reportable and non-reportable events), inspection_records (inline and field inspections), environmental_monitoring (emissions and environmental data), and regulatory_filings. This data has strict access controls and audit requirements.",
        "category": "data_catalog",
        "source": "Safety Data Governance",
    },
    {
        "question": "Who do I contact for help with the data platform?",
        "answer": "For platform issues: contact the Data Platform team via ServiceNow or #data-platform Slack channel. For data access: contact the relevant data steward. For data quality issues: use the Data Quality portal. For urgent production issues: page the on-call data engineer.",
        "category": "support",
        "source": "Support Directory",
    },
]

# COMMAND ----------

# Write FAQ entries to Delta table
rows = [Row(**entry) for entry in faq_entries]
df = spark.createDataFrame(rows)
df.write.mode("overwrite").saveAsTable(TARGET_TABLE)
print(f"Wrote {len(rows)} FAQ entries to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Index for Knowledge Base

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    VectorIndexType,
)

w = WorkspaceClient()
VS_ENDPOINT = "uc-advisor-vs"
VS_INDEX = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.knowledge_vs_index"

# Concatenate question + answer for embedding
spark.sql(f"""
    CREATE OR REPLACE TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.knowledge_base_embedded AS
    SELECT *, concat(question, ' ', answer) AS search_text
    FROM {TARGET_TABLE}
""")

try:
    w.vector_search_indexes.create_index(
        name=VS_INDEX,
        endpoint_name=VS_ENDPOINT,
        primary_key="question",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.knowledge_base_embedded",
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="search_text",
                    embedding_model_endpoint_name="databricks-bge-large-en",
                )
            ],
            pipeline_type="TRIGGERED",
        ),
    )
    print(f"Created VS index: {VS_INDEX}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"VS index already exists: {VS_INDEX}")
        w.vector_search_indexes.sync_index(index_name=VS_INDEX)
        print("Triggered index sync")
    else:
        raise
