=== Required grants for app_identity user: allan.cao@databricks.com ===

Unity Catalog (run as metastore admin or catalog owner):
  GRANT USE CATALOG ON CATALOG acao_accuweather TO `allan.cao@databricks.com`;
  GRANT SELECT ON CATALOG acao_accuweather TO `allan.cao@databricks.com`;
  -- Grant USE SCHEMA on each schema in acao_accuweather as needed
  GRANT ALL PRIVILEGES ON CATALOG acao_accuweather_advisor_catalog TO `allan.cao@databricks.com`;

Serving endpoint:
  Grant CAN_QUERY on endpoint "acao-accuweather-advisor-llm" via workspace UI or API

Vector Search:
  Grant CAN_USE on endpoint "acao-accuweather-advisor-vs" via workspace UI or API

Lakebase:
  Add user as instance role on "acao-accuweather-advisor-sessions"
  GRANT ALL ON DATABASE  TO "allan.cao@databricks.com";
  GRANT ALL ON SCHEMA public TO "allan.cao@databricks.com";
  GRANT ALL ON ALL TABLES IN SCHEMA public TO "allan.cao@databricks.com";
