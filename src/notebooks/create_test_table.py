# Databricks notebook source
# MAGIC %md
# MAGIC # Create Test Table for UC Data Advisor
# MAGIC
# MAGIC This notebook creates a sample table to test the UC Data Advisor agent.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema if not exists
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
# MAGIC COMMENT 'Schema for UC Data Advisor test resources';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a sample customers table with metadata
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.test_customers (
# MAGIC   customer_id BIGINT COMMENT 'Unique customer identifier',
# MAGIC   first_name STRING COMMENT 'Customer first name',
# MAGIC   last_name STRING COMMENT 'Customer last name',
# MAGIC   email STRING COMMENT 'Customer email address',
# MAGIC   created_at TIMESTAMP COMMENT 'When the customer record was created',
# MAGIC   updated_at TIMESTAMP COMMENT 'When the customer record was last updated'
# MAGIC )
# MAGIC COMMENT 'Test customer table for UC Data Advisor demo'
# MAGIC TBLPROPERTIES (
# MAGIC   'created_by' = 'uc-data-advisor',
# MAGIC   'purpose' = 'testing'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample data
# MAGIC INSERT INTO ${catalog}.${schema}.test_customers VALUES
# MAGIC   (1, 'John', 'Doe', 'john.doe@example.com', current_timestamp(), current_timestamp()),
# MAGIC   (2, 'Jane', 'Smith', 'jane.smith@example.com', current_timestamp(), current_timestamp()),
# MAGIC   (3, 'Bob', 'Johnson', 'bob.j@example.com', current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a sample orders table
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.test_orders (
# MAGIC   order_id BIGINT COMMENT 'Unique order identifier',
# MAGIC   customer_id BIGINT COMMENT 'Reference to customer',
# MAGIC   order_date DATE COMMENT 'Date the order was placed',
# MAGIC   total_amount DECIMAL(10,2) COMMENT 'Total order amount in USD',
# MAGIC   status STRING COMMENT 'Order status: pending, shipped, delivered, cancelled'
# MAGIC )
# MAGIC COMMENT 'Test orders table for UC Data Advisor demo';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample order data
# MAGIC INSERT INTO ${catalog}.${schema}.test_orders VALUES
# MAGIC   (101, 1, '2024-01-15', 150.00, 'delivered'),
# MAGIC   (102, 1, '2024-02-20', 75.50, 'shipped'),
# MAGIC   (103, 2, '2024-03-01', 200.00, 'pending'),
# MAGIC   (104, 3, '2024-03-10', 99.99, 'delivered');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify tables were created
# MAGIC SHOW TABLES IN ${catalog}.${schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table metadata
# MAGIC DESCRIBE TABLE EXTENDED ${catalog}.${schema}.test_customers;

# COMMAND ----------

print("Test tables created successfully!")
print(f"Tables: {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}.test_customers")
print(f"        {dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}.test_orders")
