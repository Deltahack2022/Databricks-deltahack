# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.batch_audit_table;
# MAGIC CREATE OR REPLACE table deltahack_dev.batch_audit_table (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   process String,
# MAGIC   error STRING,
# MAGIC   message STRING,
# MAGIC   createtimestamp TIMESTAMP
# MAGIC ) USING DELTA

# COMMAND ----------

insert into {db}.
