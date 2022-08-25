# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.stg_team;
# MAGIC CREATE OR REPLACE table deltahack_dev.stg_team (
# MAGIC   Team_Id INT,
# MAGIC   Team_Name STRING
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.Team;
# MAGIC CREATE OR REPLACE table deltahack_dev.Team (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Team_Id INT,
# MAGIC   Team_Name STRING,
# MAGIC   Team_Name_Short STRING
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


