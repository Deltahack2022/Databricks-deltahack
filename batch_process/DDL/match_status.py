# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.stg_match_status;
# MAGIC CREATE OR REPLACE table deltahack_dev.stg_match_status (
# MAGIC   match_id int,
# MAGIC Toss_winner string,
# MAGIC match_winner string,
# MAGIC Toss_Name string,
# MAGIC Win_Type string,
# MAGIC Outcome_Type string,
# MAGIC ManOfMach string,
# MAGIC Win_Margin string
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.match_status;
# MAGIC CREATE OR REPLACE table deltahack_dev.match_status (
# MAGIC match_status_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   match_id int,
# MAGIC Toss_winner string,
# MAGIC match_winner string,
# MAGIC Toss_Name string,
# MAGIC Win_Type string,
# MAGIC Outcome_Type string,
# MAGIC ManOfMach int,
# MAGIC Win_Margin int
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


