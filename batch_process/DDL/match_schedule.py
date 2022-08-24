# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.stg_match_schedule;
# MAGIC CREATE OR REPLACE table deltahack_dev.stg_match_schedule (
# MAGIC   match_id int,
# MAGIC Team1 string,
# MAGIC Team2 string,
# MAGIC match_date date,
# MAGIC Season_Year string,
# MAGIC Venue_Name string,
# MAGIC City_Name string,
# MAGIC Country_Name string
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.match_schedule;
# MAGIC CREATE OR REPLACE table deltahack_dev.match_schedule (
# MAGIC match_schedule_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   match_id int,
# MAGIC Team1 string,
# MAGIC Team2 string,
# MAGIC match_date date,
# MAGIC Season_Year string,
# MAGIC Venue_Name string,
# MAGIC City_Name string,
# MAGIC Country_Name string
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


