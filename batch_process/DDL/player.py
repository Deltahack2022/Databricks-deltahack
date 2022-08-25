# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.stg_player;
# MAGIC CREATE OR REPLACE table deltahack_dev.stg_player (
# MAGIC   Player_Id INT,
# MAGIC   Player_Name STRING,
# MAGIC   DOB Date,
# MAGIC   Batting_hand STRING,
# MAGIC   Bowling_skill STRING,
# MAGIC   Country_Name STRING,
# MAGIC   Player_team STRING,
# MAGIC   Season_year INT
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.player;
# MAGIC CREATE OR REPLACE table deltahack_dev.player (
# MAGIC   Player_SK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Player_Id INT,
# MAGIC   Player_Name STRING,
# MAGIC   DOB Date,
# MAGIC   Batting_hand STRING,
# MAGIC   Bowling_skill STRING,
# MAGIC   Country_Name STRING,
# MAGIC   Player_team STRING,
# MAGIC   effc_start_dt Date,
# MAGIC   effc_end_dt Date
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


