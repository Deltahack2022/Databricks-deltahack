# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.stg_player_match;
# MAGIC CREATE OR REPLACE table deltahack_dev.stg_player_match (
# MAGIC   Match_Id string,
# MAGIC Player_Id string,
# MAGIC Role_Desc string,
# MAGIC Opposit_Team string,
# MAGIC Season_year string,
# MAGIC is_manofThematch string,
# MAGIC IsPlayers_Team_won string,
# MAGIC Batting_Status string,
# MAGIC Bowling_Status string,
# MAGIC Player_Captain string,
# MAGIC Opposit_captain string,
# MAGIC Player_keeper string,
# MAGIC Opposit_keeper string
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.player_match;
# MAGIC CREATE OR REPLACE table deltahack_dev.player_match (
# MAGIC Match_Player_SK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   Match_Id INT,
# MAGIC Player_Id INT,
# MAGIC Role_Desc string,
# MAGIC Opposit_Team string,
# MAGIC Season_year string,
# MAGIC is_manofThematch boolean,
# MAGIC IsPlayers_Team_won boolean,
# MAGIC Batting_Status string,
# MAGIC Bowling_Status string,
# MAGIC Player_Captain string,
# MAGIC Opposit_captain string,
# MAGIC Player_keeper string,
# MAGIC Opposit_keeper string
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


