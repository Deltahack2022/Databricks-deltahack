# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.aggr_match_stats;
# MAGIC CREATE OR REPLACE table deltahack_dev.aggr_match_stats (match_id	int,
# MAGIC Team1	String,
# MAGIC Team2	String,
# MAGIC match_date	Date,
# MAGIC Season_Year	String,
# MAGIC Venue_Name	String,
# MAGIC City_Name	String,
# MAGIC Country_Name	String,
# MAGIC Toss_winner	String,
# MAGIC match_winner	String,
# MAGIC Toss_Name	String,
# MAGIC Win_Type	String,
# MAGIC Outcome_Type	String,
# MAGIC ManOfMach	String,
# MAGIC Win_Margin	int
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


