# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists deltahack_dev.aggr_player_match;
# MAGIC CREATE OR REPLACE table deltahack_dev.aggr_player_match (
# MAGIC Match_Player_SK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC Match_Id	int,
# MAGIC Player_Id	int,
# MAGIC Player_Name	String,
# MAGIC DOB	Date,
# MAGIC Batting_hand	String,
# MAGIC Bowling_skill	String,
# MAGIC Country_Name	String,
# MAGIC Role_Desc	String,
# MAGIC Player_team	String,
# MAGIC Opposit_Team	String,
# MAGIC Season_year	String,
# MAGIC is_manofThematch	boolean,
# MAGIC Age_As_on_match	int,
# MAGIC IsPlayers_Team_won	boolean,
# MAGIC Batting_Status	String,
# MAGIC Bowling_Status	String,
# MAGIC Player_Captain	String,
# MAGIC Opposit_captain	String,
# MAGIC Player_keeper	String,
# MAGIC Opposite_keeper	String
# MAGIC ) USING DELTA
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------


