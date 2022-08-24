# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

mas_commit_no = get_commit_no('dth_test_db.Match_Status')
ms_commit_no = get_commit_no('dth_test_db.match_schedule')

# COMMAND ----------

silver_match_schedule = spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                  .option("startingVersion", ms_commit_no) \
                  .table('dth_test_db.match_schedule')

silver_match_status = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", mas_commit_no) \
                  .table('dth_test_db.match_status')

# COMMAND ----------

aggr_match_stats = silver_match_status.join(silver_match_schedule, how='inner', on='Match_Id')

# COMMAND ----------

aggr_match_stats= aggr_match_stats['match_id',
'Team1',
'Team2',
'match_date',
'Season_Year',
'Venue_Name',
'City_Name',
'Country_Name',
'Toss_winner',
'match_winner',
'Toss_Name',
'Win_Type',
'Outcome_Type',
'ManOfMach',
'Win_Margin']

# COMMAND ----------

aggr_match_stats.createOrReplaceTempView("aggr_match_stats_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dth_test_db.aggr_match_stats (
# MAGIC match_id,
# MAGIC Team1,
# MAGIC Team2,
# MAGIC match_date,
# MAGIC Season_Year,
# MAGIC Venue_Name,
# MAGIC City_Name,
# MAGIC Country_Name,
# MAGIC Toss_winner,
# MAGIC match_winner,
# MAGIC Toss_Name,
# MAGIC Win_Type,
# MAGIC Outcome_Type,
# MAGIC ManOfMach,
# MAGIC Win_Margin
# MAGIC )
# MAGIC SELECT match_id,
# MAGIC Team1,
# MAGIC Team2,
# MAGIC match_date,
# MAGIC Season_Year,
# MAGIC Venue_Name,
# MAGIC City_Name,
# MAGIC Country_Name,
# MAGIC Toss_winner,
# MAGIC match_winner,
# MAGIC Toss_Name,
# MAGIC Win_Type,
# MAGIC Outcome_Type,
# MAGIC ManOfMach,
# MAGIC Win_Margin
# MAGIC FROM aggr_match_stats_data

# COMMAND ----------


