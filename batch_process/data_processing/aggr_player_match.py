# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

pm_commit_no = get_commit_no('dth_test_db.player_match')
p_commit_no = get_commit_no('dth_test_db.player')
ms_commit_no = get_commit_no('dth_test_db.match_schedule')

# COMMAND ----------

silver_player_match = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", pm_commit_no) \
                  .table('dth_test_db.player_match')

# COMMAND ----------

silver_player = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", p_commit_no) \
                  .table('dth_test_db.player')

# COMMAND ----------

silver_match_schedule = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", ms_commit_no) \
                  .table('dth_test_db.match_schedule')

# COMMAND ----------

joined_player_match = silver_player_match.join(silver_player.where(F.col('effc_end_dt')=='9999-12-31'), how='inner', on='Player_Id')
joined_player_match = joined_player_match.join(silver_match_schedule.select('Match_Id','match_date'), how='inner', on='Match_Id')

# COMMAND ----------

joined_player_match = joined_player_match.withColumn('age_as_on_match', (F.months_between(F.col('match_date'), F.col('DOB')) / 12).cast('int'))

# COMMAND ----------

aggr_player_match = joined_player_match[
'Match_Id',
'Player_Id',
'Player_Name',
'DOB',
'Batting_hand',
'Bowling_skill',
'Country_Name',
'Role_Desc',
'Player_team',
'Opposit_Team',
'Season_year',
'is_manofThematch',
'Age_As_on_match',
'IsPlayers_Team_won',
'Batting_Status',
'Bowling_Status',
'Player_Captain',
'Opposit_captain',
'Player_keeper',
'Opposit_keeper'
]

# COMMAND ----------

aggr_player_match.createOrReplaceTempView("aggr_player_match_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dth_test_db.aggr_player_match (
# MAGIC Match_Id,
# MAGIC Player_Id,
# MAGIC Player_Name,
# MAGIC DOB,
# MAGIC Batting_hand,
# MAGIC Bowling_skill,
# MAGIC Country_Name,
# MAGIC Role_Desc,
# MAGIC Player_team,
# MAGIC Opposit_Team,
# MAGIC Season_year,
# MAGIC is_manofThematch,
# MAGIC Age_As_on_match,
# MAGIC IsPlayers_Team_won,
# MAGIC Batting_Status,
# MAGIC Bowling_Status,
# MAGIC Player_Captain,
# MAGIC Opposit_captain,
# MAGIC Player_keeper,
# MAGIC Opposite_keeper
# MAGIC )
# MAGIC SELECT Match_Id,
# MAGIC Player_Id,
# MAGIC Player_Name,
# MAGIC DOB,
# MAGIC Batting_hand,
# MAGIC Bowling_skill,
# MAGIC Country_Name,
# MAGIC Role_Desc,
# MAGIC Player_team,
# MAGIC Opposit_Team,
# MAGIC Season_year,
# MAGIC is_manofThematch,
# MAGIC Age_As_on_match,
# MAGIC IsPlayers_Team_won,
# MAGIC Batting_Status,
# MAGIC Bowling_Status,
# MAGIC Player_Captain,
# MAGIC Opposit_captain,
# MAGIC Player_keeper,
# MAGIC Opposit_keeper
# MAGIC FROM aggr_player_match_data

# COMMAND ----------


