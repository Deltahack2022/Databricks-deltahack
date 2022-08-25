# Databricks notebook source
env = dbutils.widgets.get('ENV')

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

db = 'deltahack_dev' if env == 'dev' else 'deltahack_prod'

# COMMAND ----------

silver_player_match = spark.read.format("delta") \
                  .table(f'{db}.player_match')

# COMMAND ----------

silver_player = spark.read.format("delta") \
                  .table(f'{db}.player')

# COMMAND ----------

silver_match_schedule = spark.read.format("delta") \
                  .table(f'{db}.match_schedule')

# COMMAND ----------

joined_player_match = silver_player_match.join(silver_player.where(F.col('effc_end_dt')=='9999-12-31'), how='inner', on='Player_Id')
joined_player_match = joined_player_match.join(silver_match_schedule.select('Match_Id','match_date'), how='left', on='Match_Id')

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

s_insert_query = f"""Merge INTO {db}.aggr_player_match  as a  
using (select * from aggr_player_match_data) as b 
on a.match_id = b.match_id and a.player_id = b.player_id
when matched then 
update set 
a.Player_Name = b.Player_Name,
a.DOB = b.DOB,
a.Batting_hand = b.Batting_hand,
a.Bowling_skill = b.Bowling_skill,
a.Country_Name = b.Country_Name,
a.Role_Desc = b.Role_Desc,
a.Player_team = b.Player_team,
a.Opposit_Team = b.Opposit_Team,
a.Season_year = b.Season_year,
a.is_manofThematch = b.is_manofThematch,
a.Age_As_on_match = b.Age_As_on_match,
a.IsPlayers_Team_won = b.IsPlayers_Team_won,
a.Batting_Status = b.Batting_Status,
a.Bowling_Status = b.Bowling_Status,
a.Player_Captain = b.Player_Captain,
a.Opposit_captain = b.Opposit_captain,
a.Player_keeper = b.Player_keeper,
a.Opposite_keeper = b.Opposit_keeper
when not matched then 
insert (
Match_Id,
Player_Id,
Player_Name,
DOB,
Batting_hand,
Bowling_skill,
Country_Name,
Role_Desc,
Player_team,
Opposit_Team,
Season_year,
is_manofThematch,
Age_As_on_match,
IsPlayers_Team_won,
Batting_Status,
Bowling_Status,
Player_Captain,
Opposit_captain,
Player_keeper,
Opposite_keeper
)
values (b.Match_Id,
b.Player_Id,
b.Player_Name,
b.DOB,
b.Batting_hand,
b.Bowling_skill,
b.Country_Name,
b.Role_Desc,
b.Player_team,
b.Opposit_Team,
b.Season_year,
b.is_manofThematch,
b.Age_As_on_match,
b.IsPlayers_Team_won,
b.Batting_Status,
b.Bowling_Status,
b.Player_Captain,
b.Opposit_captain,
b.Player_keeper,
b.Opposit_keeper)"""

# COMMAND ----------

spark.sql(s_insert_query)

# COMMAND ----------


