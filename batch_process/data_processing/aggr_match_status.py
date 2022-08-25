# Databricks notebook source
env = dbutils.widgets.get('ENV')

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

db = 'deltahack_dev' if env == 'dev' else 'deltahack_prod'

# COMMAND ----------

silver_match_schedule = spark.read.format("delta") \
                  .table(f'{db}.match_schedule')

silver_match_status = spark.read.format("delta") \
                  .table(f'{db}.match_status')

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

s_merge_query = f"""merge INTO {db}.aggr_match_stats as a using 
(select * from aggr_match_stats_data) as b 
on a.match_id = b.match_id 
when matched then update set
a.Team1 = b.Team1,
a.Team2 = b.Team2,
a.match_date = b.match_date,
a.Season_Year = b.Season_Year,
a.Venue_Name = b.Venue_Name,
a.City_Name = b.City_Name,
a.Country_Name = b.Country_Name,
a.Toss_winner = b.Toss_winner,
a.match_winner = b.match_winner,
a.Toss_Name = b.Toss_Name,
a.Win_Type = b.Win_Type,
a.Outcome_Type = b.Outcome_Type,
a.ManOfMach = b.ManOfMach,
a.Win_Margin = b.Win_Margin
when not matched then insert (
match_id,
Team1,
Team2,
match_date,
Season_Year,
Venue_Name,
City_Name,
Country_Name,
Toss_winner,
match_winner,
Toss_Name,
Win_Type,
Outcome_Type,
ManOfMach,
Win_Margin
)
values (b.match_id,
b.Team1,
b.Team2,
b.match_date,
b.Season_Year,
b.Venue_Name,
b.City_Name,
b.Country_Name,
b.Toss_winner,
b.match_winner,
b.Toss_Name,
b.Win_Type,
b.Outcome_Type,
b.ManOfMach,
b.Win_Margin)"""

# COMMAND ----------

spark.sql(s_merge_query)

# COMMAND ----------


