# Databricks notebook source
# MAGIC %run ../../Config/batch_configs/onetime_hist_configs

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

configs = player_match_configs

# COMMAND ----------

file_location = configs['file_location']
file_type = configs["file_type"]
file_name = configs['file_name']
season = configs['season']
file_path = file_location+"/"+file_name+"/"+season
infer_schema = configs['infer_schema']
first_row_is_header = configs['first_row_is_header']
delimiter = configs['delimiter']
db = configs['db']

# COMMAND ----------

df_bronze = spark.read.format(file_type) \
      .options(header=first_row_is_header, delimiter=delimiter,inferSchema=infer_schema) \
      .load(file_path)

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_player_match_dataset")

# COMMAND ----------

b_insert_query = f"""INSERT INTO {db}.stg_player_match TABLE bronze_player_match_dataset"""

# COMMAND ----------

spark.sql(b_insert_query)

# COMMAND ----------

commit_no = get_commit_no(f'{db}.stg_player_match')

# COMMAND ----------

bronze_player_match = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table(f'{db}.stg_player_match')

# COMMAND ----------

bronze_player_match = bronze_player_match.withColumn("is_manofThematch",F.when(bronze_player_match['is_manofThematch']==1 ,True).otherwise(False))
bronze_player_match = bronze_player_match.withColumn("IsPlayers_Team_won",F.when(bronze_player_match['IsPlayers_Team_won']==1 ,True).otherwise(False))

# COMMAND ----------

null_check_dq(bronze_player_match,'match_id')

# COMMAND ----------

null_check_dq(bronze_player_match,'Player_Id')

# COMMAND ----------

bronze_player_match.createOrReplaceTempView("silver_player_match_dataset")

# COMMAND ----------

s_insert_query = f"""
INSERT INTO {db}.player_match (Match_Id ,
Player_Id ,
Role_Desc ,
Opposit_Team ,
Season_year ,
is_manofThematch ,
IsPlayers_Team_won ,
Batting_Status ,
Bowling_Status ,
Player_Captain ,
Opposit_captain ,
Player_keeper ,
Opposit_keeper  )
SELECT Match_Id ,
Player_Id ,
Role_Desc ,
Opposit_Team ,
Season_year ,
is_manofThematch ,
IsPlayers_Team_won ,
Batting_Status ,
Bowling_Status ,
Player_Captain ,
Opposit_captain ,
Player_keeper ,
Opposit_keeper  
FROM silver_player_match_dataset"""

# COMMAND ----------

spark.sql(s_insert_query)

# COMMAND ----------


