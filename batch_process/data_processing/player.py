# Databricks notebook source
# MAGIC %run ../../Config/batch_configs/batch_configs

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

configs = player_configs

# COMMAND ----------

file_location = configs['file_location']
file_type = configs["file_type"]
file_name = configs['file_name']
season = configs['season']
file_path = file_location+"/"+file_name+"/"+season
db = configs['db']

# COMMAND ----------

schema = StructType() \
         .add("Player_Id",IntegerType(),False) \
         .add("Player_Name",StringType(),False) \
         .add("DOB",StringType(),False) \
         .add("Batting_hand",StringType(),False) \
         .add("Bowling_skill",StringType(),False) \
         .add("Country_Name",StringType(),False) \
         .add("Player_team",StringType(),False) \
         .add("Season_year",StringType(),False)

# COMMAND ----------

df_bronze = spark.read.format(file_type) \
      .schema(schema) \
      .load(file_path)

# COMMAND ----------

df_bronze = df_bronze.withColumn('DOB', dateparserfunc(F.col('DOB')))

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_palyer_dataset")

# COMMAND ----------

b_insert_query = f"""INSERT INTO {db}.stg_player TABLE bronze_palyer_dataset"""

# COMMAND ----------

spark.sql(b_insert_query)

# COMMAND ----------

commit_no = get_commit_no(f'{db}.stg_player')

# COMMAND ----------

bronze_player = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table(f'{db}.stg_player')

# COMMAND ----------

bronze_player = column_propcase(bronze_player,['Player_Name'])

# COMMAND ----------

age_checker(bronze_player,'DOB')

# COMMAND ----------

bronze_player = bronze_player.withColumn("effc_start_dt", F.concat_ws("/",F.lit("01"),F.lit("01"),bronze_player.Season_year))
bronze_player = bronze_player.withColumn("effc_end_dt", lit("12/31/9999"))

# COMMAND ----------

bronze_player = bronze_player.withColumn('effc_start_dt', dateparserfunc(F.col('effc_start_dt')))
bronze_player = bronze_player.withColumn('effc_end_dt', dateparserfunc(F.col('effc_end_dt')))

# COMMAND ----------

bronze_player.createOrReplaceTempView("silver_player_dataset")

# COMMAND ----------

merge_query = f"""
MERGE INTO {db}.Player as a
USING 
  (
  SELECT b.player_id as mergekey,b.* FROM silver_player_dataset as b join {db}.Player
  on {db}.Player.player_id = b.player_id
  where _commit_version = {commit_no} and {db}.Player.Player_team <> b.Player_team
  
  Union all
  
  select Null as mergekey, b.*
  from silver_player_dataset as b join {db}.Player
  on {db}.Player.player_id = b.player_id
  where _commit_version = {commit_no} and {db}.Player.Player_team <> b.Player_team )
  as c
ON a.Player_Id = mergekey
WHEN MATCHED and a.player_team <> c.player_team THEN
  UPDATE SET 
  a.effc_end_dt = c.effc_start_dt-1  
WHEN NOT MATCHED
  THEN INSERT (Player_Id, Player_Name, DOB,Batting_hand,Bowling_skill,Country_Name,Player_team,effc_start_dt,effc_end_dt) 
  VALUES (c.Player_Id, c.Player_Name, c.DOB,c.Batting_hand,c.Bowling_skill,c.Country_Name,c.Player_team,c.effc_start_dt,c.effc_end_dt)"""

# COMMAND ----------

spark.sql(merge_query)

# COMMAND ----------


