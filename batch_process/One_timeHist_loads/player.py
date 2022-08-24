# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

season = 'all'
file_location = "dbfs:/FileStore/delta_hack/batch_process_data"
file_name = f"Player/{season}"
file_path = file_location+"/"+file_name
file_type = "json"
db = 'deltahack_dev'

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

commit_no = get_commit_no('dth_test_db.stg_player')

# COMMAND ----------

bronze_player = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table('dth_test_db.stg_player')

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

s_insert_query = f"""INSERT INTO dth_test_db.player (Player_Id, Player_Name, DOB,Batting_hand,Bowling_skill,Country_Name,Player_team,effc_start_dt,effc_end_dt)
SELECT Player_Id, Player_Name, DOB,Batting_hand,Bowling_skill,Country_Name,Player_team,effc_start_dt,effc_end_dt
FROM silver_player_dataset where _commit_version = {commit_no}"""

# COMMAND ----------

spark.sql(s_insert_query)

# COMMAND ----------


