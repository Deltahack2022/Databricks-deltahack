# Databricks notebook source
# MAGIC %run ../../Config/batch_configs/onetime_hist_configs

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

configs = match_schedule_configs

# COMMAND ----------

file_location = configs['file_location']
file_type = configs["file_type"]
file_name = configs['file_name']
season = configs['season']
file_path = file_location+"/"+file_name+"/"+season
db = configs['db']

# COMMAND ----------

df_bronze = spark.read.format(file_type) \
      .load(file_path)

# COMMAND ----------

df_bronze = df_bronze.withColumn('match_date', dateparserfunc(F.col('match_date')))

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_match_sch_dataset")

# COMMAND ----------

b_insert_query = f"""INSERT INTO {db}.stg_match_schedule TABLE bronze_match_sch_dataset"""

# COMMAND ----------

spark.sql(b_insert_query)

# COMMAND ----------

commit_no = get_commit_no(f'{db}.stg_match_schedule')

# COMMAND ----------

bronze_match_sch = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table(f'{db}.stg_match_schedule')

# COMMAND ----------

bronze_match_sch.createOrReplaceTempView("silver_match_sch_dataset")

# COMMAND ----------

s_inser_query = f"""
INSERT INTO {db}.match_schedule (match_id,
Team1 ,
Team2 ,
match_date ,
Season_Year ,
Venue_Name ,
City_Name ,
Country_Name)
SELECT match_id,
Team1 ,
Team2 ,
match_date ,
Season_Year ,
Venue_Name ,
City_Name ,
Country_Name
FROM silver_match_sch_dataset where _commit_version = {commit_no} """

# COMMAND ----------

spark.sql(s_inser_query)

# COMMAND ----------


