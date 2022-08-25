# Databricks notebook source
# MAGIC %run ../../Config/batch_configs/batch_configs

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

s_merge_query = f"""
Merge INTO {db}.match_schedule as a 
using (select * from silver_match_sch_dataset) as b 
on a.match_id= b.match_id
when matched then 
update set a.Team1 = b.Team1,
a.Team2 = b.Team2,
a.match_date = b.match_date,
a.Season_Year = b.Season_Year,
a.Venue_Name = b.Venue_Name,
a.City_Name = b.City_Name,
a.Country_Name = b.Country_Name
when not matched then 
insert (match_id,
Team1 ,
Team2 ,
match_date ,
Season_Year ,
Venue_Name ,
City_Name ,
Country_Name)
values ( b.match_id,
b.Team1 ,
b.Team2 ,
b.match_date ,
b.Season_Year ,
b.Venue_Name ,
b.City_Name ,
b.Country_Name)"""

# COMMAND ----------

spark.sql(s_merge_query)

# COMMAND ----------


