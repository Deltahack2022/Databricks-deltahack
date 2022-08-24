# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

file_location = "dbfs:/FileStore/delta_hack/batch_process_data"
file_name = "match_status/all"
file_path = file_location+"/"+file_name
file_type = "csv"

infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
db = 'deltahack_dev'

# COMMAND ----------

df_bronze = spark.read.format(file_type) \
      .options(header=first_row_is_header, delimiter=delimiter,inferSchema=infer_schema) \
      .load(file_path)

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_match_status_dataset")

# COMMAND ----------

b_insert_query = f"""INSERT INTO {db}.stg_match_status TABLE bronze_match_status_dataset"""

# COMMAND ----------

spark.sql(b_insert_query)

# COMMAND ----------

commit_no = get_commit_no('dth_test_db.stg_match_status')

# COMMAND ----------

bronze_match_status = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table('dth_test_db.stg_match_status')

# COMMAND ----------

bronze_match_status = column_lowercase(bronze_match_status, ['Toss_Name','Win_Type','Outcome_Type'])

# COMMAND ----------

bronze_match_status = bronze_match_status.withColumn("Win_Type",F.when(bronze_match_status['Outcome_Type'].isin(['tied']) ,'tie').otherwise(bronze_match_status["Win_Type"]))
bronze_match_status = bronze_match_status.filter(F.col('Outcome_Type') != 'abandoned')
bronze_match_status = bronze_match_status.withColumn("Win_Type",F.when(bronze_match_status['Win_Type'].isin(['run']) ,'runs').otherwise(bronze_match_status["Win_Type"]))

# COMMAND ----------

string_value_check_dq(bronze_match_status,'Toss_Name',['bat','field'])

# COMMAND ----------

string_value_check_dq(bronze_match_status,'Win_Type',['runs','wickets','tie','no result'])

# COMMAND ----------

null_check_dq(bronze_match_status,'match_id')

# COMMAND ----------

bronze_match_status =bronze_match_status.withColumn("ManOfMach", F.col('ManOfMach').cast("integer"))
bronze_match_status =bronze_match_status.withColumn("Win_Margin", F.col('Win_Margin').cast("integer"))

# COMMAND ----------

bronze_match_status.createOrReplaceTempView("silver_match_status_dataset")

# COMMAND ----------

s_insert_query = f"""
INSERT INTO {db}.match_status (match_id ,
Toss_winner ,
match_winner ,
Toss_Name ,
Win_Type ,
Outcome_Type ,
ManOfMach ,
Win_Margin )
SELECT match_id ,
Toss_winner ,
match_winner ,
Toss_Name ,
Win_Type ,
Outcome_Type ,
ManOfMach ,
Win_Margin 
FROM silver_match_status_dataset"""

# COMMAND ----------

spark.sql(s_insert_query)

# COMMAND ----------


