# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

season = 2017
file_location = "dbfs:/FileStore/delta_hack/batch_process_data"
file_name = f"match_status/{season}"
file_path = file_location+"/"+file_name
file_type = "csv"

infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
db = 'dth_test_db'

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

columns_data_map = {'ManOfMach': 'integer',
                   'Win_Margin': 'integer'}

# COMMAND ----------

bronze_match_status = cast_type(bronze_match_status,columns_data_map)

# COMMAND ----------

bronze_match_status.createOrReplaceTempView("silver_match_status_dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dth_test_db.match_status (match_id ,
# MAGIC Toss_winner ,
# MAGIC match_winner ,
# MAGIC Toss_Name ,
# MAGIC Win_Type ,
# MAGIC Outcome_Type ,
# MAGIC ManOfMach ,
# MAGIC Win_Margin )
# MAGIC SELECT match_id ,
# MAGIC Toss_winner ,
# MAGIC match_winner ,
# MAGIC Toss_Name ,
# MAGIC Win_Type ,
# MAGIC Outcome_Type ,
# MAGIC ManOfMach ,
# MAGIC Win_Margin 
# MAGIC FROM silver_match_status_dataset

# COMMAND ----------


