# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

file_location = "dbfs:/FileStore/delta_hack/batch_process_data"
file_type = "csv"
file_name = "Team-1.csv"
file_path = file_location+"/"+file_name
# dbfs:/FileStore/delta_hack/batch_process_data/Team-1.csv
# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","
db = 'deltahack_dev'

# COMMAND ----------

df_bronze = spark.read.format(file_type) \
      .options(header=first_row_is_header, delimiter=delimiter,inferSchema=infer_schema) \
      .load(file_path)

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_team_dataset")

# COMMAND ----------

B_insert_query = f"INSERT INTO {db}.stg_team TABLE bronze_team_dataset"
spark.sql(B_insert_query)

# COMMAND ----------

commit_no = get_commit_no('dth_test_db.stg_team')

# COMMAND ----------

bronze_team = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table('dth_test_db.stg_team') \
                  .where(col("_change_type") != "preimage")

# COMMAND ----------

null_check_dq(bronze_team, "Team_Id")

# COMMAND ----------

bronze_team = column_lowercase(bronze_team,['Team_Name'])

# COMMAND ----------

bronze_team = bronze_team.withColumn("sname", F.upper(udf_sname_gen(col('Team_Name'))))

# COMMAND ----------

bronze_team.createOrReplaceTempView("silver_team_dataset")

# COMMAND ----------

s_insert_query = f"""INSERT INTO dth_test_db.Team (Team_Id, Team_Name, Team_Name_Short)
SELECT Team_Id, Team_Name, sname
FROM silver_team_dataset where _commit_version = {commit_no}"""

# COMMAND ----------

spark.sql(s_insert_query)

# COMMAND ----------


