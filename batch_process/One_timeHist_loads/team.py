# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

# MAGIC %run ../../Config/batch_configs/onetime_hist_configs

# COMMAND ----------

configs = team_configs

# COMMAND ----------

file_location = configs['file_location']
file_type = configs["file_type"]
file_name = configs['file_name']
file_path = file_location+"/"+file_name
infer_schema = configs['infer_schema']
first_row_is_header = configs['first_row_is_header']
delimiter = configs['delimiter']
db = configs['db']

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

commit_no = get_commit_no(f'{db}.stg_team')

# COMMAND ----------

bronze_team = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table(f'{db}.stg_team') \
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

s_insert_query = f"""INSERT INTO {db}.Team (Team_Id, Team_Name, Team_Name_Short)
SELECT Team_Id, Team_Name, sname
FROM silver_team_dataset"""

# COMMAND ----------

spark.sql(s_insert_query)

# COMMAND ----------


