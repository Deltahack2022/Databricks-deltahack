# Databricks notebook source
# MAGIC %run ../../Config/batch_configs/batch_configs

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

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

try:
    df_bronze = spark.read.format(file_type) \
      .options(header=first_row_is_header, delimiter=delimiter,inferSchema=infer_schema) \
      .load(file_paths)
except Exception as e:
    print("Error: file not found")
    excp = f" {e}"
    audit_entry(db,"team",excp,"unable to read the file, Error: file not found error")

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_team_dataset")

# COMMAND ----------

B_insert_query = f"INSERT INTO {db}.stg_team TABLE bronze_team_dataset"

try:
    spark.sql(B_insert_query)
except Exception as e:
    print("Error: unable to insert into stage table")
    excp = f" {e}"
    audit_entry(db,"team",excp,"unable to load table, Error: table not found")

# COMMAND ----------

commit_no = get_commit_no(f'{db}.stg_team')

# COMMAND ----------

try:
    bronze_team = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table(f'{db}.stg_team') \
                  .where(col("_change_type") != "preimage")
except Exception as e:
    print("Error: stage table not found")
    excp = f" {e}"
    audit_entry(db,"team",excp,"unable to find table, Error: table not found")

# COMMAND ----------

null_check_dq(bronze_team, "Team_Id")

# COMMAND ----------

bronze_team = column_lowercase(bronze_team,['Team_Name'])

# COMMAND ----------

try:
    bronze_team = bronze_team.withColumn("sname", F.upper(udf_sname_gen(col('Team_Name'))))
except Exception as e:
    print("Error: unable to process short name")
    excp = f" {e}"
    audit_entry(db,"team",excp,"unable to process, Error: column not found")

# COMMAND ----------

bronze_team.createOrReplaceTempView("silver_team_dataset")

# COMMAND ----------

merge_query = f"""MERGE INTO {db}.Team as a
USING 
  (SELECT * FROM silver_team_dataset where _commit_version = {commit_no} )
  as b
ON b.Team_Id = a.Team_Id
WHEN MATCHED THEN
  UPDATE SET a.Team_Name = b.Team_Name , 
  a.Team_Name_Short = b.sname  
WHEN NOT MATCHED
  THEN INSERT (Team_Id, Team_Name, Team_Name_Short) VALUES (b.Team_Id, b.Team_Name, b.sname)"""

# COMMAND ----------

try:
    spark.sql(merge_query)
except Exception as e:
    print("Error: unable to marge into table")
    excp = f" {e}"
    audit_entry(db,"team",excp,"unable to load table, Error: table not found")

# COMMAND ----------


