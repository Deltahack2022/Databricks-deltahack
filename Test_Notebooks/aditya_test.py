# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE deltahack_prod;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM deltahack_dev

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deltahack_dev.stg_match_status

# COMMAND ----------

with open('../Config/ball_by_ball_config.txt') as f:
    contents = f.read()
    with open('dbfs:/Users/aditya.adkar@accenture.com/ball_by_ball_config.txt', 'w') as f2:
        f2.write(contents)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED test_dlt_db.ball_by_ball_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM test_db.ball_by_ball
# MAGIC SHOW TABLES FROM test_dlt_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_dlt_db.ball_by_ball_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_db.team

# COMMAND ----------

file_location = "/Workspace/Repos/DeltaHack/Databricks-deltahack/EDA_Data"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(f"file:{file_location}/Ball_By_Ball.csv")

display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

df.write.option("header",True).format("csv").mode('overwrite').save("/Users/aditya.adkar@accenture.com/streaming_data/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/pipelines/a8338f14-a1c4-41f1-acb0-9918b10fd022/system/events/

# COMMAND ----------

df_audit = spark.read.format("delta").load("dbfs:/pipelines/fd4f5d2d-a5fc-473d-818a-715b55289206/system/events/")
display(df_audit)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE deltahack_dev.audit_table
# MAGIC USING DELTA LOCATION 'dbfs:/pipelines/fd4f5d2d-a5fc-473d-818a-715b55289206/system/events/'

# COMMAND ----------

df.write.format("delta").saveAsTable("test_db.team")

# COMMAND ----------

import sys
print("\n".join(sys.path))

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES FROM test_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_db.match

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED test_dlt_db.ball_by_ball_gold
