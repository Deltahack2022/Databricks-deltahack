# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE schema test_db2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE test_dlt_db

# COMMAND ----------

with open('dbfs:/Workspace/Repos/DeltaHack/Databricks-deltahack/Config/ball_by_ball_config.txt') as f:
    contents = f.read()
    print(contents)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM test_db.ball_by_ball
# MAGIC SHOW TABLES FROM test_dlt_db

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_dlt_db.ball_by_ball_gold

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

# MAGIC %fs ls dbfs:/Users/aditya.adkar@accenture.com/streaming_data/

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
