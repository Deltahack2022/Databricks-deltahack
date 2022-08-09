# Databricks notebook source
import json


# COMMAND ----------

def read_schema(fullpath,env):
    import json
    # read the formated schema of a table from CSV
    df = spark.read.csv(fullpath,inferSchema=True, header=True)
    
    # Get all the unique tables
    tables = set(df.select("Table").toPandas()["Table"])
    
    
    #generate schema for all tables and store in DBFS
    for table in tables:
        schema = {}
        for row in df.filter(df["Table"]==table).collect():
            if row["Table"] in schema:
                schema[row["Table"]].append({"name":row["column"], "type":row["datatype"], "nullable":row["nullable"]})
            else:
                schema[row["Table"]] = [{"name":row["column"], "type":row["datatype"], "nullable":row["nullable"]}]
    
        with open(f"/dbfs/FileStore/delta_hack/{env}/source/schemas/{table}_schema.json", 'w') as fp:
            json.dump({table:schema}, fp)

# COMMAND ----------


