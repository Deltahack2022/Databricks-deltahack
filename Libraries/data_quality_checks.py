# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import datetime 

# COMMAND ----------

def dq_chk_greater(column_value:list,df):
    
    chk_df = df.withColumn("gt_range", column_value[1] < F.col(column_value[0])).filter(F.col('gt_range') == False).count()
    if chk_df > 0:
        F.raise_error("greater than DQ check failed")
        raise Exception("greater than DQ check failed")

# COMMAND ----------

def dq_chk_lesser(column_value:list,df):
    
    chk_df = df.withColumn("lt_range", column_value[1] > F.col(column_value[0])).filter(F.col('lt_range') == True).count()
    if chk_df > 0:
        F.raise_error("lesser than DQ check failed")
        raise Exception("lesser than DQ check failed")

# COMMAND ----------

def dq_chk_range(column,_value:list,df):
    
    chk_df = df.withColumn("_range", ((_value[1] > F.col(column)) & (_value[0] < F.col(column)))).filter(F.col('_range') == False).count()
    if chk_df > 0:
        F.raise_error("range DQ check failed")
        raise Exception("range DQ check failed")

# COMMAND ----------

def dq_date_range(df, date_col, date_range:list):
    
    chk_df = df.withColumn("_range", ((date_range[1] > F.col(date_col).cast('timestamp')) & (date_range[0] < F.col(date_col).cast('timestamp')))).filter(F.col('_range') == False).count()
    if chk_df > 0:
        F.raise_error("date range DQ check failed")
        raise Exception("date range DQ check failed")

# COMMAND ----------

def null_check_dq(df, column):
    df_check = df.withColumn("Success",F.when(df[column].isNull() ,True).otherwise(False))
    try:
        assert not df_check.select(F.expr('any(Success == True)')).collect()[0][0], f"Uh oh! Mandatory column '{column}' is having null values: Failed"
        print(f"All Items in the column '{column}' are not null: Passed")
    except AssertionError as e:
        raise e

# COMMAND ----------

def column_lowercase(df,columns:list):
    for column in  columns:
        df = df.withColumn(column, F.lower(col(column)))
        
    return df

# COMMAND ----------

def string_value_check_dq(df, column, values:list):
    df_check = df.withColumn("Success",F.when(df[column].isin(values) ,True).otherwise(False))
    try:
        assert not df_check.select(F.expr('any(Success == False)')).collect()[0][0], f"Uh oh! Values in the Mandatory column :'{column}' are not accepted, Please refer values '{values}' Status: Failed"
        print(f"All Items in the column '{column}' are valid Status: Passed")
    except AssertionError as e:
        raise e

# COMMAND ----------

def get_commit_no(table):
    latestcommit = spark.sql(f"select max(_commit_version) from table_changes('{table}',1)")
    return latestcommit.collect()[0][0]

# COMMAND ----------

def gen_name_shortcut(name):
    sname = ""
    for let in name.split(' '):
        sname += let[0]
    return sname

udf_sname_gen = F.udf(lambda name:gen_name_shortcut(name), StringType())

# COMMAND ----------

dateparserfunc =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

# COMMAND ----------

def column_propcase(df,columns:list):
    for column in  columns:
        df = df.withColumn(column, F.initcap(F.col(column)))
        
    return df


# COMMAND ----------

def age_checker(df,column):
    agedf = df.withColumn('age', (F.months_between(current_date(), F.col(column)) / 12).cast('int'))
    agedf = agedf.withColumn("Success",F.when(agedf['age']>=16,True).otherwise(False))
    
    try:
        assert agedf.select(F.expr('any(Success == True)')), f"Uh oh! players age is not above 16 years. He cannot registered : Failed"
        print(f"Players can be sucessfully registered: Passed")
    except AssertionError as e:
        raise e
    

# COMMAND ----------

def cast_type(df, columns_data_map:dict):
    for col, dtype in columns_data_map.items():
        df = df.withColumn(col, F.col(col).cast(dtype))
    
    return df

# COMMAND ----------


