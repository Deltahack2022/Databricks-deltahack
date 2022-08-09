# Databricks notebook source
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


