# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import dlt
import json

# COMMAND ----------

source_file_path = "dbfs:/Users/aditya.adkar@accenture.com/"
with open('/dbfs/FileStore/DeltaHack/schemas/ball_by_ball_schema.txt') as f:
    ball_schema = f.read()

# COMMAND ----------

@dlt.table(
    comment = "This is a raw bronze ball_by_ball table",
    table_properties = {"quality": "bronze"},
#     schema = ball_schema
)
@dlt.expect_or_drop("valid ball", F.col("Ball_id") <= "6" )
@dlt.expect("valid match", "MatcH_id IS NOT NULL" )
def ball_by_ball_bronze():
    return(
        spark.readStream
            .format("cloudFiles")
            .option("cloudfiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source_file_path}streaming_data/")
            .withColumnRenamed("MatcH_id", "match_id")
            .withColumnRenamed("Over_id", "over_id")
            .withColumnRenamed("Innings_No", "innings_no")
            .withColumnRenamed("Team_Batting", "team_batting")
            .withColumnRenamed("Team_Bowling", "team_bowling")
            .withColumnRenamed("Runs_Scored", "runs_scored")
            .withColumnRenamed("Out_type", "out_type")
            .withColumnRenamed("Match_Date", "match_date")
            .withColumnRenamed("Season", "season")
    )
# dbfs:/Users/aditya.adkar@accenture.com/    
# file:/Workspace/Repos/DeltaHack/Databricks-deltahack/EDA_Data/Streaming/

# COMMAND ----------

@dlt.table(
    comment = "This is a cleaned silver ball_by_ball table",
    table_properties = {"quality": "silver"},
    partition_cols=["match_id"],
)
def ball_by_ball_silver():
    return(
        dlt.read_stream("ball_by_ball_bronze").select("match_id", "over_id", "innings_no", "team_batting",
                                                     "team_bowling", "runs_scored", "out_type", "match_date",
                                                     "season")
    )

# COMMAND ----------

@dlt.table(
    comment = "This is a cleaned gold ball_by_ball table",
    table_properties = {"quality": "gold"},
    partition_cols=["match_id"],
)
def ball_by_ball_gold():
    return (
        dlt.read("ball_by_ball_silver").alias("s")
            .join(
                spark.read.table("test_db.match").alias("m"),
                on="match_id",
                how="inner"
            )
            .select("s.match_id", "s.over_id", "s.innings_no", "s.team_batting", "s.team_bowling", 
                    F.concat(F.col("m.Team1"), F.lit(" vs "), F.col("m.Team2")).alias("match"),
                    "s.runs_scored", "s.out_type", "s.match_date", "s.season", "m.match_winner")
    )
