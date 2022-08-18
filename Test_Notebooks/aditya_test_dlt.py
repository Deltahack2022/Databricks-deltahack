# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import dlt
import json

# COMMAND ----------

source_file_path = "dbfs:/Users/aditya.adkar@accenture.com/"
# f = open("file:/Workspace/Repos/DeltaHack/Databricks-deltahack/Config/ball_by_ball.json")
# ball_schema = json.loads(f.read())
ball_schema = StructType([StructField('MatcH_id', StringType(), True), StructField('Over_id', StringType(), True), StructField('Ball_id', StringType(), True), StructField('Innings_No', StringType(), True), StructField('Team_Batting', StringType(), True), StructField('Team_Bowling', StringType(), True), StructField('Striker_Batting_Position', StringType(), True), StructField('Extra_Type', StringType(), True), StructField('Runs_Scored', StringType(), True), StructField('Extra_runs', StringType(), True), StructField('Wides', StringType(), True), StructField('Legbyes', StringType(), True), StructField('Byes', StringType(), True), StructField('Noballs', StringType(), True), StructField('Penalty', StringType(), True), StructField('Bowler_Extras', StringType(), True), StructField('Out_type', StringType(), True), StructField('Caught', StringType(), True), StructField('Bowled', StringType(), True), StructField('Run_out', StringType(), True), StructField('LBW', StringType(), True), StructField('Retired_hurt', StringType(), True), StructField('Stumped', StringType(), True), StructField('caught_and_bowled', StringType(), True), StructField('hit_wicket', StringType(), True), StructField('ObstructingFeild', StringType(), True), StructField('Bowler_Wicket', StringType(), True), StructField('Match_Date', StringType(), True), StructField('Season', StringType(), True), StructField('Striker', StringType(), True), StructField('Non_Striker', StringType(), True), StructField('Bowler', StringType(), True), StructField('Player_Out', StringType(), True), StructField('Fielders', StringType(), True), StructField('Striker_match_SK', StringType(), True), StructField('StrikerSK', StringType(), True), StructField('NonStriker_match_SK', StringType(), True), StructField('NONStriker_SK', StringType(), True), StructField('Fielder_match_SK', StringType(), True), StructField('Fielder_SK', StringType(), True), StructField('Bowler_match_SK', StringType(), True), StructField('BOWLER_SK', StringType(), True), StructField('PlayerOut_match_SK', StringType(), True), StructField('BattingTeam_SK', StringType(), True), StructField('BowlingTeam_SK', StringType(), True), StructField('Keeper_Catch', StringType(), True), StructField('Player_out_sk', StringType(), True), StructField('MatchDateSK', StringType(), True)])

# COMMAND ----------

@dlt.table(
    comment = "This is a raw bronze ball_by_ball table",
    table_properties = {"quality": "bronze"},
    schema = ball_schema
)
# @dlt.expect_or_drop("ball id", F.col("Ball_id") )
def ball_by_ball_bronze():
    return(
        spark.readStream
            .format("cloudFiles")
            .option("cloudfiles.format", "csv")
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

# @dlt.table(
#     comment = "This is a cleaned silver ball_by_ball table",
#     table_properties = {"quality": "silver"},
#     partition_cols=["match_id"],
# )
# def ball_by_ball_silver():
#     return(
#         dlt.read_stream("ball_by_ball_bronze").select("match_id", "over_id", "innings_no", "team_batting",
#                                                      "team_bowling", "runs_scored", "out_type", "match_date",
#                                                      "season")
#     )

# COMMAND ----------

# @dlt.table(
#     comment = "This is a cleaned gold ball_by_ball table",
#     table_properties = {"quality": "gold"},
#     partition_cols=["match_id"],
# )
# def ball_by_ball_gold():
#     return (
#         dlt.read("ball_by_ball_silver").alias("s")
#             .join(
#                 spark.read.table("test_db.match").alias("m"),
#                 on="match_id",
#                 how="inner"
#             )
#             .select("s.match_id", "s.over_id", "s.innings_no", "s.team_batting", "s.team_bowling", 
#                     F.concat(F.col("m.Team1"), F.lit(" vs "), F.col("m.Team2")).alias("match"),
#                     "s.runs_scored", "s.out_type", "s.match_date", "s.season", "m.match_winner")
#     )
