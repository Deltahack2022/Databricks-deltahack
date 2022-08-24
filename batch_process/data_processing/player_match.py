# Databricks notebook source
# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

season = 2017
file_location = "dbfs:/FileStore/delta_hack/batch_process_data"
file_name = f"Player_match/{season}"
file_path = file_location+"/"+file_name
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
db = 'deltahack_dev'

# COMMAND ----------

schema = StructType() \
         .add("Match_Id",IntegerType(),False) \
         .add("Player_Id",IntegerType(),False) \
         .add("Role_Desc",StringType(),False) \
         .add("Opposit_Team",StringType(),False) \
         .add("Season_year",StringType(),False) \
         .add("is_manofThematch",BooleanType(),False) \
         .add("IsPlayers_Team_won",BooleanType(),False) \
         .add("Batting_Status",StringType(),False) \
         .add("Bowling_Status",StringType(),False) \
         .add("Player_Captain",StringType(),False) \
         .add("Opposit_captain",StringType(),False) \
         .add("Player_keeper",StringType(),False) \
         .add("Opposit_keeper",StringType(),False) 

# COMMAND ----------

df_bronze = spark.read.format(file_type) \
      .options(header=first_row_is_header, delimiter=delimiter,inferSchema=infer_schema) \
      .load(file_path)

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_player_match_dataset")

# COMMAND ----------

b_insert_query = f"""INSERT INTO {db}.stg_player_match TABLE bronze_player_match_dataset"""

# COMMAND ----------

spark.sql(b_insert_query)

# COMMAND ----------

commit_no = get_commit_no('dth_test_db.stg_player_match')

# COMMAND ----------

bronze_player_match = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table('dth_test_db.stg_player_match')

# COMMAND ----------

bronze_player_match = bronze_player_match.withColumn("is_manofThematch",F.when(bronze_player_match['is_manofThematch']==1 ,True).otherwise(False))
bronze_player_match = bronze_player_match.withColumn("IsPlayers_Team_won",F.when(bronze_player_match['IsPlayers_Team_won']==1 ,True).otherwise(False))

# COMMAND ----------

null_check_dq(bronze_player_match,'match_id')

# COMMAND ----------

null_check_dq(bronze_player_match,'Player_Id')

# COMMAND ----------

bronze_player_match.createOrReplaceTempView("silver_player_match_dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dth_test_db.player_match (Match_Id ,
# MAGIC Player_Id ,
# MAGIC Role_Desc ,
# MAGIC Opposit_Team ,
# MAGIC Season_year ,
# MAGIC is_manofThematch ,
# MAGIC IsPlayers_Team_won ,
# MAGIC Batting_Status ,
# MAGIC Bowling_Status ,
# MAGIC Player_Captain ,
# MAGIC Opposit_captain ,
# MAGIC Player_keeper ,
# MAGIC Opposit_keeper  )
# MAGIC SELECT Match_Id ,
# MAGIC Player_Id ,
# MAGIC Role_Desc ,
# MAGIC Opposit_Team ,
# MAGIC Season_year ,
# MAGIC is_manofThematch ,
# MAGIC IsPlayers_Team_won ,
# MAGIC Batting_Status ,
# MAGIC Bowling_Status ,
# MAGIC Player_Captain ,
# MAGIC Opposit_captain ,
# MAGIC Player_keeper ,
# MAGIC Opposit_keeper  
# MAGIC FROM silver_player_match_dataset

# COMMAND ----------


