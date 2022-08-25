# Databricks notebook source
# MAGIC %run ../../Config/batch_configs/batch_configs

# COMMAND ----------

# MAGIC %run ../../Libraries/data_quality_checks

# COMMAND ----------

configs = player_match_configs

# COMMAND ----------

file_location = configs['file_location']
file_type = configs["file_type"]
file_name = configs['file_name']
season = configs['season']
file_path = file_location+"/"+file_name+"/"+str(season)
infer_schema = configs['infer_schema']
first_row_is_header = configs['first_row_is_header']
delimiter = configs['delimiter']
db = configs['db']
process = 'player_match'

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

try:
    df_bronze = spark.read.format(file_type) \
      .options(header=first_row_is_header, delimiter=delimiter,inferSchema=infer_schema) \
      .load(file_path)
except Exception as e:
    print("Error: unable to load file")
    excp = f" {e}"
    audit_entry(db,process,excp,"unable to load file, Error: file not found")

# COMMAND ----------

df_bronze.createOrReplaceTempView("bronze_player_match_dataset")

# COMMAND ----------

b_insert_query = f"""INSERT INTO {db}.stg_player_match TABLE bronze_player_match_dataset"""

# COMMAND ----------

try:
    spark.sql(b_insert_query)
except Exception as e:
    print("Error: unable to insert into stage table")
    excp = f" {e}"
    audit_entry(db,process,excp,"unable to load table, Error: table not found")

# COMMAND ----------

commit_no = get_commit_no(f'{db}.stg_player_match')

# COMMAND ----------

try:
    bronze_player_match = spark.read.format("delta") \
                  .option("readChangeFeed", "true") \
                  .option("startingVersion", commit_no) \
                  .table(f'{db}.stg_player_match')
except Exception as e:
    print("Error: stage table not found")
    excp = f" {e}"
    audit_entry(db,process,excp,"unable to find table, Error: table not found")

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

s_merge_query = F"""
merge INTO {db}.player_match as a 
using (select * from silver_player_match_dataset) as b 
on b.match_id = a.match_id and a.player_id = b.player_id
when matched then
    update set 
a.Role_Desc = b.Role_Desc,
a.Opposit_Team = b.Opposit_Team,
a.Season_year = b.Season_year,
a.is_manofThematch = b.is_manofThematch,
a.IsPlayers_Team_won = b.IsPlayers_Team_won ,
a.Batting_Status = b.Batting_Status,
a.Bowling_Status = b.Bowling_Status,
a.Player_Captain = b.Player_Captain,
a.Opposit_captain = b.Opposit_captain,
a.Player_keeper = b.Player_keeper,
a.Opposit_keeper = b.Opposit_keeper
WHEN NOT MATCHED 
    Then Insert (Match_Id ,
Player_Id ,
Role_Desc ,
Opposit_Team ,
Season_year ,
is_manofThematch ,
IsPlayers_Team_won ,
Batting_Status ,
Bowling_Status ,
Player_Captain ,
Opposit_captain ,
Player_keeper ,
Opposit_keeper  )
VALUES (b.Match_Id ,
b.Player_Id ,
b.Role_Desc ,
b.Opposit_Team ,
b.Season_year ,
b.is_manofThematch ,
b.IsPlayers_Team_won ,
b.Batting_Status ,
b.Bowling_Status ,
b.Player_Captain ,
b.Opposit_captain ,
b.Player_keeper ,
b.Opposit_keeper  )"""


# COMMAND ----------

try:
    spark.sql(s_merge_query)
except Exception as e:
    print("Error: unable to insert into table")
    excp = f" {e}"
    audit_entry(db,process,excp,"unable to load table, Error: table not found")

# COMMAND ----------


