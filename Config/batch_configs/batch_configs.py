# Databricks notebook source
env = dbutils.widgets.get('ENV')

# COMMAND ----------

team_configs = {'file_location' : "dbfs:/FileStore/delta_hack/batch_process_data",
'file_type' : "csv",
'file_name' : "Team_update.csv",
'infer_schema' : "false",
'first_row_is_header' : "true",
'delimiter' : ",",
'db': 'deltahack_dev' if env == 'dev' else 'deltahack_prod'}

# COMMAND ----------

player_match_configs = {'season' : '2017',
  'file_location' : "dbfs:/FileStore/delta_hack/batch_process_data",
'file_name' : "Player_match",
'file_type' : "csv",
'infer_schema' : "true",
'first_row_is_header' : "true",
'delimiter' : ",",
'db' : 'deltahack_dev' if env == 'dev' else 'deltahack_prod'}

# COMMAND ----------

player_configs = {'season' : '2017',
'file_location' : "dbfs:/FileStore/delta_hack/batch_process_data",
'file_name' : "Player",
'file_type' : "json",
'db' : 'deltahack_dev' if env == 'dev' else 'deltahack_prod'}

# COMMAND ----------

match_status_configs = {'file_location' : "dbfs:/FileStore/delta_hack/batch_process_data",
                        'season' : '2017',
'file_name' : "match_status",
'file_type' : "csv",
'infer_schema' : "false",
'first_row_is_header' : "true",
'delimiter' : ",",
'db' : 'deltahack_dev' if env == 'dev' else 'deltahack_prod'}

# COMMAND ----------

match_schedule_configs = {'season' : '2017',
'file_location' : "dbfs:/FileStore/delta_hack/batch_process_data",
'file_name' : "match",
'file_type' : "parquet",
'db' : 'deltahack_dev' if env == 'dev' else 'deltahack_prod'}

# COMMAND ----------


