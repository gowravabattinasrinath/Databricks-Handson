# Databricks notebook source
from pyspark.sql.functions import col, sum, count, countDistinct, min, max, avg, row_number, rank, dense_rank, window, lit

# COMMAND ----------

# MAGIC %run ./Utilities/config_paths

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

filter_df = race_results_df.filter(race_results_df.race_year == 2020)

# COMMAND ----------

display(filter_df.select(countDistinct(filter_df.race_name).alias('cnt')))

# COMMAND ----------

filter_df.select(sum('points').alias('total')).show()

# COMMAND ----------

filter_df.filter(filter_df.driver_name == 'Sebastian Vettel').show()

# COMMAND ----------

filter_df.groupBy('driver_name').sum('points').show()

# COMMAND ----------

filter_df.groupBy('driver_name').agg(sum('points'),countDistinct('race_name')).show()

# COMMAND ----------

test_df = race_results_df.filter('race_year = 2019 or race_year = 2020')
