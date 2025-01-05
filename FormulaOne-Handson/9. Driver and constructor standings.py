# Databricks notebook source
from pyspark.sql.functions import col, sum, count, countDistinct, min, max, avg, row_number, rank, dense_rank, window, lit, when, col

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./Utilities/config_paths

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

agg_df = race_results_df.groupBy('race_year','driver_name','driver_nationality','team').agg(sum('points').alias('total_points'),sum(when(col('position') == 1, 1).otherwise(0)).alias('wins'))

# COMMAND ----------

display(agg_df.filter('race_year = 2020').orderBy(agg_df.total_points.desc()))

# COMMAND ----------

rank_df = agg_df.withColumn('rating',dense_rank().over(Window.partitionBy(agg_df.race_year).orderBy(agg_df.total_points.desc(),agg_df.wins.desc())))

# COMMAND ----------

display(rank_df.filter('race_year = 2020').orderBy(rank_df.rating))

# COMMAND ----------

rank_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/driver_standings')

# COMMAND ----------

# MAGIC %md
# MAGIC ### constructor standings

# COMMAND ----------

cons_agg_df = race_results_df.groupBy('team','race_year').agg(sum('points').alias('total_points'),sum(when(col('position') == 1, 1).otherwise(0)).alias('wins'))

# COMMAND ----------

display(cons_agg_df.filter('race_year = 2020').orderBy('race_year','total_points','wins'))

# COMMAND ----------

cons_rank_df = cons_agg_df.withColumn('rank',row_number().over(Window.partitionBy(cons_agg_df.race_year).orderBy(cons_agg_df.total_points.desc(),cons_agg_df.wins.desc())))

# COMMAND ----------

display(cons_rank_df.filter('race_year = 2020').orderBy('rank'))

# COMMAND ----------

cons_rank_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/constructor_standings')

# COMMAND ----------

cons_rank_df.createOrReplaceGlobalTempView('gv_cons')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_cons

# COMMAND ----------

race_results_df.createOrReplaceTempView('tv_cons')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tv_cons
