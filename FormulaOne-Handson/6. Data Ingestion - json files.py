# Databricks notebook source
# MAGIC %md
# MAGIC ### constructor file

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

df_constructor = spark.read.schema(constructor_schema).json('/mnt/gsdevadls1/raw/constructors.json')

# COMMAND ----------

df_constructor = df_constructor.withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref')\
    .withColumn('ingestion_date', current_timestamp())\
    .drop('url')

# COMMAND ----------

#display(df_constructor)

# COMMAND ----------

#df_constructor.write.mode('overwrite').parquet('/mnt/gsdevadls1/processed/constructors')

# COMMAND ----------

df_constructor.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drivers file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

name_schema = StructType([StructField('forename', StringType(), True)\
    ,StructField('surname', StringType(), True)])

# COMMAND ----------

drivers_schema = StructType([StructField('driverId', IntegerType(), True)\
    ,StructField('driverRef', StringType(), True)\
    ,StructField('number', IntegerType(), True)\
    ,StructField('code', StringType(), True)\
    ,StructField('name', name_schema)\
    ,StructField('dob', DateType(), True)\
    ,StructField('nationality', StringType(), True)\
    ,StructField('url', StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json('/mnt/gsdevadls1/raw/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                    .drop('url')

# COMMAND ----------

##drivers_df.write.mode("overwrite").parquet("/mnt/gsdevadls1/processed/drivers")

# COMMAND ----------

drivers_df.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results file

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/gsdevadls1/raw/results.json")

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .drop("statusId")

# COMMAND ----------

display(results_df.count())

# COMMAND ----------

#results_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/gsdevadls1/processed/results")

# COMMAND ----------

results_df.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pitstops File

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/gsdevadls1/raw/pit_stops.json")

# COMMAND ----------

pit_stops_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

##pit_stops_df.write.mode("overwrite").parquet("/mnt/gsdevadls1/processed/pit_stops")

# COMMAND ----------

pit_stops_df.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.pit_stops")

# COMMAND ----------

#display(spark.read.parquet("/mnt/gsdevadls1/processed/pit_stops"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lap time

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/gsdevadls1/raw/lap_times/lap*.csv")

# COMMAND ----------

lap_times_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

#lap_times_df.write.mode("overwrite").parquet("/mnt/gsdevadls1/processed/lap_times")

# COMMAND ----------

lap_times_df.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualifying file

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/gsdevadls1/raw/qualifying")

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#qualifying_df.write.mode("overwrite").parquet("/mnt/gsdevadls1/processed/qualifying")

# COMMAND ----------

qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.qualifying")
