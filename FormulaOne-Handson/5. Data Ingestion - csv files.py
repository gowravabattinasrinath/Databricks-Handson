# Databricks notebook source
print('hello')

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

df_circuits = spark.read.options(header=True,delimeter=',').schema(circuits_schema).csv('/mnt/gsdevadls1/raw/circuits.csv')

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

df_circuits = df_circuits.drop('url')

# COMMAND ----------

df_circuits = df_circuits.withColumnRenamed('circuitId','circuit_id')\
    .withColumnRenamed('circuitRef','circuit_ref')\
    .withColumnRenamed('lat','latitude')\
    .withColumnRenamed('lng','longitude')\
    .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC write to parquet

# COMMAND ----------

## df_circuits.write.mode('overwrite').parquet('/mnt/gsdevadls1/processed/circuits')

# COMMAND ----------

df_circuits.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet('/mnt/gsdevadls1/processed/circuits'))

# COMMAND ----------

races_schema = StructType([StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

df_races = spark.read.options(header='true', delimiter=',').schema(races_schema).csv('/mnt/gsdevadls1/raw/races.csv')

# COMMAND ----------

df_races = df_races.withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('year', 'race_year')\
    .withColumnRenamed('circuitId', 'circuit_id')\
    .withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('ingestion_date',current_timestamp())\
    .drop('date','time','url')

# COMMAND ----------

display(df_races)

# COMMAND ----------

##df_races.write.mode('overwrite').parquet("/mnt/gsdevadls1/processed/races")

# COMMAND ----------

df_races.write.mode("overwrite").format("parquet").saveAsTable("hive_metastore.f1_processed.races")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/gsdevadls1/processed/races"))

# COMMAND ----------

#df_races.write.mode('overwrite').partitionBy("race_year").parquet("/mnt/gsdevadls1/processed/races")
