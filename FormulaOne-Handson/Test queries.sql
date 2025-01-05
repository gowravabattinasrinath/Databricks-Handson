-- Databricks notebook source
select max(race_id) from hive_metastore.f1_processed.results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark._jsparkSession.catalog())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.tableExists("hive_metastore.f1_processed.results")

-- COMMAND ----------

drop table hive_metastore.f1_processed.results
