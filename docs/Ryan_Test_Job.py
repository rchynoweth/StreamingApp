# Databricks notebook source
# MAGIC %pip install git+https://github.com/rchynoweth/StreamingTemplates.git@main

# COMMAND ----------

import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Custom Python Library - i.e. "template"
from dlt_platform.connectors.kafka_connect import KafkaConnect
from dlt_platform.connectors.file_source_connect import FileSourceConnect
from dlt_platform.connectors.jdbc_connect import JDBCConnect
from dlt_platform.connectors.delta_lake_connect import DeltaLakeConnect
from dlt_platform.transforms.sql_transform import SQLTransform

# COMMAND ----------

v0 = FileSourceConnect() 
@dlt.table(name='orders_stream') 
def orders_stream(): 
	return ( v0.read_file_stream(spark,'/databricks-datasets/retail-org/sales_orders/','json') )

# COMMAND ----------

v1 = FileSourceConnect() 
@dlt.table(name='iot_stream') 
def iot_stream(): 
	return ( v1.read_file_stream(spark,'/databricks-datasets/structured-streaming/events','json') )

# COMMAND ----------

v2 = DeltaLakeConnect() 
@dlt.table(name='silver_iot') 
def silver_iot(): 
	return ( v2.read_stream_delta_table(spark,'live.iot_stream') )

# COMMAND ----------

v3 = SQLTransform() 
@dlt.table(name='gold_iot') 
def gold_iot(): 
	return ( v3.spark_sql(spark,'select action, to_timestamp(time) as datetime, time, date_format(from_unixtime(time), "yyyy-MM-dd HH:00:00") as hourly_datetime from live.silver_iot') )

# COMMAND ----------

v4 = SQLTransform() 
@dlt.table(name='orders_silver') 
def orders_silver(): 
	return ( v4.spark_sql(spark,'select customer_id, sum(number_of_line_items) as number_of_line_items from live.orders_stream group by customer_id') )

# COMMAND ----------


