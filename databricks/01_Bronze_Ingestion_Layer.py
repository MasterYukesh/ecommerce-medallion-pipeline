# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion 
# MAGIC
# MAGIC - **Source**: Kaggle E-commerce Supply Chain Dataset
# MAGIC
# MAGIC - **Purpose**: Raw structured ingestion with schema enforcement and metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.ecommerce.bronze_vol;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

customers_ddl = StructType([
    StructField('customer_id',StringType(),True),
    StructField('customer_zip_code_prefix',IntegerType(),True),
    StructField('customer_city',StringType(),True),
    StructField('customer_state',StringType(),True) 
    ])

customers_df = spark.read.format('csv').schema(customers_ddl).option("includeMetadata", "true").option('header','true').load('/Volumes/workspace/ecommerce/raw/df_Customers.csv')

customers_df = customers_df.withColumn('ingestion_timestamp',current_timestamp()).withColumn('source_file',col('_metadata.file_path'))
customers_df.display()
customers_df.printSchema()

#customers_df.write.format('delta').mode('overwrite').save('/Volumes/workspace/ecommerce/bronze_vol/customers')

customers_df.write.format('delta').mode('overwrite').saveAsTable('workspace.ecommerce.bronze_customers')

# COMMAND ----------

order_items_ddl = StructType([
     StructField('order_id',StringType(),True),
    StructField('product_id',StringType(),True),
    StructField('seller_id',StringType(),True),
    StructField('price',DoubleType(),True),
    StructField('shipping_charges',DoubleType(),True)  
    ])

order_items_df = spark.read.format('csv').schema(order_items_ddl).option("includeMetadata", "true").option('header','true').load('/Volumes/workspace/ecommerce/raw/df_OrderItems.csv')

order_items_df = order_items_df.withColumn('ingestion_timestamp',current_timestamp()).withColumn('source_file',col('_metadata.file_path'))
order_items_df.display()
order_items_df.printSchema()

# order_items_df.write.format('delta').save('/Volumes/workspace/ecommerce/bronze_vol/order_items')
order_items_df.write.format('delta').mode('overwrite').saveAsTable('workspace.ecommerce.bronze_order_items')

# COMMAND ----------

orders_ddl = StructType([
     StructField('order_id',StringType(),True),
    StructField('customer_id',StringType(),True),
    StructField('order_purchase_timestamp',TimestampType(),True),
    StructField('order_approved_at',TimestampType(),True)
    ])

orders_df = spark.read.format('csv').schema(orders_ddl).option("includeMetadata", "true").option('header','true').load('/Volumes/workspace/ecommerce/raw/df_Orders.csv')

orders_df = orders_df.withColumn('ingestion_timestamp',current_timestamp()).withColumn('source_file',col('_metadata.file_path'))
orders_df.display()
orders_df.printSchema()

# orders_df.write.format('delta').save('/Volumes/workspace/ecommerce/bronze_vol/orders')
orders_df.write.format('delta').mode('overwrite').saveAsTable('workspace.ecommerce.bronze_orders')




# COMMAND ----------

payments_ddl = StructType([
     StructField('order_id',StringType(),True),
    StructField('payment_sequential',IntegerType(),True),
    StructField('payment_type',StringType(),True),
    StructField('payment_installments',IntegerType(),True),
    StructField('payment_value',DoubleType(),True)
    ])

payments_df = spark.read.format('csv').schema(payments_ddl).option("includeMetadata", "true").option('header','true').load('/Volumes/workspace/ecommerce/raw/df_Payments.csv')

payments_df = payments_df.withColumn('ingestion_timestamp',current_timestamp()).withColumn('source_file',col('_metadata.file_path'))
payments_df.display()
payments_df.printSchema()

#payments_df.write.format('delta').save('/Volumes/workspace/ecommerce/bronze_vol/payments')
payments_df.write.format('delta').mode('overwrite').saveAsTable('workspace.ecommerce.bronze_payments')


# COMMAND ----------

products_ddl = StructType([
     StructField('product_id',StringType(),True),
    StructField('product_category_name',StringType(),True),
    StructField('product_weight_g',DoubleType(),True),
    StructField('product_length_cm',DoubleType(),True),
    StructField('product_height_cm',DoubleType(),True),
    StructField('product_width_cm',DoubleType(),True)
    ])

products_df = spark.read.format('csv').schema(products_ddl).option("includeMetadata", "true").option('header','true').load('/Volumes/workspace/ecommerce/raw/df_Products.csv')

products_df = products_df.withColumn('ingestion_timestamp',current_timestamp()).withColumn('source_file',col('_metadata.file_path'))
products_df.display()
products_df.printSchema()

#products_df.write.format('delta').save('/Volumes/workspace/ecommerce/bronze_vol/products')
products_df.write.format('delta').mode('overwrite').saveAsTable('workspace.ecommerce.bronze_products')

# COMMAND ----------

