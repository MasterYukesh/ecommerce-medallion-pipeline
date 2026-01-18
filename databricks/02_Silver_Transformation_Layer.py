# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformations
# MAGIC - Data Cleansing 
# MAGIC - Building of Fact and Dinemsion Tables**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Building Silver Customer Dimension Table


df_bronze_customers = spark.table('workspace.ecommerce.bronze_customers')

df_bronze_customers_clean = df_bronze_customers.filter(col("customer_id").isNotNull()).withColumn('customer_city',upper(trim(col("customer_city")))).withColumn('customer_state',upper(trim(col("customer_state"))))

df_bronze_customers_dedup = df_bronze_customers_clean.dropDuplicates(['customer_id']).withColumn('start_date',current_date()).withColumn('end_date',lit(None)).withColumn('is_current',lit(True))

df_bronze_customers_dedup = df_bronze_customers_dedup.withColumn('end_date',col('end_date').cast('date'))
df_bronze_customers_dedup.display()


# COMMAND ----------

# DBTITLE 1,Untitled
df_silver_customers = df_bronze_customers_dedup.select("customer_id","customer_city","customer_state","start_date","end_date","is_current")

from delta.tables import DeltaTable

table_name = 'workspace.ecommerce.silver_customers'
table_exists = spark.catalog.tableExists(table_name)

if not table_exists:
    df_silver_customers.write.mode("overwrite").option('overwriteSchema','true').saveAsTable("workspace.ecommerce.silver_customers")
else:
    target = DeltaTable.forName(spark,table_name)
    (
        target.alias("t")
        .merge(df_silver_customers.alias("s")
        , "t.customer_id = s.customer_id AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition = " t.customer_city <> s.customer_city OR t.customer_state <> s.customer_state ",
            set = {
                "end_date" : current_date(),
                "is_current" : lit(False),
            }
        )
        #.whenNotMatchedInsertAll() - this can be used but might insert wrong start_date,end_date,is_current values
        .whenNotMatchedInsert(
            values = {
                'customer_id' : col('s.customer_id'),
                'customer_city' : col('s.customer_city'),
                'customer_state' : col('s.customer_state'),
                'start_date' : current_date(),
                'end_date': expr('CAST(NULL AS DATE)'),
                'is_current' : lit(True)
            }
        ).execute()
    )
        


# COMMAND ----------

# MAGIC %sql
# MAGIC /*DROP TABLE workspace.ecommerce.silver_customers;*/

# COMMAND ----------

# Building Silver Products Dimension Table
df_bronze_products = spark.table('workspace.ecommerce.bronze_products')

df_bronze_products_clean = df_bronze_products.select("product_id","product_category_name")
df_bronze_products_clean = df_bronze_products_clean.filter(col("product_id").isNotNull()).withColumn("product_category_name",upper(trim(col("product_category_name"))))

df_silver_products = df_bronze_products_clean.dropDuplicates(['product_id'])

df_silver_products.write.mode("overwrite").option('overwriteSchema','true').saveAsTable('workspace.ecommerce.silver_products')


# COMMAND ----------

# Building Silver Order Fact Table

# Read all bronze tables

df_order_items = spark.table('workspace.ecommerce.bronze_order_items')

df_orders = spark.table('workspace.ecommerce.bronze_orders')

df_payments = spark.table('workspace.ecommerce.bronze_payments')

df_products = spark.table('workspace.ecommerce.silver_products')

df_customers = spark.table('workspace.ecommerce.silver_customers')



# COMMAND ----------

# Join all tables to create the fact table

df_order_items = df_order_items.select("order_id","product_id","price","shipping_charges")
df_order_items_upd = df_order_items.withColumnRenamed("order_id","order_id_items").withColumnRenamed("product_id","product_id_items")

df_orders = df_orders.select("order_id","customer_id","order_purchase_timestamp")
df_orders_upd = df_orders.withColumnRenamed("customer_id","customer_id_orders")
df_payments = df_payments.select("order_id","payment_value","payment_type")



fact_base = df_orders_upd.join(df_order_items_upd,df_orders_upd.order_id == df_order_items_upd.order_id_items,"inner")

fact_customers = fact_base.join(df_silver_customers,fact_base.customer_id_orders == df_silver_customers.customer_id,"left")
fact_products = fact_customers.join(df_products,fact_customers.product_id_items == df_products.product_id,"left")


df_payments_agg = df_payments.groupBy("order_id").agg(sum("payment_value").alias("total_payment_value")).withColumnRenamed("order_id","order_id_agg")


fact_payments = fact_products.join(df_payments_agg,fact_products.order_id == df_payments_agg.order_id_agg,"left")





# COMMAND ----------

# select only the required columns

fact_orders = fact_payments.select("order_id","customer_id","product_id","price","shipping_charges","total_payment_value","order_purchase_timestamp")

fact_orders = fact_orders.withColumnRenamed("price","item_revenue").withColumnRenamed("order_purchase_timestamp","order_ts")

fact_orders.display()

# COMMAND ----------

# LOGIC TO MAKE SURE IF FIRST RUN -> THEN TABLE FULL LOAD, ELSE MERGE 

table_name = 'workspace.ecommerce.silver_orders'
table_exists = spark.catalog.tableExists(table_name)
is_empty = False

if table_exists:
    # check if it has data
    count = spark.table(table_name).limit(1).count()
    is_empty = (count == 0)

from delta.tables import DeltaTable

if not table_exists or is_empty: # if table does not exist or exists and does not have any data
    # run full load
    fact_orders.write.mode("overwrite").option('overwriteSchema','true').saveAsTable("workspace.ecommerce.silver_orders")
else:
    # MERGE INTO
    
    # get the delta table
    target = DeltaTable.forName(spark, "workspace.ecommerce.silver_orders")

    (
        target.alias("t")
        .merge(
            fact_orders.alias("s"),
            "t.order_id = s.order_id AND t.product_id = s.product_id" 
            )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    # the above merge will update the existing records and insert new records present in the delta table
     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.ecommerce.silver_orders VERSION AS OF 4;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Checks

# COMMAND ----------

#record count check
df_silver_count = df_silver_orders.count()
if df_silver_count  == 0:
  raise Exception("❌ Data Quality Failed: Silver table has 0 records")

# check for nulls in primary key

pk_nulls_count = df_silver_orders.filter(col("order_id").isNull()).count()

if pk_nulls_count >0:
    raise Exception(f"❌ Data Quality Failed: {null_count} null customer_id found")

# Duplicate Records check (Primary Key)

silver_dup_rec_count = df_silver_orders.groupBy("order_id").count().filter("count > 1").count()

if silver_dup_rec_count > 0:
    raise Exception(f"❌ Data Quality Failed: {silver_dup_rec_count} duplicate records found")

print("✅ Data Quality Checks Passed for Silver layer")


# COMMAND ----------

