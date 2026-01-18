# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC - Create tables after applying business logic on top of silver tables

# COMMAND ----------

from pyspark.sql.functions import *

# read all tables from silver layer - 2 dimensions and 1 fact table
df_products = spark.table('workspace.ecommerce.silver_products')
df_products = df_products.fillna('UNKOWN')

df_customers = spark.table('workspace.ecommerce.silver_customers')
df_orders = spark.table('workspace.ecommerce.silver_orders')

df_products.printSchema()
df_customers.printSchema()
df_orders.printSchema()


# COMMAND ----------

# DBTITLE 1,Untitled
df_orders = df_orders.withColumn('item_revenue',col('item_revenue').cast('double')).withColumn('shipping_charges',col('shipping_charges').cast('double')).withColumn('total_payment_value',col('total_payment_value').cast('double'))

# COMMAND ----------

df_prod_join = df_products.join(df_orders,df_products.product_id == df_orders.product_id,'inner')
#df_prod_join.printSchema()

gold_category = df_prod_join.groupBy('product_category_name').agg(sum('item_revenue').alias('total_revenue'),sum('shipping_charges').alias('total_shipping_charges'),sum('total_payment_value').alias('total_payment_v'))

gold_category = gold_category.withColumnRenamed('product_category_name','category')
gold_category = gold_category.sort('category')
#gold_category.display()

gold_category.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('workspace.ecommerce.gold_category')

# COMMAND ----------

df_customers_join = df_customers.join(df_orders,df_customers.customer_id == df_orders.customer_id,'inner')
df_customers_join.printSchema()

gold_customer = df_customers_join.groupBy('customer_city').agg(sum('item_revenue').alias('total_revenue'),sum('shipping_charges').alias('total_shipping_charges'),sum('total_payment_value').alias('total_payment'))

gold_customer = gold_customer.withColumnRenamed('customer_city','city')
gold_customer = gold_customer.sort('city')
gold_customer.display()

gold_customer.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('workspace.ecommerce.gold_customer_city')

# COMMAND ----------

df_orders_upd = df_orders.withColumn('order_date',to_date(col('order_ts')))
gold_orders = df_orders_upd.groupBy('order_date').agg(sum('item_revenue').alias('total_revenue'),sum('shipping_charges').alias('total_shipping_charges'),sum('total_payment_value').alias('total_payment'))


gold_orders = gold_orders.sort(col('order_date'))

gold_orders.display()

gold_orders.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('workspace.ecommerce.gold_orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city,round(total_revenue,2) FROM workspace.ecommerce.gold_customer_city
# MAGIC WHERE total_revenue<1000;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Checks

# COMMAND ----------

gold_order_count = gold_orders.count()
gold_customer_count = gold_customer.count()
gold_category_count = gold_category.count()

if gold_order_count == 0 or gold_customer_count == 0 or gold_category_count == 0:
   raise Exception("❌ Data Quality Failed: Gold table is empty")

print("✅ Gold Data Quality Passed")