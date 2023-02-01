# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC # read all tables

# COMMAND ----------

Silver_Transactions_df = spark.table('sfsbi_helper.Silver_Transactions')
sfsbi_si_organisation_df = spark.table('shaa_prod_db.sfsbi_si_organisation')
sfsbi_br_dim_card_df = spark.table('shaa_prod_db.sfsbi_br_dim_card')
dnb_br_enterprise_df =  spark.table('shaa_prod_db.dnb_br_enterprise')
dnb_br_cots_analytics_df = spark.table('shaa_prod_db.dnb_br_cots_analytics')
dnb_sic_code_2_digit_df = spark.table('shaa_dev_db.dnb_sic_code_2_digit')
sfsbi_br_dim_vehicle_df = spark.table('shaa_prod_db.sfsbi_br_dim_vehicle')
sfsbi_br_dim_device_details_df = spark.table('shaa_dev_db.sfsbi_br_dim_device_details')
sfsbi_br_fct_sales_item_df=spark.table('shaa_prod_db.sfsbi_br_fct_sales_item')
sfsbi_br_dim_account_df = spark.table('shaa_prod_db.sfsbi_br_dim_account')

# COMMAND ----------

# MAGIC %md
# MAGIC # life span(Months) of customers with other details

# COMMAND ----------

#display(sfsbi_br_dim_account_df)

# COMMAND ----------

cols=['account_customer_number','account_name','line_off_business_description','status_description','nace_division_description','nace_group_description','nace_class_description','marketing_class_description','trading_name','age_months']

life_span_of_cust = sfsbi_br_dim_account_df.withColumn("age_months",datediff(current_date(),col("start_date")) /30).select(*cols)
#display(life_span_of_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC # No. of cards by Status in card table

# COMMAND ----------

def card_status_count(df):
  try:
    total_status = [i['card_status'] for i in df.select('card_status').distinct().collect()]
    #print(total_status)
    #card_status_count_df = spark.createDataFrame([], StructType([StructField('account_customer_number', StringType(), True)]))
    total_cards_df = df.groupby('account_number').agg(countDistinct('source_card_id').alias('total_card_count')).withColumnRenamed('account_number','account_customer_number')
    for status in total_status:
      temp_df = df.filter(col('card_status') == status)\
                  .groupby('account_number')\
                  .agg(countDistinct('source_card_id').alias(status + '_card_count'))\
                  .withColumnRenamed('account_number','account_customer_number')
      #display(temp_df)
      total_cards_df = total_cards_df.join(temp_df,['account_customer_number'],'left').fillna(0)

    return total_cards_df
  except Exception as e:
    print(e)

# COMMAND ----------

card_status_count_df = card_status_count(sfsbi_br_dim_card_df)
#display(card_status_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Real Transaction Profile

# COMMAND ----------

from pyspark.sql import *
import pyspark.sql.functions as func 
from pyspark.sql.functions import lit, lag, col, avg, countDistinct
from pyspark.sql.types import IntegerType, FloatType, DecimalType
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sales_details AS 
# MAGIC   SELECT
# MAGIC 
# MAGIC       le.line_of_business_description AS business,
# MAGIC       le.banding,
# MAGIC       le.status_description,
# MAGIC       si.legal_entity_customer_number,
# MAGIC       si.source_sales_item_id,
# MAGIC       si.source_card_id,
# MAGIC       si.transaction_date_time,
# MAGIC       substring(si.transaction_month,1,4) as Transaction_Year,
# MAGIC       si.global_product_id,
# MAGIC       prd.global_product_description,
# MAGIC       si.customer_invoice_total_net,
# MAGIC       le.CPA_Customer_Number,
# MAGIC       si.customer_retail_value_total_net,
# MAGIC       si.quantity, 
# MAGIC       si.unit_of_measure,
# MAGIC       si.system_invoice_total_net,
# MAGIC       si.h3_site_id,
# MAGIC       si.PAN,
# MAGIC       si.original_transaction_indicator,
# MAGIC       si.original_transaction_item_ID,
# MAGIC       si.euroshell_site_number,
# MAGIC       si.incoming_site_location_description,
# MAGIC       si.incoming_site_number,
# MAGIC       si.billing_delivery_company_number,
# MAGIC       si.invoice_number,
# MAGIC       si.invoice_document_type_description,
# MAGIC       si.transaction_correction_flag,
# MAGIC       si.transaction_type_description,
# MAGIC       si.dispute_indicator,
# MAGIC       si.manual_transaction_indicator,
# MAGIC       le.marketing_class_description,
# MAGIC       prd.fuel_product as Fuel_Product,
# MAGIC       prd.service_product as Service_product,
# MAGIC       prd.fee_product as Fee_product,
# MAGIC       colco.COUNTRY_NAME as Colco_Market
# MAGIC       
# MAGIC   FROM shaa_prod_db.sfsbi_br_fct_sales_item si   
# MAGIC   LEFT JOIN shaa_prod_db.sfsbi_br_dim_legal_entity le ON si.legal_entity_customer_number = le.legal_entity_customer_number
# MAGIC   Left join shaa_prod_db.sfsbi_br_dim_global_product as prd
# MAGIC   on si.global_product_id = prd.global_product_id
# MAGIC   left join shaa_prod_db.sfsbi_br_dim_collecting_company as colco
# MAGIC   on si.colco_code = colco.COLCO_CODE
# MAGIC   where le.marketing_class_description not in ('Leasing','Resellers','Employee')

# COMMAND ----------

# Reading temp view: 'sales_details'
df_transaction_raw = spark.read.table('sales_details')
# dropping duplicates
df_transaction = df_transaction_raw.dropDuplicates()
# Creating country ISO code based on 'legal_entity_customer_number'
df_transaction = df_transaction.withColumn('country', func.substring(col('legal_entity_customer_number'),0, 2))

# Creating unique site id
# Unique site ids - to distinguish Shell and TPNs 
df_transaction = df_transaction.withColumn('tpn', func.concat(func.regexp_replace(col('incoming_site_location_description'),"\'",''), col('incoming_site_number'), col('billing_delivery_company_number')))

countries = ['IT', 'NO', 'FI', 'DK', 'SE', 'ES']
df_transaction = df_transaction.withColumn('site_num', col('euroshell_site_number').cast(IntegerType()))
df_transaction = df_transaction.withColumn('Network', func.when((col('site_num') < 998999) & (~(col('country').isin(countries))),'shell').otherwise('tpn'))
df_transaction = df_transaction.withColumn('site_id', func.when(col('Network') == 'shell', col('h3_site_id')).otherwise(col('tpn')))
df_transaction = df_transaction.drop('tpn', 'Network', 'incoming_site_location_description', 'incoming_site_number', 'billing_delivery_company_number')

# Creating variable with customer info (legal ent or CPA)
df_transaction = df_transaction.withColumn('customer_number', 
                               func.when(col('CPA_Customer_Number').isNull(), col('legal_entity_customer_number')).otherwise(col('CPA_Customer_Number')))

# COMMAND ----------

# CRT and Fleet filter
business_list = ['CRT', 'Fleet']
df_transaction = df_transaction.filter(col('business').isin(business_list))                                               
df_transaction = df_transaction.dropDuplicates()

df_transaction_Y = df_transaction.filter(col('original_transaction_indicator') == 'Y')
corrections = df_transaction.filter(col('original_transaction_indicator') == 'N')
corrections_agg = corrections.groupBy('original_transaction_item_ID').agg(func.sum('quantity').alias('corr_quantity'))
corrections_agg = corrections_agg.withColumnRenamed('original_transaction_item_ID', 'corr_id')
df_transaction_new = df_transaction_Y.join(corrections_agg, df_transaction_Y.source_sales_item_id == corrections_agg.corr_id, how = 'left')
df_transaction_new = df_transaction_new.na.fill(value=0,subset=["corr_quantity"])
df_transaction_new = df_transaction_new.withColumn('quantity', df_transaction_new.quantity + df_transaction_new.corr_quantity)
df_transaction_new = df_transaction_new.drop('corr_quantity')
df_transaction_final = df_transaction_new.filter(col('quantity') != 0)
#df_transaction_final.display()

# COMMAND ----------

real_transaction_df = df_transaction_final.withColumnRenamed('customer_number','account_customer_number')

# COMMAND ----------


