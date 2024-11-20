# Databricks notebook source
# MAGIC %md
# MAGIC # SCB Reconcile
# MAGIC ## Input Parameters:
# MAGIC - **environment**: _Environment Value, supported **dldev**, **dlsit**, **dlprod**_
# MAGIC - **source_table_name**: Source Table for the Reconciliation, should be in the format **schema.catalog.table**_
# MAGIC - **target_table_name**: Target Table for the Reconciliation, should be in the format **schema.catalog.table**_
# MAGIC - **layer_name**: Layer for the Reconciliation, supported values: **ingestion**, **transformation**, **olap**, **outbound**_
# MAGIC - **additional_exclusion_columns**: Additional Columns to be excluded for Reconciliation requires a comma separated list of columns_
# MAGIC - **additional_key_columns**: Additional Columns to be used as Key for Data Reconciliation requires a comma separated list of columns_
# MAGIC - **data_comparison_filter**: Data Comparison filter to be provided in the format of sql string, for e.g. dl_data_dt='2022-11-29'_
# MAGIC
# MAGIC ## Reports:
# MAGIC - Executes the Aggregate Recon on any numerical/date/datetime/boolean fields to see if there are any mismatch in the values using appropriate aggregations
# MAGIC - Post execution of the Aggregate Recons, in case of failures on failed fields we execute the Row Comparison to find any mismatch between the values, in case of success we execute the Row Comparison on string fields to find any mismatch between the values
# MAGIC - For failures in either of the above,to get the exact rows we will execute the Data Recon on the tables for the failed columns post selecting the Key Columns from the CtlFw Tables
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("environment","dldev","Environment Identifier")
dbutils.widgets.text("source_table_name","scbucudpdev.dldev_persist_st_db.st_fc_strtem01_1_0","Recon Source Table")
dbutils.widgets.text("target_table_name","hivemetastore.dldev_persist_st_db.st_fc_strtem01_1_0","Recon Target Table")
dbutils.widgets.text("layer_name","ingestion","Layer")
dbutils.widgets.text("additional_exclusion_columns","","Comma Separated Additional Exclusion Columns")
dbutils.widgets.text("additional_key_columns","","Comma Separated Additional Key Columns")
dbutils.widgets.text("data_comparison_filter","","Filter for Data Recon")

# COMMAND ----------

# Capturing User Input
environment = dbutils.widgets.get("environment")
source_table_name = dbutils.widgets.get("source_table_name")
target_table_name = dbutils.widgets.get("target_table_name")
layer_name = dbutils.widgets.get("layer_name")
additional_exclusion_columns = dbutils.widgets.get("additional_exclusion_columns")
additional_exclusion_columns_list = additional_exclusion_columns.split(",")
additional_key_columns = dbutils.widgets.get("additional_key_columns")
additional_key_columns_list = additional_key_columns.split(",")
data_comparison_filter = dbutils.widgets.get("data_comparison_filter")

# COMMAND ----------

aggregate_recon_id = ""
row_recon_id = ""
data_recon_id = ""

# COMMAND ----------
import logging
logging.getLogger().setLevel(logging.WARN)

from src.databricks.labs.remorph.reconcile.scb_reconcile import SCB_Reconcile
reconcile_process = SCB_Reconcile(environment = environment,
                                  source_table=source_table_name,
                                  target_table=target_table_name,
                                  layer=layer_name,
                                  additional_excl_cols_list=additional_exclusion_columns_list,
                                  additional_key_cols_list=additional_key_columns_list,
                                  data_comparison_filter=data_comparison_filter,
                                  spark=spark)
data_reconcile_required,agg_recon_id,failed_recon_cols = reconcile_process.execute_aggregate_recons(report_type = "row")
aggregate_recon_id = agg_recon_id.split(',')[0]
row_recon_id = agg_recon_id.split(',')[1] if len(agg_recon_id.split(",")) > 1 else ""
if data_reconcile_required:
    print(f"""*****Aggregation Reconciliation failed, Id: {agg_recon_id.split(',')[0]}, additional Row Reconciliation performed with the Id: {agg_recon_id.split(',')[1]},
     for columns {failed_recon_cols}, please check the metadata table for details********""")

    data_reconcile_passed,data_recon_id,_ = reconcile_process.execute_data_recon(recon_columns=failed_recon_cols)
    if not data_reconcile_passed:
        print(f"""*****Data Reconciliation failed, Id: {data_recon_id}, please check the metadata table for details********""")
    data_recon_id = data_recon_id
else:
    print(f"*****Succesfully Aggregation Reconciliation Completed with Id: {agg_recon_id.split(',')[0]}, additional Row Reconciliation performed with the Id: {agg_recon_id.split(',')[1] if len(agg_recon_id.split(',')) > 1 else ''}********")

# COMMAND ----------

# MAGIC %md
# MAGIC # AGGREGATE FAILURES

# COMMAND ----------

spark.sql(f"""with recon_main as(Select * from scbucudpdev.reconcile.main where recon_id = '{aggregate_recon_id}'),
recon_agg_rules as (Select * from scbucudpdev.reconcile.aggregate_rules),
recon_agg_details as (Select rm.recon_id
,rm.source_table.`catalog` as source_catalog
,rm.source_table.`schema` as source_schema 
,rm.source_table.table_name as source_table
,rm.target_table.`catalog` as target_catalog
,rm.target_table.`schema` as target_schema
,rm.target_table.table_name as target_table
,rar.rule_info.agg_type as agg_type
,rar.rule_info.agg_column as agg_column
,explode(rad.data)as recon_failures
,rad.recon_type
from scbucudpdev.reconcile.aggregate_details rad 
inner join recon_main rm on rm.recon_table_id = rad.recon_table_id
inner join recon_agg_rules rar on rar.rule_id = rad.rule_id)

Select rad.recon_id
, concat_ws('.',rad.source_catalog,rad.source_schema,rad.source_table) as source_table_name
, concat_ws('.',rad.target_catalog,rad.target_schema,rad.target_table) as target_table_name
,rad.recon_type
,upper(rad.agg_type) || CONCAT('(', rad.agg_column, ')') as rule
,rad.recon_failures[concat_ws(
        '_',
        'source',
        rad.agg_type,
        rad.agg_column
      )] as source_value
,rad.recon_failures[concat_ws(
        '_',
        'target',
        rad.agg_type,
        rad.agg_column
      )] as target_value
from 
recon_agg_details rad
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ROW FAILURES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing in Source

# COMMAND ----------

spark.sql(f"""
with recon_main as(Select * from scbucudpdev.reconcile.main main where main.recon_id = '{row_recon_id}'),
recon_details as (Select * from scbucudpdev.reconcile.details dtl 
inner join recon_main main on main.recon_table_id = dtl.recon_table_id
and dtl.recon_type = "missing_in_source"),
dtls_info as (Select explode(dtl.data) as missing_data from recon_details dtl)
Select missing_data from dtls_info""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing in Target

# COMMAND ----------

spark.sql(f"""
with recon_main as(Select * from scbucudpdev.reconcile.main main where main.recon_id = '{row_recon_id}'),
recon_details as (Select * from scbucudpdev.reconcile.details dtl 
inner join recon_main main on main.recon_table_id = dtl.recon_table_id
and dtl.recon_type = "missing_in_target"),
dtls_info as (Select explode(dtl.data) as missing_data from recon_details dtl)
Select missing_data from dtls_info""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Recon

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing in Source

# COMMAND ----------

spark.sql(f"""
with recon_main as(Select * from scbucudpdev.reconcile.main main where main.recon_id = '{data_recon_id}'),
recon_details as (Select * from scbucudpdev.reconcile.details dtl 
inner join recon_main main on main.recon_table_id = dtl.recon_table_id
and dtl.recon_type = "missing_in_source"),
dtls_info as (Select explode(dtl.data) as missing_data from recon_details dtl)
Select missing_data from dtls_info""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing in Target

# COMMAND ----------

spark.sql(f"""
with recon_main as(Select * from scbucudpdev.reconcile.main main where main.recon_id = '{data_recon_id}'),
recon_details as (Select * from scbucudpdev.reconcile.details dtl 
inner join recon_main main on main.recon_table_id = dtl.recon_table_id
and dtl.recon_type = "missing_in_target"),
dtls_info as (Select explode(dtl.data) as missing_data from recon_details dtl)
Select missing_data from dtls_info""").display()
