# Databricks notebook source
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
print(f"*****Succesfully Aggregation Reconciliation Completed with Id: {agg_recon_id}********")
if data_reconcile_required:
    print(f"""*****Aggregation Reconciliation failed, Id: {agg_recon_id.split(',')[0]}, additional Row Reconciliation performed with the Id: {agg_recon_id.split(',')[1]},
     for columns {failed_recon_cols}, please check the metadata table for details********""")

    data_reconcile_passed,data_recon_id,_ = reconcile_process.execute_data_recon(recon_columns=failed_recon_cols)
    if not data_reconcile_passed:
        print(f"""*****Data Reconciliation failed, Id: {data_recon_id}, please check the metadata table for details********""")

# COMMAND ----------


