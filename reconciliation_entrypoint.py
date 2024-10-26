# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import logging
logging.getLogger("databricks.labs.remorph").setLevel(logging.INFO)

# COMMAND ----------

dbutils.widgets.text("table_name","scbucudpdev.dldev_persist_st_db.st_fc_strtem01_1_0")
dbutils.widgets.text("layer_name","ingestion")
dbutils.widgets.text("sqlServer","scbudpseaasq001dev")
dbutils.widgets.text("sqlDatabase","scbudpseaasqdb001dev")
dbutils.widgets.text("sqlUser","udpdip0004@scbcorp.onmicrosoft.com")
dbutils.widgets.text("sqlPassword","UDPDIP@scb2024#")
dbutils.widgets.text("metastore_catalog",'scbucudpdev')
dbutils.widgets.text("metastore_schema",'reconcile')
dbutils.widgets.text("metastore_volume","reconcile_volume")

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
layer_name = dbutils.widgets.get("layer_name")
sqlServer = dbutils.widgets.get("sqlServer")
sqlDatabase = dbutils.widgets.get("sqlDatabase")
sqlUser = dbutils.widgets.get("sqlUser")
sqlPassword = dbutils.widgets.get("sqlPassword")
metastore_catalog = dbutils.widgets.get("metastore_catalog")
metastore_schema = dbutils.widgets.get("metastore_schema")
metastore_volume = dbutils.widgets.get("metastore_volume")

# COMMAND ----------

scope_name="scbudpseaakv001dev"
storage_account="scbdipseasta003stddev"

# COMMAND ----------

def get_scope_key_for_storage_name(scope_name,storage_name):
    """
    Gets the scope key based on the scope name and storage name extracted from the location.
    Input:
        - scope_name: The scope name
        - storage_name : The storage name extracted from the location path.
    Output:
        - scope_key : The scope key which contains the sas key.
    """
    return [v.key for v in dbutils.secrets.list(scope_name) if storage_name in v.key][0]

# COMMAND ----------

scope_key= get_scope_key_for_storage_name(scope_name,storage_account)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", dbutils.secrets.get(scope=scope_name, key=scope_key))

# COMMAND ----------

from src.databricks.labs.remorph.reconcile.scb_reconcile import SCB_Reconcile
reconcile_process = SCB_Reconcile(table=table_name,
                                  layer=layer_name,
                                  sqlServer=sqlServer,
                                  sqlDatabase=sqlDatabase,
                                  sqlUser=sqlUser,
                                  sqlPassword=sqlPassword,
                                  metadata_catalog = metastore_catalog,
                                  metadata_schema = metastore_schema,
                                  metadata_volume = metastore_volume,
                                  spark=spark)
data_reconcile_required,agg_recon_id,failed_recon_cols = reconcile_process.execute_aggregate_recons(
    group_by_columns = [],
    exclusion_columns = [],
    report_type = "row")
if data_reconcile_required:
    reconcile_process.execute_data_recon()
# /Workspace/Users/ajai.sivadas@databricks.com/remorph_scb/src/databricks/labs/remorph/__init__.py

# COMMAND ----------