from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from typing import List

from src.databricks.labs.remorph.config import DatabaseConfig, ReconcileMetadataConfig, ReconcileConfig, TableRecon
from src.databricks.labs.remorph.reconcile.datatype_reconciliation import DataType_Recon
from src.databricks.labs.remorph.reconcile.exception import ReconciliationException
from src.databricks.labs.remorph.reconcile.execute import reconcile_aggregates, recon
from src.databricks.labs.remorph.reconcile.recon_config import Table, ReconcileTableOutput, Filters, ReconcileOutput

import logging
import re
import yaml

from src.databricks.labs.remorph.reconcile.scb_key_cols_derivation import SCB_Key_Cols_Derivation


logger = logging.getLogger(__name__)


def table_name_split(table_name):

    table_name_pattern = r"^([^\.]+)\.([^\.]+)\.([^\.]+)$"
    tbl_nm_matches = re.findall(table_name_pattern,table_name)
    if len(tbl_nm_matches[0]) != 3:
        logger.error("Invalid Table Name, Ensure the table name is in the pattern catalog.schema.table")
        raise Exception("Invalid Table Name, Ensure the table name is in the pattern catalog.schema.table")
    else:
        dbx_catalog = tbl_nm_matches[0][0]
        dbx_schema_table = tbl_nm_matches[0][1]+ "." + tbl_nm_matches[0][2]



    return dbx_catalog,dbx_schema_table


class SCB_Reconcile():

    def __init__(self,
                 environment:str,
                 source_table:str,
                 target_table:str,
                 layer:str,
                 additional_excl_cols_list:List[str],
                 additional_key_cols_list:List[str],
                 data_comparison_filter:str,
                 spark:SparkSession):

        # Setting Received Parameters
        self.source_table = source_table
        self.target_table = target_table
        self.layer = layer
        self.additional_excl_cols_list = additional_excl_cols_list
        self.additional_key_cols_list = additional_key_cols_list
        self.data_comparison_filter = data_comparison_filter

        self.spark = spark

        #Reading and capturing Environment Level Parameters
        with open("./env_config.yaml","r") as config_data:
            self.config = yaml.safe_load(config_data)

        self.sqlServer = self.config["env"][environment]["sql_server"]
        self.sqlDatabase = self.config["env"][environment]["sql_database"]
        self.sqlUserKey = self.config["env"][environment]["sql_username_key"]
        self.sqlPasswordKey = self.config["env"][environment]["sql_password_key"]

        self.secretScope = self.config["env"][environment]["secret_scope"]

        self.dbutils = DBUtils(self.spark)

        self.sqlUser = self.dbutils.secrets.get(scope=self.secretScope, key=self.sqlUserKey)
        self.sqlPassword = self.dbutils.secrets.get(scope=self.secretScope, key=self.sqlPasswordKey)

        self.metadata_catalog = self.config["env"][environment]["metadata_catalog"]
        self.metadata_schema = self.config["env"][environment]["metadata_schema"]
        self.metadata_volume = self.config["env"][environment]["metadata_volume"]

        self.storage_accounts = self.config["env"][environment]["storage_accounts"]


        self.sqlConnectionString = f"""jdbc:sqlserver://{self.sqlServer}.database.windows.net:1433;database={self.sqlDatabase};user={self.sqlUser};password={self.sqlPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"""



        # Getting the Catalog and Schema.Table_Name from the received input
        self.src_catalog, self.src_schema_table = table_name_split(self.source_table)
        self.tgt_catalog, self.tgt_schema_table = table_name_split(self.target_table)

        # Setting up Workspace Client for usage In Recon
        self.wrkspc_client = WorkspaceClient(
            product="reconcile",
            product_version="0.0.1"
        )

        #Setting up Spark Configurations to resolve any ADLS Access Issues while fetching data for reconcile
        self.storage_account_acces(self.storage_accounts)


        #Getting the Key Columns based on configured values and additional provided as part of user input
        self.key_cols_derivations = SCB_Key_Cols_Derivation(
            target_table_name=self.tgt_schema_table,
            connection_string=self.sqlConnectionString,
            additional_key_cols_list=self.additional_key_cols_list,
            spark = self.spark
        )

        self.key_cols = self.key_cols_derivations.get_final_key_cols()

        #Getting the Columns to be excluded from the Reconciliation
        self.system_exclusion_columns = ['udp_row_md5', 'udp_key_md5', 'udp_job_id', 'udp_run_id', 'batch_uuid',
                                         'data_load_ts','dl_data_dt']
        if len(self.additional_excl_cols_list) != 0:
            self.exclusion_cols = list(set(self.additional_excl_cols_list + self.system_exclusion_columns))
        else:
            self.exclusion_cols = self.system_exclusion_columns

    def get_scope_key_for_storage_name(self, storage_name):
        """
        Gets the scope key based on the scope name and storage name extracted from the location.
        Input:
            - scope_name: The scope name
            - storage_name : The storage name extracted from the location path.
        Output:
            - scope_key : The scope key which contains the sas key.
        """
        return [v.key for v in self.dbutils.secrets.list(self.secretScope) if storage_name in v.key][0]

    def storage_account_acces(self,storage_accounts):
        """
            Sets the Spark Configurations to resolve any ADLS Access Issues
            Input:
                - storage_accounts : List of storage accounts to fetch sas access key for from secrets
            Output:
                - None
        """
        for storage_account in storage_accounts:
            scope_key = self.get_scope_key_for_storage_name(storage_account)
            self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
            self.spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net",
                           "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
            self.spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net",
                           self.dbutils.secrets.get(scope=self.secretScope, key=scope_key))



    def get_agg_table_schema(self):
        """
            To get a list of columns to run recon against excluding set framework columns and user provided exclusion
            columns
            Input: None
            Output:
                - Dict of Column and its datatype. For e.g. {"cust_id":"int"}
        """

        schema = self.spark.read.table(self.target_table).schema
        schema_column_data_types = {field.name: field.dataType.simpleString()
                                    for field in schema.fields if field.name not in self.exclusion_cols}
        return schema_column_data_types

    def get_recon_config(self,report_type):
        """
            Returns Reconcile Config based on the Report Type Provided.
            Input:
                - Report Type
            Output:
                - Reconcile Config which has the Database Configuration and Reconcile Metadata Configuration updated
                based on the Inputs provided.
        """

        db_config = DatabaseConfig(
            source_catalog=self.src_catalog,
            target_catalog=self.tgt_catalog,
            target_schema=self.src_schema_table.split(".")[0],
            source_schema=self.tgt_schema_table.split(".")[0]
        )

        metadata_config = ReconcileMetadataConfig(
            catalog=self.metadata_catalog,
            schema=self.metadata_schema,
            volume=self.metadata_volume
        )

        recon_config = ReconcileConfig(
            data_source="databricks",
            report_type=report_type,
            secret_scope=None,
            database_config=db_config,
            metadata_config=metadata_config
        )

        return recon_config

    def data_recon_schema(self,testing_columns:List[str]):
        """
        Returns Schema for table to include only required columns and key columns for Data Recon
        Input:
            - List of Columns to be data reconciled against
        Output:
            - Dict of Columns containing the columns to be tested against and Key Columns

        """
        schema = self.spark.read.table(self.target_table).schema
        inclusion_columns  = []

        if len(self.key_cols) != 0:
            if self.key_cols[0] == "*":
                inclusion_columns = [field.name for field in schema.fields if field.name not in self.exclusion_cols]
            else:
                inclusion_columns = self.key_cols + testing_columns

        schema_column_data_types = {field.name: field.dataType.simpleString() for field in schema.fields if
                                    field.name in inclusion_columns}
        return schema_column_data_types

    def get_table(self,
                  recon_aggs=None,
                  recon_join_cols=None,
                  recon_select_cols=None,
                  recon_drop_cols=None,
                  recon_col_mapping=None,
                  recon_trnsfrms=None,
                  recon_filters=None,
                  recon_tbl_thresholds=None
                  ):

        """
        Return Table Object based on the received inputs for Reconciliation:
        Inputs:
            - Aggregations for Recon
            - Join Columns for Aggregations
            - Columns on which Reconciliation to be actioned against
            - Columns to be dropped from Reconciliation
            - Column Mapping for disparate Column names to be applied
            - Transformation to be applied for Columns
            - Filters to be applied against Source & Target for Reconciliation
            - Thresholds for Columns to be applied for Reconciliation

        Output:
            Table Object to be utilized for Recon
        """

        table = Table(source_name=self.src_schema_table.split('.')[1],
                      target_name=self.tgt_schema_table.split('.')[1],
                      aggregates=recon_aggs,
                      join_columns=recon_join_cols,
                      select_columns=recon_select_cols,
                      drop_columns=recon_drop_cols,
                      column_mapping=recon_col_mapping,
                      transformations=recon_trnsfrms,
                      filters=recon_filters,
                      table_thresholds=recon_tbl_thresholds
                      )

        return table

    def execute_data_recon(self,recon_columns:List[str],report_type="data"):
        """
            Process for executing Data Level Recon
            Inputs:
                Columns against which Recon is to be applied
            Output:
                Data Recon Pass Status as a Boolean Flag
                Data Recon Id
                Columns against which Recon failed
        """

        recon_join_cols = self.key_cols

        if self.data_comparison_filter != '':
            recon_filter = Filters(source = f"{self.data_comparison_filter}",
            target = f"{self.data_comparison_filter}")
        else:
            recon_filter = None

        recon_table = self.get_table(
                  recon_aggs=None,
                  recon_join_cols=recon_join_cols if recon_join_cols else recon_columns,
                  recon_select_cols=recon_columns if not recon_join_cols else [],
                  recon_drop_cols=None,
                  recon_col_mapping=None,
                  recon_trnsfrms=None,
                  recon_filters=recon_filter,
                  recon_tbl_thresholds=None
                  )

        recon_config = self.get_recon_config(report_type)

        table_recon = TableRecon(
            source_catalog=self.src_catalog,
            source_schema=self.src_schema_table.split(".")[0],
            target_catalog=self.tgt_catalog,
            target_schema=self.tgt_schema_table.split(".")[0],
            tables=[recon_table]
        )

        try:
            exec_data_recon = recon(
            ws = self.wrkspc_client,
            spark = self.spark,
            table_recon = table_recon,
            reconcile_config = recon_config
            )

            data_recon_passed = True
            success_data_recon_id = exec_data_recon.recon_id
            failed_columns_list = []

            return data_recon_passed,success_data_recon_id,failed_columns_list


        except ReconciliationException as recon_excep:
            print("Recon Exception")
            data_failure_output  = recon_excep.reconcile_output

            data_recon_passed = True
            failed_columns_list = []

            failed_data_recon_id: str = data_failure_output.recon_id
            failed_data_recon_results: list[ReconcileTableOutput] = data_failure_output.results

            if len(failed_data_recon_results) != 0:
                data_recon_passed = False
                failed_columns_list = recon_columns

            return data_recon_passed, failed_data_recon_id, failed_columns_list

        except Exception as ex:
            print(str(ex))
            raise Exception(str(ex))


    def execute_aggregate_recons(self,report_type="row"):
        """
            Process to Execute Aggregation Recon
            Inputs:
                Report Type
            Outputs:
                Aggregations Recon Pass Status as a Boolean Flag
                Aggregation Recon Id
                Columns against which Recon failed
        """
        logger.info("Starting Aggregate & Row Recon")
        recon_agg_helper = DataType_Recon()
        input_columns_mapping = self.get_agg_table_schema()
        recon_aggs, select_cols = recon_agg_helper.get_agg_recon_table_objects(input_columns_mapping,[])
        if self.data_comparison_filter != '':
            recon_filter = Filters(source=f"{self.data_comparison_filter}",
                                   target=f"{self.data_comparison_filter}")
        else:
            recon_filter = None

        agg_recon_table = self.get_table(recon_aggs=recon_aggs,
                                     recon_join_cols=None,
                                     recon_select_cols=None,
                                     recon_drop_cols=None,
                                     recon_col_mapping=None,
                                     recon_trnsfrms=None,
                                     recon_filters=recon_filter,
                                     recon_tbl_thresholds=None)

        recon_config = self.get_recon_config(report_type)

        agg_table_recon = TableRecon(
            source_catalog=self.src_catalog,
            source_schema=self.src_schema_table.split(".")[0],
            target_catalog = self.tgt_catalog,
            target_schema = self.tgt_schema_table.split(".")[0],
            tables = [agg_recon_table]
        )

        data_exec_required = False
        success_recon_id = ""
        failed_columns = []

        try:
            logger.info("Executing Aggregate Recon")
            if len(recon_aggs) != 0:
                exec_agg_recon = reconcile_aggregates(
                    ws = self.wrkspc_client,
                    spark = self.spark,
                    table_recon = agg_table_recon,
                    reconcile_config = recon_config
                )

                success_agg_recon_id = exec_agg_recon

                success_recon_id = success_agg_recon_id

            return data_exec_required,success_recon_id,failed_columns

        except ReconciliationException as agg_recon_excep:
            logger.error("Aggregate Recon Failed")
            data_exec_required = True
            agg_recon_failure_output: ReconcileOutput = agg_recon_excep.reconcile_output
            failed_recon_id: str = agg_recon_failure_output.recon_id
            failed_agg_recon_results: list[ReconcileTableOutput] = agg_recon_failure_output.results

            if len(failed_agg_recon_results) != 0:


                failed_columns = list(self.spark.sql(f"""Select distinct rule_info.agg_column 
                                        from {self.metadata_catalog}.{self.metadata_schema}.aggregate_rules 
                                        where rule_id in (
                                        Select rule_id 
                                        from {self.metadata_catalog}.{self.metadata_schema}.aggregate_details
                                         where recon_table_id in (
                                         Select recon_table_id 
                                         from {self.metadata_catalog}.{self.metadata_schema}.main where recon_id = '{agg_recon_failure_output.recon_id}' ))""")\
                                              .toPandas()['agg_column'])

            try:
                logger.info("Executing Row Recon")
                if len(select_cols) != 0:
                    row_recon_table = self.get_table(recon_aggs=None,
                                                     recon_join_cols=None,
                                                     recon_select_cols=select_cols,
                                                     recon_drop_cols=None,
                                                     recon_col_mapping=None,
                                                     recon_trnsfrms=None,
                                                     recon_filters=recon_filter,
                                                     recon_tbl_thresholds=None)

                    recon_config = self.get_recon_config(report_type)

                    row_table_recon = TableRecon(
                        source_catalog=self.src_catalog,
                        source_schema=self.src_schema_table.split(".")[0],
                        target_catalog=self.tgt_catalog,
                        target_schema=self.tgt_schema_table.split(".")[0],
                        tables=[row_recon_table]
                    )

                    exec_trnsfrm_recon = recon(
                        ws=self.wrkspc_client,
                        spark=self.spark,
                        table_recon=row_table_recon,
                        reconcile_config=recon_config
                    )

                    success_recon_id = exec_trnsfrm_recon.recon_id

            except ReconciliationException as trnfrm_recon_excep:
                print("Recon Exception")
                trnfrm_recon_failure_output = trnfrm_recon_excep.reconcile_output

                if failed_recon_id != "":
                    failed_recon_id = f"{failed_recon_id},{trnfrm_recon_failure_output.recon_id}"
                else:
                    failed_recon_id = trnfrm_recon_failure_output.recon_id

                failed_trnsfrm_recon_results: list[ReconcileTableOutput] = trnfrm_recon_failure_output.results

                if len(failed_trnsfrm_recon_results) != 0:
                    data_exec_required = True
                    if len(failed_columns) == 0:
                        failed_columns = list(self.spark.sql(f"""Select distinct rule_info.agg_column 
                                                from {self.metadata_catalog}.{self.metadata_schema}.aggregate_rules 
                                                where rule_id in (
                                                Select rule_id 
                                                from {self.metadata_catalog}.{self.metadata_schema}.aggregate_details
                                                 where recon_table_id in (
                                                 Select recon_table_id 
                                                 from {self.metadata_catalog}.{self.metadata_schema}.main where recon_id = '{trnfrm_recon_failure_output.recon_id}' ))""")\
                                              .toPandas()['agg_column'])
                    else:

                        failed_columns = failed_columns + list(self.spark.sql(f"""Select distinct rule_info.agg_column 
                                                from {self.metadata_catalog}.{self.metadata_schema}.aggregate_rules 
                                                where rule_id in (
                                                Select rule_id 
                                                from {self.metadata_catalog}.{self.metadata_schema}.aggregate_details
                                                 where recon_table_id in (
                                                 Select recon_table_id 
                                                 from {self.metadata_catalog}.{self.metadata_schema}.main where recon_id = '{trnfrm_recon_failure_output.recon_id}' ))""")\
                                              .toPandas()['agg_column'])

                return data_exec_required, failed_recon_id, failed_columns

        except Exception as ex:
            print(str(ex))
            raise Exception(str(ex))