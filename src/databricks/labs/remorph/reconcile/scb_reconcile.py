from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from typing import List

from reconciliation_entrypoint import metastore_schema
from src.databricks.labs.remorph.config import DatabaseConfig, ReconcileMetadataConfig, ReconcileConfig, TableRecon
from src.databricks.labs.remorph.reconcile.datatype_reconciliation import DataType_Recon
from src.databricks.labs.remorph.reconcile.execute import reconcile_aggregates
from src.databricks.labs.remorph.reconcile.recon_config import Table, ReconcileOutput, ReconcileTableOutput

import re

from tests.unit.reconcile.connectors.test_mock_data_source import catalog


class SCB_Reconcile():

    def __init__(self,table:str,
                 layer:str,
                 sqlServer:str,
                 sqlDatabase:str,
                 sqlUser:str,
                 sqlPassword:str,
                 metadata_catalog:str,
                 metadata_schema:str,
                 metadata_volume:str,
                 spark:SparkSession):

        self.table = table
        self.layer = layer
        self.metadata_catalog = metadata_catalog
        self.metadata_schema = metadata_schema
        self.metadata_volume = metadata_volume

        self.spark = spark
        self.sqlServer = sqlServer
        self.sqlDatabase = sqlDatabase
        self.sqlUser = sqlUser
        self.sqlPassword = sqlPassword

        self.configuration_table_mapping = {
        "ingestion": "p1dcfudp.UDP_JOB",
        "transformation": "p1dcfudp.UDP_JOB_TRANSFORM_CHK_UNQ"
        }
        self.sqlConnectionString = f"""jdbc:sqlserver://{self.sqlServer}.database.windows.net:1433;database={self.sqlDatabase};user={self.sqlUser};password={self.sqlPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryPassword"""
        print(self.sqlConnectionString)
        self.dbutils = DBUtils(self.spark)

        self.dbx_catalog, self.dbx_schema_table = self.table_name_split()

        self.wrkspc_client = WorkspaceClient(
            product="reconcile",
            product_version="0.0.1"
        )


    def table_name_split(self):

        table_name_pattern = r"^([^\.]+)\.([^\.]+)\.([^\.]+)$"
        tbl_nm_matches = re.findall(table_name_pattern,self.table)
        if len(tbl_nm_matches[0]) != 3:
            raise Exception("Invalid Table Name, Ensure the table name is in the pattern catalog.schema.table")
        else:
            dbx_catalog = tbl_nm_matches[0][0]
            dbx_schema_table = tbl_nm_matches[0][1]+ "." + tbl_nm_matches[0][2]



        return dbx_catalog,dbx_schema_table


    def query(self,
              configuration_table:str):

        query_string = f"""Select * from {configuration_table}"""

        configuration_data = self.spark.read\
            .format("jdbc")\
            .option("url",self.sqlConnectionString)\
            .option("query",query_string)\
            .load()

        return configuration_data


    def get_config(self):

        config_data = self.query(self.configuration_table_mapping[self.layer.lower()])
        return config_data

    def get_table_schema(self,exclusion_columns:List[str]=[]):

        schema = self.spark.read.table(self.table).schema
        system_exclusion_columns = ['row_md5','batch_uuid']
        exclusion_columns_final = list(set(exclusion_columns + system_exclusion_columns))

        schema_column_data_types = {field.name: field.dataType.simpleString() for field in schema.fields if field.name not in exclusion_columns_final}
        return schema_column_data_types

    def get_recon_config(self,report_type):

        db_config = DatabaseConfig(
            source_catalog="hive_metastore",
            target_catalog=self.dbx_catalog,
            target_schema=self.dbx_schema_table.split(".")[0],
            source_schema=self.dbx_schema_table.split(".")[0]
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




    def execute_aggregate_recons(self,group_by_columns:List[str]=[],exclusion_columns:List[str]=[],report_type="row"):
        recon_agg_helper = DataType_Recon()
        input_columns_mapping = self.get_table_schema(exclusion_columns)
        recon_aggs, recon_trnsfrms = recon_agg_helper.get_recon_objects(input_columns_mapping,group_by_columns)
        table = Table(source_name=self.dbx_schema_table.split('.')[1],
                      target_name=self.dbx_schema_table.split('.')[1],
                      aggregates = recon_aggs,
                      transformations=recon_trnsfrms)

        recon_config = self.get_recon_config(report_type)

        table_recon = TableRecon(
            source_catalog="hive_metastore",
            source_schema=self.dbx_schema_table.split(".")[0],
            target_catalog = self.dbx_catalog,
            target_schema = self.dbx_schema_table.split(".")[0],
            tables = [table]
        )

        try:
            exec_recon:ReconcileOutput = reconcile_aggregates(
            ws = self.wrkspc_client,
            spark = self.spark,
            table_recon = table_recon,
            reconcile_config = recon_config
            )

            data_exec_required = False

            exec_recon_id:str = exec_recon.recon_id
            exec_recon_results: list[ReconcileTableOutput] = exec_recon.results

            if len(exec_recon_results) != 0:
                data_exec_required = True
                failed_columns = self.spark.sql(f"""Select distinct rule_info.agg_column 
                from {self.metastore_catalog}.{self.metastore_schema}.aggregate_rules 
                where rule_id in (
                Select rule_id 
                from {self.metastore_catalog}.{self.metastore_schema}.aggregate_details
                 where recon_table_id in (
                 Select recon_table_id 
                 from {self.metastore_catalog}.{self.metastore_schema}.main where recon_id = '{exec_recon_id}' ))""")

                failed_columns_list = list(failed_columns.toPandas()['agg_column'])

            return data_exec_required, exec_recon_id,failed_columns_list

        except Exception as ex:
            display(str(ex))