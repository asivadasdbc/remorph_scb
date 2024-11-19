from pyspark.sql import SparkSession
from sqlglot import Dialect

from src.databricks.labs.remorph.reconcile.connectors.data_source import DataSource
from src.databricks.labs.remorph.reconcile.connectors.databricks import DatabricksDataSource
from src.databricks.labs.remorph.reconcile.connectors.oracle import OracleDataSource
from src.databricks.labs.remorph.reconcile.connectors.snowflake import SnowflakeDataSource
from src.databricks.labs.remorph.reconcile.connectors.filestore import FileStoreDataSource
from src.databricks.labs.remorph.reconcile.connectors.olap import OlapDataSource
from src.databricks.labs.remorph.snow.databricks import Databricks
from src.databricks.labs.remorph.snow.oracle import Oracle
from src.databricks.labs.remorph.snow.snowflake import Snow
from src.databricks.labs.remorph.snow.filestore import FileStore
from src.databricks.labs.remorph.snow.olap import Olap
from databricks.sdk import WorkspaceClient


def create_adapter(
    engine: Dialect,
    spark: SparkSession,
    ws: WorkspaceClient,
    secret_scope: str,
    file_config: dict,
    olap_connection:str
) -> DataSource:
    if isinstance(engine, Snow):
        return SnowflakeDataSource(engine, spark, ws, secret_scope)
    if isinstance(engine, Oracle):
        return OracleDataSource(engine, spark, ws, secret_scope)
    if isinstance(engine, Databricks):
        return DatabricksDataSource(engine, spark, ws, secret_scope)
    if isinstance(engine, FileStore):
        return FileStoreDataSource(engine,spark,ws,secret_scope,file_config)
    if isinstance(engine, Olap):
        return OlapDataSource(engine,spark,ws,secret_scope,olap_connection)

    raise ValueError(f"Unsupported source type --> {engine}")
