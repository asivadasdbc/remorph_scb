import logging
import re
import yaml
from datetime import datetime

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from sqlglot import Dialect

from src.databricks.labs.remorph.reconcile.connectors.data_source import DataSource
from src.databricks.labs.remorph.reconcile.connectors.secrets import SecretsMixin
from src.databricks.labs.remorph.reconcile.recon_config import JdbcReaderOptions, Schema
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class OlapDataSource(DataSource, SecretsMixin):

    def __init__(
        self,
        engine: Dialect,
        spark: SparkSession,
        ws: WorkspaceClient,
        secret_scope: str,
        connection_string:str
    ):
        self._engine = engine
        self._spark = spark
        self._ws = ws
        self._secret_scope = secret_scope
        self._connection_string = connection_string

    def read_data(
        self,
        catalog: str | None,
        schema: str,
        table: str,
        query: str,
        options: JdbcReaderOptions | None,
    ) -> DataFrame:

        table_query = query.replace(":tbl", "olap_data")
        data_read_query = f"""Select * from {schema}.{table}"""

        try:

            self._spark.read.format("jdbc")\
                .option("url", self._connection_string)\
                .option("query",data_read_query)\
                .load()\
                .createOrReplaceTempView("file_data")
            df = self._spark.sql(table_query)

            return df.select([col(column).alias(column.lower()) for column in df.columns])
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "data", table)

    def get_schema(
        self,
        catalog: str | None,
        schema: str,
        table: str,
    ) -> list[Schema]:

        try:
            data_read_query = f"""Select top 1 * from {schema}.{table}"""
            logger.info(f"Fetching Schema: Started at: {datetime.now()}")
            schema_metadata = self._spark.read.format("jdbc")\
                .option("url", self._connection_string)\
                .option("query",data_read_query)\
                .load()\
                .createOrReplaceTempView("file_data")\
                .schema
            logger.info(f"Schema fetched successfully. Completed at: {datetime.now()}")
            return [Schema(field.name.lower(), field.dataType.simpleString().lower()) for field in schema_metadata if '#' not in field.name]
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "schema", "File Read")
