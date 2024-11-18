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


class FileStoreDataSource(DataSource, SecretsMixin):

    def __init__(
        self,
        engine: Dialect,
        spark: SparkSession,
        ws: WorkspaceClient,
        secret_scope: str,
        file_config:str
    ):
        self._engine = engine
        self._spark = spark
        self._ws = ws
        self._secret_scope = secret_scope
        self._file_config = file_config

    def read_data(
        self,
        catalog: str | None,
        schema: str,
        table: str,
        query: str,
        options: JdbcReaderOptions | None,
    ) -> DataFrame:

        header = True if self._file_config['header_info'] == 'Y' else False
        field_separator = self._file_config['field_separator']
        try:
            df = self._spark.read.format("csv").option("header", header).option("sep", field_separator).load(table)
            return df.select([col(column).alias(column.lower()) for column in df.columns])
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "data", table)

    def get_schema(
        self,
        catalog: str | None,
        schema: str,
        table: str,
    ) -> list[Schema]:
        header = True if self._file_config['header_info'] == 'Y' else False
        field_separator = self._file_config['field_separator']

        try:
            logger.info(f"Fetching Schema: Started at: {datetime.now()}")
            schema_metadata = self._spark.read.format("csv").option("header", header).option("sep", field_separator).load(table)\
                .where("col_name not like '#%'").distinct().collect()
            logger.info(f"Schema fetched successfully. Completed at: {datetime.now()}")
            return [Schema(field.col_name.lower(), field.data_type.lower()) for field in schema_metadata]
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "schema", "File Read")
