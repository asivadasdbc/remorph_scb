from pyspark.sql import SparkSession
from typing import List


class SCB_Key_Cols_Derivation():

    def __init__(self,
                 target_table_name:str,
                 connection_string:str,
                 additional_key_cols_list:List[str],
                 spark:SparkSession):

        self.target_table_name = target_table_name
        self.connection_string = connection_string
        self.additional_key_cols_list = additional_key_cols_list
        self.configuration_table = "p1dcfudp.UDP_JOB_TRANSFORM_CHK_UNQ"
        self.spark = spark

    def query(self):

        query_string = f"""Select column_nm_key from {self.configuration_table} where table_nm = '{self.target_table_name}'"""

        configuration_data = self.spark.read \
            .format("jdbc") \
            .option("url", self.connection_string) \
            .option("query", query_string) \
            .load()

        return configuration_data

    def get_final_key_cols(self):
        configured_key_cols = self.query()
        if configured_key_cols.isEmpty():
            configured_key_cols_str = ''
        else:
            configured_key_cols_str = configured_key_cols.collect()[0].column_nm_key

        if '[ALL_COLUMNS]' in configured_key_cols_str:
            key_cols_list = ["*"]
        elif configured_key_cols_str != "":
            configured_key_cols_list = configured_key_cols_str.split(",")
            if len(self.additional_key_cols_list) != 0:
                key_cols_list = list(set(configured_key_cols_list+self.additional_key_cols_list))
            else:
                key_cols_list = list(set(configured_key_cols_list))
        else:
            key_cols_list = []

        return key_cols_list
