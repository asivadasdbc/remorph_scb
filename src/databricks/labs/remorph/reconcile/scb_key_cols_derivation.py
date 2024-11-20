from pyspark.sql import SparkSession
from typing import List


class SCB_Key_Cols_Derivation():

    def __init__(self,
                 target_table_name:str,
                 connection_string:str,
                 additional_key_cols_list:List[str],
                 unique_keys_table:str,
                 outbound_config_table:str,
                 spark:SparkSession):

        self.target_table_name = target_table_name
        self.connection_string = connection_string
        self.additional_key_cols_list = additional_key_cols_list
        self.unique_keys_table = unique_keys_table
        self.outbound_config_table = outbound_config_table
        self.spark = spark

    def outbound_config_query(self):

        outbound_job = self.target_table_name.split("$")[0]

        query_string = f"""Select src_fl_nm_ext,field_separator,header_info,quotation
         from {self.outbound_config_table}  where job_name = '{outbound_job}'"""

        configuration_data = self.spark.read \
            .format("jdbc") \
            .option("url", self.connection_string) \
            .option("query", query_string) \
            .load()

        if configuration_data.count() > 0:
            configuration_data_dict = configuration_data.toPandas().to_dict('records')[0]
        else:
            raise Exception(f"Outbound Job {outbound_job} not found ")


        return configuration_data_dict


    def unique_keys_query(self):

        query_string = f"""Select column_nm_key from {self.unique_keys_table} 
        where catalog_nm = '{self.target_table_name.split(".")[0]}' and schema_nm = 
        '{self.target_table_name.split(".")[1]}' and table_nm = '{self.target_table_name.split(".")[-1]}'"""

        configuration_data = self.spark.read \
            .format("jdbc") \
            .option("url", self.connection_string) \
            .option("query", query_string) \
            .load()

        return configuration_data

    def get_final_key_cols(self):
        configured_key_cols = self.unique_keys_query()
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
