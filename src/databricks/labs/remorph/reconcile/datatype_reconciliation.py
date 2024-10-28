import re

from src.databricks.labs.remorph.reconcile.recon_config import Transformation, Aggregate, Table
from typing import Dict, List


class DataType_Recon():
    def __init__(self):
        self.string_data_type_matcher = r"(?i)\b(?:string)"
        self.numeric_data_type_matcher = r"(?i)\b(?:decimal|int|float|timestamp)"
        self.date_data_type_matcher = r"(?i)\b(?:date)"
        self.bool_data_type_matcher = r"(?i)\b(?:boolean)"

    def agg_str_type_test(self,column_name:str,group_by_columns:List[str]):
        transforms = [Transformation(
            column_name=column_name,
            source=f"len({column_name})",
            target=f"len({column_name})"
        ),
        Transformation(
            column_name=column_name,
            source=f"min({column_name})",
            target=f"min({column_name})"
        ),
        Transformation(
            column_name=column_name,
            source=f"max({column_name})",
            target=f"max({column_name})"
        )]

        aggregates = []

        return aggregates,transforms

    def agg_numerical_type_test(self,column_name:str,grp_by_clmns:List[str]):
        aggregates = [
            Aggregate(
                agg_columns=[column_name],
                type="MIN",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="MAX",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="MEAN",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="SUM",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            )
            ]

        transforms = []
        return aggregates,transforms

    def agg_bool_type_test(self,column_name:str,grp_by_clmns:List[str]):
        aggregates = [
            Aggregate(
                agg_columns=[column_name],
                type="SUM",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="MAX",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            )
        ]
        transforms = []
        return aggregates,transforms

    def agg_date_type_test(self,column_name:str,grp_by_clmns:List[str]):
        aggregates = [
            Aggregate(
                agg_columns=[column_name],
                type="MIN",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="MAX",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="SUM",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            )
        ]

        transforms = [
            Transformation(
                column_name=column_name,
                source=f"datediff(max({column_name}),min({column_name}))",
                target=f"datediff(max({column_name}),min({column_name}))"
            )
        ]

        return aggregates,transforms

    def agg_catchall_type_test(self,column_name,grp_by_clmns:List[str]):
        aggregates = [
            Aggregate(
                agg_columns=[column_name],
                type="MIN",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            ),
            Aggregate(
                agg_columns=[column_name],
                type="MAX",
                group_by_columns = grp_by_clmns if len(grp_by_clmns) != 0 else None
            )
        ]

        transforms = []

        return  aggregates,transforms


    def get_agg_recon_table_objects(self,input_columns:Dict,group_by_columns:List[str]):
        aggregations = transformations = []
        for column_name in input_columns:
            datatype = input_columns[column_name]
            if len(re.findall(self.string_data_type_matcher, datatype)) != 0:
                dt_type_aggregations , dt_type_transformations = self.agg_str_type_test(column_name,group_by_columns)
                aggregations = aggregations + dt_type_aggregations
                transformations =  transformations + dt_type_transformations

            elif len(re.findall(self.numeric_data_type_matcher, datatype)) != 0:
                dt_type_aggregations , dt_type_transformations = self.agg_numerical_type_test(column_name,group_by_columns)
                aggregations = aggregations + dt_type_aggregations
                transformations = transformations + dt_type_transformations

            elif len(re.findall(self.date_data_type_matcher, datatype)) != 0:
                dt_type_aggregations , dt_type_transformations = self.agg_date_type_test(column_name,group_by_columns)
                aggregations = aggregations + dt_type_aggregations
                transformations = transformations + dt_type_transformations

            elif len(re.findall(self.bool_data_type_matcher, datatype)) != 0:
                dt_type_aggregations , dt_type_transformations = self.agg_date_type_test(column_name,group_by_columns)
                aggregations = aggregations + dt_type_aggregations
                transformations = transformations + dt_type_transformations

            else:
                dt_type_aggregations , dt_type_transformations = self.agg_catchall_type_test(column_name,group_by_columns)
                aggregations = aggregations + dt_type_aggregations
                transformations = transformations + dt_type_transformations

        return aggregations,transformations
