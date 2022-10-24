from aws_cdk import (
    Duration,
    Stack,
    aws_glue as _glue,
    RemovalPolicy as rp
)
from constructs import Construct
from pathlib import Path
from os import path as _path


class CdsViewTemplate(Stack):

    def __init__(self, scope: Construct, construct_id: str,json_dict: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ## create trigger
        trigger = _glue.CfnTrigger(self, 
            "CnfCDSviewTrigger", 
            type="ON_DEMAND",
            name=json_dict["project"] + "-" + json_dict["subject"] + "-" + json_dict["config"]["job_src"] + "-trigger",
            actions=[_glue.CfnTrigger.ActionProperty(job_name="raw_layer_job", arguments={"job_src": json_dict["config"]["job_src"]})]
            )

        ## create table
        columns = []

        for col in json_dict["data_object"]["schema"]:
            columns.append(_glue.CfnTable.ColumnProperty(
                        name=col["name"],
                        comment=col["comment"],
                        type=col["type"]
                    ))

        cfn_table = _glue.CfnTable(self, 
            "GlueCfnTable", 
            catalog_id="680832645642",
            database_name="pipelines_db",
            removal_policy=rp.DESTROY,
            table_input=_glue.CfnTable.TableInputProperty(
                description="description",
                name=json_dict["data_object"]["name"],
                storage_descriptor=_glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    compressed=False,
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    location="s3://" + json_dict["prepared"]["preparedBucket"] + "/" + json_dict["raw"]["rawS3Folder"] + "/",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=_glue.CfnTable.SerdeInfoProperty(
                        name="parquet",
                        parameters={"serialization.format": "1"},
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    )
                ),
                table_type="EXTERNAL_TABLE"
        ))





