import imp
from aws_cdk import (
    Stack,
    pipelines as _pipelines
)
from constructs import Construct
from cicd_pipeline.basic_application import BasicApplication
from cicd_pipeline.template_stack import TemplateStack
import os
from pathlib import Path
import json


class CICDPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        pipeline = _pipelines.CodePipeline(self, "Pipeline",
            synth=_pipelines.ShellStep("Synth",
                # Use a connection created using the AWS console to authenticate to GitHub
                # Other sources are available.
                input=_pipelines.CodePipelineSource.connection("kaleshkk/aws-genaric-datapipeline", "pipeline",
                    connection_arn="arn:aws:codestar-connections:us-east-1:680832645642:connection/0824f3ec-e9ed-416b-8560-86ab187deb2a"
                ),
                commands=[
                    "npm install -g aws-cdk", "pip install -r requirements.txt", "cdk synth"
                ]
            )
        )

        # 'MyApplication' is defined below. Call `addStage` as many times as
        # necessary with any account and region (may be different from the
        # pipeline's).

        pipeline.add_stage(BasicApplication(self, "BasicApplication"))

        dir_path = Path().absolute()
        pipeline_path = os.path.join(dir_path, "pipelines")
        print(pipeline_path)
        for path, subdirs, files in os.walk(pipeline_path):
            for name in files:
                file_path = os.path.join(path, name)
                f = open(file_path)
                data = json.load(f)

                if data['template'] == 'cds_view':
                    stack_name = data["project"] + "-" + data["subject"] + "-" + data["config"]["job_src"]
                    pipeline.add_stage(TemplateStack(self, stack_name, json_dict=data))
                    