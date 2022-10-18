from aws_cdk import (
    Duration,
    Stack,
    pipelines as _pipelines,
    Environment as _Environment
)
from constructs import Construct
import os
from pathlib import Path
import json

from basic_infrastructure.basic_infrastructure_stack import BasicInfrastructureStack
from templates.cds_view_template import CdsViewTemplate


class CICDPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        pipeline = _pipelines.CodePipeline(self, "Pipeline",
                                           synth=_pipelines.ShellStep("Synth",
                                                                     input=_pipelines.CodePipelineSource.connection(
                                                                         "my-org/my-app", "main",
                                                                         connection_arn="arn:aws:codestar-connections:us-east-1:222222222222:connection/7d2469ff-514a-4e4f-9003-5ca4a43cdc41"
                                                                         ),
                                                                     commands=["npm ci", "npm run build",
                                                                               "npx cdk synth"
                                                                               ]
                                                                     )
                                           )

        env = _Environment(region="us-east-1", account="680832645642")
        pipeline.add_stage(BasicInfrastructureStack(self, "BasicInfrastructureStack", env=env))

        dir_path = Path().absolute()
        pipeline_path = os.path.join(dir_path, "../", "pipelines")

        for path, subdirs, files in os.walk(pipeline_path):
            for name in files:
                file_path = os.path.join(path, name)
                f = open(file_path)
                data = json.load(f)

                if data['template'] == 'cds_view':
                    stack_name = data["project"] + "-" + data["subject"] + "-" + data["config"]["job_src"]
                    CdsViewTemplate(self, stack_name, json_dict=data, env=env)


