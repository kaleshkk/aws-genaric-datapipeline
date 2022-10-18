from aws_cdk import (
    Stack,
    pipelines as _pipelines
)
from constructs import Construct
from pathlib import Path
from application import Application



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

        
        pipeline.add_stage(Application(self, "Application"))
