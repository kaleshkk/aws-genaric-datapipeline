#!/usr/bin/env python3
import aws_cdk as cdk
from cicd_pipeline.cicd_pipeline_stack import CICDPipelineStack

app = cdk.App()
CICDPipelineStack(app, "PipelineStack",
    env=cdk.Environment(
        account="680832645642",
        region="us-east-1"
    ))
app.synth()
