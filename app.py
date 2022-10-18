#!/usr/bin/env python3
import aws_cdk as cdk
from cicd_pipeline.cicd_pipeline_stack import CICDPipelineStack

app = cdk.App()
CICDPipelineStack(app, "PipelineStack")
app.synth()
