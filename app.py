#!/usr/bin/env python3
import os
from pathlib import Path
import json

import aws_cdk as cdk
from aws_genaric_datapipeline.aws_genaric_datapipeline_stack import AwsGenaricDatapipelineStack
from templates.cds_view_template import CdsViewTemplate

app = cdk.App()
env = cdk.Environment(region="us-east-1", account="680832645642")
AwsGenaricDatapipelineStack(app, "AwsGenaricDatapipelineStack", env=env)

dir_path  = Path().absolute()
pipeline_path = os.path.join(dir_path, "pipelines")

for path, subdirs, files in os.walk(pipeline_path):
    for name in files:
        file_path = os.path.join(path, name)
        f = open(file_path)
        data = json.load(f)

        if data['template'] == 'cds_view':
            stack_name = data["project"] + "-" + data["subject"] + "-" + data["config"]["job_src"]
            CdsViewTemplate(app, stack_name, json_dict=data,  env=env)


app.synth()
