from aws_cdk import (
    Stage,
    pipelines as _pipelines,
    Environment as _Environment
)

from constructs import Construct
import os
from pathlib import Path
import json

from basic_infrastructure.basic_infrastructure_stack import BasicInfrastructureStack
from templates.cds_view_template import CdsViewTemplate

class Application(Stage):
    def __init__(self, scope, id, *, env=None, outdir=None, stageName=None):
        super().__init__(scope, id, env=env, outdir=outdir, stageName=stageName)

        env = _Environment(region="us-east-1", account="680832645642")

        dir_path = Path().absolute()
        pipeline_path = os.path.join(dir_path, "pipelines")

        for path, subdirs, files in os.walk(pipeline_path):
            for name in files:
                file_path = os.path.join(path, name)
                f = open(file_path)
                data = json.load(f)

                if data['template'] == 'cds_view':
                    stack_name = data["project"] + "-" + data["subject"] + "-" + data["config"]["job_src"]
                    CdsViewTemplate(self, stack_name, json_dict=data, env=env)