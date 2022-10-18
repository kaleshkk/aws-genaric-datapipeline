from aws_cdk import (
    Stage,
    Environment as _Environment
)

from constructs import Construct

from templates.cds_view_template import CdsViewTemplate

class TemplateStack(Stage):
    def __init__(self, scope: Construct, construct_id: str, json_dict: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env = _Environment(region="us-east-1", account="680832645642")

        CdsViewTemplate(self, construct_id, json_dict)