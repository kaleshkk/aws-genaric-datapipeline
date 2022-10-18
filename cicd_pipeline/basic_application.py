from aws_cdk import (
    Stage,
    Environment as _Environment
)

from constructs import Construct

from basic_infrastructure.basic_infrastructure_stack import BasicInfrastructureStack
class BasicApplication(Stage):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env = _Environment(region="us-east-1", account="680832645642")

        BasicInfrastructureStack(self, "BasicInfrastructureStack")