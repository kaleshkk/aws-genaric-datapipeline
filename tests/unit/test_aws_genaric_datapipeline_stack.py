import aws_cdk as core
import aws_cdk.assertions as assertions

from aws_genaric_datapipeline.aws_genaric_datapipeline_stack import AwsGenaricDatapipelineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in aws_genaric_datapipeline/aws_genaric_datapipeline_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AwsGenaricDatapipelineStack(app, "aws-genaric-datapipeline")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
