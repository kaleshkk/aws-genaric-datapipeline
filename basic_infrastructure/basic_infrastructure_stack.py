from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as _dynamodb,
    aws_glue as _glue,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _event_sourse,
    aws_s3 as _s3,
    aws_s3_deployment as _s3_deployment,
    RemovalPolicy as rp
)
from constructs import Construct
from pathlib import Path
from os import path as _path


class BasicInfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ## import vpc
        # vpc = _ec2.Vpc.from_lookup(self, "vpc_id", vpc_id='vpc-0a7b6569fa938dc03')

        # Create DynamoDB
        dytbl = _dynamodb.Table(self, "pipeline-table", table_name="pipeline_table",partition_key=_dynamodb.Attribute(
            name='partition_key', type=_dynamodb.AttributeType.STRING),
            removal_policy=rp.DESTROY,
            stream=_dynamodb.StreamViewType.NEW_IMAGE,
            )

        # Glue job
        raw_job = _glue.CfnJob(self,
                               id="raw_layer_job",
                               name="raw_layer_job",
                               role="AWSGlueServiceRole-ETL",
                               command=_glue.CfnJob.JobCommandProperty(
                                   name="glueetl",
                                   python_version="3",
                                   script_location="s3://genaric-pipeline-code-resourse/source/raw_layer_job.py"
                               ),
                               connections= {
                                    "connections": ['my-sql'],
                                },
                               glue_version="2.0",
                               number_of_workers=2,
                               worker_type="Standard",
                               default_arguments={
                                   '--job_src': 'not set',
                                   '--extra-jars':'s3://genaric-pipeline-code-resourse/jar/mysql-connector-java-8.0.30.jar'
                               }
                               )

        prepared_job = _glue.CfnJob(self,
                                    id="prepared_layer_job",
                                    name="prepared_layer_job",
                                    role="AWSGlueServiceRole-ETL",
                                    command=_glue.CfnJob.JobCommandProperty(
                                        name="glueetl",
                                        python_version="3",
                                        script_location="s3://genaric-pipeline-code-resourse/source/prepared_layer_job.py"
                                    ),
                                    glue_version="2.0",
                                    number_of_workers=2,
                                    worker_type="Standard",
                                    default_arguments={
                                        '--job_src': 'not set'
                                    }
                                    )

        # create lambda
        # Defines an AWS Lambda resource
        my_lambda = _lambda.Function(
            self, 'invoke_prepared',
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.Code.from_asset('lambda'),
            handler='invoke_prepared.handler',
        )

        my_lambda.add_event_source(_event_sourse.DynamoEventSource(dytbl,
            starting_position=_lambda.StartingPosition.TRIM_HORIZON,
            batch_size=1
        ))


        dir_path  = Path().absolute()
        pipeline_path = _path.join(dir_path, "pipelines")
        glue_path = _path.join(dir_path, "glue src")
        destination_bucket = _s3.Bucket.from_bucket_arn(self, "dest_bucket", "arn:aws:s3:::genaric-pipeline-code-resourse")

        _s3_deployment.BucketDeployment(self, 
            "sourse_file_deplayment",
            sources=[_s3_deployment.Source.asset(pipeline_path)],
            destination_bucket=destination_bucket,
            destination_key_prefix='pipelines/'
        )

        _s3_deployment.BucketDeployment(self, 
            "sourse_code_deplayment",
            sources=[_s3_deployment.Source.asset(glue_path)],
            destination_bucket=destination_bucket,
            destination_key_prefix='source/'
        )
        

            
