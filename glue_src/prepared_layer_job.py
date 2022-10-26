import sys
import boto3
import json
from boto3.dynamodb.conditions import Key

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext


def main():
    """
    main functions dose start the execution
    """

    ## initializing spark and glue context
    spark, glueContext = initializeGlueSpark()

    try:
        print("starting job....")
        job = Job(glueContext)
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'job_src'])

        job.init(args['JOB_NAME'], args)
        jobRunId = args['JOB_RUN_ID']
        jobName = args['JOB_NAME']
        job_src = args['job_src']

        s3 = boto3.resource('s3')
        content_object = s3.Object('genaric-pipeline-code-resourse', f"pipelines/{job_src}/config.json")
        file_content = content_object.get()['Body'].read().decode('utf-8')
        job_args = json.loads(file_content)

        preparedBucket = job_args['prepared']['preparedBucket']
        dynamoTable = job_args['config']['dynamoTable']
        print(dynamoTable, job_src)


        ## Read data from dynamoDB
        items = readFromDynamoDB(dynamoTable, job_src)
        print(items)

        for item in items:
            ## processing raw completed items
            if (item['State'] == 'RAW COMPLETED'):
                ## fetch source data from SAP env
                srcDYF = loadDataFrame(glueContext, item)

                # write data into raw layer
                writeDataFrame(glueContext, srcDYF, item, preparedBucket, jobName, dynamoTable)

    except Exception as e:
        print("ERROR main process: {0}".format(e))
        sys.exit(1)


def initializeGlueSpark():
    """
    Initilizing spark session and glue context
    """

    print("Initializing Spark and Glue Variables...")

    try:
        glueContext = GlueContext(SparkContext.getOrCreate())
        sparkSession = glueContext.sparkSession

        return (sparkSession, glueContext)

    except Exception as e:
        print("ERROR initializing the spark and glue variables: {0}".format(e))
        sys.exit(1)


def loadDataFrame(glueContext, item):
    """
    Read data from RAW layer and return DynamicFrame
    param glueContext: glue context
    param item: Dictionary contains single record from dynamoDB
    return: DynamicFrame of source data
    """

    try:
        print("Reading data from raw layer...")

        ## reading data from raw layer, path creating from dynamoDB logged record
        raw_DYF = glueContext.create_dynamic_frame_from_options(
            's3',
            {'paths': ['s3://' + item['RawBucket'] + '/' + item['RawFolder'] + '/' + item['partition_key'] + '/']},
            format='parquet'
        )

        print("Reading data from ray layer completed...")

        return raw_DYF
    except Exception as e:
        print("ERROR while reading data from source: {0}".format(e))
        sys.exit(1)


def writeDataFrame(glueContext, preparedDYF, item, preparedBucket, jobName, dynamoTable):
    """
    Write data into Prepared layer both incremental and compacted
    param glueContext: glue context
    param preparedDYF: final dataframe after all transformation
    param item: Dictionary contains single record from dynamoDB
    param jobName: glue job name
    param dynamoTable: DynamoDB name
    """

    try:
        print("Write data into Raw layer...")
        preparedDF = preparedDYF.toDF()
        preparedDYF = DynamicFrame.fromDF(preparedDF, glueContext, "preparedDYF")
        preparedEntryCount = preparedDF.count()
        preparedS3Folder =  item["RawFolder"]

        print("Writing to S3 Incremental location")
        glueContext.write_dynamic_frame_from_options(
            frame=preparedDYF, connection_type="s3",
            connection_options={
                "path": "s3://" + preparedBucket + "/" + preparedS3Folder,
                "compression": "snappy"},
            format="parquet")
        print("Written to S3 Incremental location")

        print("Written to S3 Incremental location")

        ##Update job details in dynamodb
        updateDynamoDB(item, preparedBucket, preparedS3Folder, preparedEntryCount, jobName, dynamoTable)
    except Exception as e:
        print("ERROR while writing data into s3: {0}".format(e))
        sys.exit(1)


def readFromDynamoDB(dynamoTable, job_src):
    """
    Read data from dynamoDB for given job scr
    param dynamoTable: dynamoDB name
    param job_src: job src name
    return: record related given job src
    """
    try:
        print("Reading from DynamoDB")
        resource = boto3.resource('dynamodb')
        table = resource.Table(dynamoTable)
        scanKeyWordArgs = {
            'FilterExpression': Key('job_src').eq(job_src) & Key('State').eq("RAW COMPLETED"),
            'ConsistentRead': True
        }

        done = False
        startKey = None
        items = []

        while not done:
            if startKey:
                scanKeyWordArgs['ExclusiveStartKey'] = startKey

            response = table.scan(**scanKeyWordArgs)
            items.extend(response.get('Items', []))
            startKey = response.get('LastEvaluatedKey', None)
            done = startKey is None

        return items

    except Exception as e:
        print("Error in  readDynamoDB: {0}".format(e))
        sys.exit(1)

def updateDynamoDB(item, preparedBucket, preparedS3Folder, preparedEntryCount, jobName, dynamoTable):
    """
    Log pipeline logs into dynamoDB
    param item: Dictionary contains single record from dynamoDB
    param preparedBucket: raw s3 bucket
    param preparedEntryCount: number records in loadDataFrame
    param jobName: glue job name
    param dynamoTable: DynamoDB name
    """

    try:
        print("Writing log into dynamoDB...")
        client = boto3.client('dynamodb')
        response = client.put_item(
            TableName='pipeline_table',
            Item={
                 "partition_key": {"S": str(item['partition_key'])},
                 "State": {"S": "PREPARED COMPLETED"},
                 "job_src": {"S": item['job_src']},
                 "RawBucket": {"S": item['RawBucket']},
                 "RawEntryCount": {"S": str(item['RawEntryCount'])},
                 "RawFolder": {"S": item['RawFolder']},
                 "RawJobName": {"S": jobName},
                 "PreparedBucket": {"S": preparedBucket},
                 "PreparedFolder": {"S": preparedS3Folder},
                 "PreparedJobName": {"S": jobName},
                 "PreparedEntryCount": {"S": str(preparedEntryCount)}
            }
        )
        print("Writing log into dynamoDB completed.")
    except Exception as e:
        print("ERROR while logging into DynamoDB: {0}".format(e))
        sys.exit(1)


if __name__ == "__main__":
    main()