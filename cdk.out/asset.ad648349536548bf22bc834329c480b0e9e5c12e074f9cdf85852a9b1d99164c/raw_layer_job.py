import sys
import boto3
import os
import json
from datetime import datetime
from pytz import timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import lit

def main():
    """
    main functions dose start the execution
    """
    ## initializing spark and glue context
    spark, glueContext = initializeGlueSpark()

    try:
        print("starting job....")
        job = Job(glueContext)

        ## resolving job args
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'job_src'])

        job.init(args['JOB_NAME'], args)
        job_run_id = args['JOB_RUN_ID']
        job_src = args['job_src']
        jobName = args['JOB_NAME']

        s3 = boto3.resource('s3')
        content_object = s3.Object('genaric-pipeline-code-resourse', f"pipelines/{job_src}/config.json")
        file_content = content_object.get()['Body'].read().decode('utf-8')
        job_args = json.loads(file_content)

        rawBucket = job_args['raw']['rawBucket']
        dynamoTable = job_args['config']['dynamoTable']
        rawS3Folder = job_args['raw']['rawS3Folder']
        cdsView = job_args['raw']['CDSView']

        ## fetch source data from SAP env
        srcDYF = loadDataFrame(glueContext, spark, cdsView)
        ## generating timestamp for data injection
        todayDateTimeFormatted = calculateDates()
        ## add ETL_PART_KEY
        targetDF = srcDYF.toDF().withColumn("ETL_PART_KEY", lit(todayDateTimeFormatted))
        targetDYF = srcDYF.fromDF(targetDF, glueContext, "targetDYF")
        # write data into raw layer
        writeDataFrame(glueContext, targetDYF, rawBucket, rawS3Folder, job_src, jobName, dynamoTable, todayDateTimeFormatted)
    except Exception as e:
        print("ERROR main process: {0}".format(e))
        sys.exit(1)

def initializeGlueSpark():
    """
    Initilizing spark session and glue context
    return: Tuple(spark session , glue context)
    """

    print("Initializing Spark and Glue Variables...")

    try:
        glueContext = GlueContext(SparkContext.getOrCreate())
        sparkSession = glueContext.sparkSession

        return (sparkSession, glueContext)

    except Exception as e:
        print("ERROR initializing the spark and glue variables: {0}".format(e))
        sys.exit(1)

def calculateDates():
    """
    Calculating current date
    return: current timestamp (yyyymmddHHssSSSSSS)
    """
    try:
        ## set timezone to EST
        est = timezone('EST')
        todayDate = datetime.now(est)
        ## format timestamp inclusive of micro seconds
        todayDateTimeFormatted = datetime.strftime(todayDate, '%Y%m%d%H%M%S%f')
        todayDateTime = datetime.strftime(todayDate, '%Y-%m-%d,%H%M%S%f')

        print("Today Date : ", todayDateTimeFormatted)
        print("Today Date", todayDateTime)
        return todayDateTimeFormatted
    except Exception as e:
        print("ERROR calculating Dates: {0}".format(e))
        sys.exit(1)

def loadDataFrame(glueContext, spark, cdsView):
    """
    Read data from SAP using JDBC and return DynamicFrame
    param glueContext: glue context
    param spark: spark session
    param secretsId: AWS Secret manager's secret id
    param cdsView: CDS view name in the format of schema.view
    return: DynamicFrame of source data
    """

    try:
        print("Reading data from source...")

        ## Fetch credentials from secrets manager
        dbUsername = ''
        dbPassword = ''
        dbUrl = ''
        jdbcDriverName = ''

        srcDF = spark.read.format("jdbc") \
            .option("driver", jdbcDriverName) \
            .option("url", dbUrl) \
            .option("user", dbUsername) \
            .option("password", dbPassword) \
            .option(
            "dbtable",
            """(select * from {cdsView}) as view""".format(
                cdsView=cdsView)).load()

        ##Convert DataFrames to AWS Glue's DynamicFrames Object
        srcDYF = DynamicFrame.fromDF(srcDF, glueContext, "srcDYF")
        srcDYF.printSchema()
        print("Reading data from source is completed...")
        return srcDYF
    except Exception as e:
        print("ERROR while reading data from source: {0}".format(e))
        sys.exit(1)

def writeDataFrame(glueContext, targetDYF, rawBucket, rawS3Folder, job_src, jobName, dynamoTable, todayDateTimeFormatted):
    """
    Write data into Raw layer
    param glueContext: glue context
    param targetDYF: DynamicFrame which are contains source data
    param rawBucket: raw s3 bucket
    param rawS3Folder: raw s3 key name
    param job_src: job source
    param jobName: glue job name
    param dynamoTable: DynamoDB name
    param todayDateTimeFormatted: Calculated timestamp
    """

    try:
        print("Write data into Raw layer...")
        rawEntryCount = targetDYF.count()
        ## generating S3 location
        rawS3Location = "s3://" + rawBucket + "/" + rawS3Folder + "/" + todayDateTimeFormatted + "/"
        print("Raw S3 Location : ", rawS3Location)

        ## write data into rwa layer in specific key by generated timestamp, in snappy compression
        glueContext.write_dynamic_frame.from_options(frame=targetDYF,
            connection_options={'path': rawS3Location, "compression": "snappy"},
            connection_type='s3', format='parquet')

        ## log job details into dynamodb
        insertIntoDynamoDB(todayDateTimeFormatted, rawBucket, rawS3Folder, rawEntryCount, job_src, jobName, dynamoTable)

    except Exception as e:
        print("ERROR while writing data into s3: {0}".format(e))
        sys.exit(1)

def insertIntoDynamoDB(todayDateTimeFormatted, rawBucket, rawS3Folder, rawEntryCount, job_src, jobName, dynamoTable):
    """
    Log pipeline logs into dynamoDB
    param todayDateTimeFormatted: Calculated timestamp
    param rawBucket: raw s3 bucket
    param rawS3Folder: raw s3 key name
    param rawEntryCount: number records in loadDataFrame
    param job_src: job source
    param jobName: glue job name
    param dynamoTable: DynamoDB name
    """

    try:
        print("Writing log into dynamoDB...")
        client = boto3.resource('dynamodb')
        tbl = client.Table(dynamoTable)
        input = {'partition_key': todayDateTimeFormatted,
                 'State': 'RAW COMPLETED',
                 'RawBucket': rawBucket,
                 'RawFolder': rawS3Folder,
                 'RawJobName': jobName,
                 'RawEntryCount': rawEntryCount,
                 'job_src': job_src
                 }
        response = tbl.put_item(Item=input)
        print("Writing log into dynamoDB completed.")
    except Exception as e:
        print("ERROR while logging into DynamoDB: {0}".format(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

