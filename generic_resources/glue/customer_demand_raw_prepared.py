import sys
import boto3
import os
import json
from datetime import datetime
import time
from pytz import timezone
from boto3.dynamodb.conditions import Key

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import Join, DropFields

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, col, concat_ws, to_date, broadcast, year, month, to_timestamp


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
            'job_src',
            'configBucket',
            'configLocation',
            'environment'
        ])

        ## fetching job args
        job.init(args['JOB_NAME'], args)
        jobRunId = args['JOB_RUN_ID']
        job_src = args['job_src']
        jobName = args['JOB_NAME']
        configBucket = args['configBucket']
        configLocation = args['configLocation']
        env = args['environment']

        ## fetching configuration file from S3
        s3 = boto3.resource('s3')
        # content_object = s3.Object(configBucket, f"{configLocation}/{job_src}/config.json")
        content_object = s3.Object(configBucket, f"{configLocation}/SamplePipeline/config.json")
        file_content = content_object.get()['Body'].read().decode('utf-8')
        job_args = json.loads(file_content)

        ## fetching job configuration from config.json
        dynamoTable = job_args.get("projectConfig", {}).get("dynamoTable", None)
        preparedBucket = job_args.get("preparedLayer", {}).get(env, {}).get("preparedBucket", None)
        s3AthenaLocation = job_args.get("preparedLayer", {}).get(env, {}).get("s3AthenaLocation", None)
        rawBucket = job_args.get("preparedLayer", {}).get(env, {}).get("rawBucket", None)
        calendarDimDB = job_args.get("preparedLayer", {}).get(env, {}).get("calendarDimDB", None)
        calendarDimTable = job_args.get("preparedLayer", {}).get(env, {}).get("calendarDimTable", None)
        transformations = job_args.get("preparedLayer", {}).get("transformations", {})
        writeConfig = job_args.get("preparedLayer", {}).get("writeConfig", {})
        athenaDatabase = job_args.get("catalog", {}).get(env, {}).get("athenaDatabase", None)
        athenaCompactedDatabase = job_args.get("catalog", {}).get(env, {}).get("athenaCompactedDatabase", None)
        athenaTable = job_args.get("catalog", {}).get("athenaIncrementalTable", None)
        athenaCompactedTable = job_args.get("catalog", {}).get("athenaCompactedTable", None)
        print(transformations)

        ## Read data from dynamoDB
        items = readFromDynamoDB(dynamoTable, job_src)

        for item in items:
            ## processing raw completed items
            if (item['State'] == 'RAW COMPLETED'):
                ## fetch source data from SAP env
                srcDYF = loadDataFrame(glueContext, item)
                ## transform dataframe according document
                todayDateTimeFormatted = calculateDates()

                preparedDYF = transformDataFrame(
                    glueContext, srcDYF, jobRunId, transformations, todayDateTimeFormatted, item,
                    calendarDimDB, calendarDimTable)

                # write data into raw layer
                writeDataFrame(glueContext, writeConfig, preparedDYF, item, preparedBucket, jobName, dynamoTable,
                               athenaDatabase,
                               athenaCompactedDatabase, athenaTable, athenaCompactedTable, s3AthenaLocation)

        # lambda_client = boto3.client('lambda')
        # payload = {'bucket': 'ald-dl-data-raw-ue1-prod', 'key': 'CompareFile/config_sap_ekkn.yaml',
        #            'jobName': 'CompareSapPrepared'}
        # json_object = json.dumps(payload)
        # lambda_client.invoke(FunctionName='CallGlueCompareTable', Payload=json_object)

    except Exception as e:
        print("ERROR main process: {0}".format(e))
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
        # todayDateTimeFormatted = datetime.strftime(todayDate, '%Y%m%d%H%M%S%f')
        todayDateTimeFormatted = datetime.strftime(todayDate, '%Y%m%d') + '_' + datetime.strftime(todayDate,
                                                                                                  '%H%M%S')
        todayDateTime = datetime.strftime(todayDate, '%Y-%m-%d,%H%M%S%f')

        print("Today Date : ", todayDateTimeFormatted)
        print("Today Date", todayDateTime)
        return todayDateTimeFormatted
    except Exception as e:
        print("ERROR calculating Dates: {0}".format(e))
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
        rawDYF = glueContext.create_dynamic_frame_from_options(
            's3',
            {'paths': ['s3://' + item['RawBucket'] + '/' + item['RawFolder'] + '/' + item['partition_key'] + '/']},
            format='parquet'
        )

        print("Reading data from ray layer completed...")
        print('s3://' + item['RawBucket'] + '/' + item['RawFolder'] + '/' + item['partition_key'] + '/')
        return rawDYF
    except Exception as e:
        print("ERROR while reading data from source: {0}".format(e))
        sys.exit(1)


def transformDataFrame(
        glueContext, srcDYF, jobRunId, transformations, todayDateTimeFormatted, item,
        calendarDimDB, calendarDimTable):
    """
    Adding meta data and cdm data into dataframe
    param glueContext: glue context
    param srcDYF: DynamicFrame which are contains source data
    param jobRunId: glue job id
    param transformations: Dict contains transformation logic
    param todayDateTimeFormatted: timestamp of job run
    param calendarDimDB: Calendar dimension DB
    param calendarDimTable: Calendar dimension table
    return: transformed dynamicframe
    """

    try:
        print("Transforming dataframe...")
        rawDF = srcDYF.toDF()
        ## dispatcher for utility function
        dispatcher = {'col': col, 'lit': lit, 'concat_ws': concat_ws}

        rawDF = rawDF.withColumn("meta_datasrc", lit(item['partition_key'])) \
            .withColumn("meta_ingestdt", lit(todayDateTimeFormatted)) \
            .withColumn("meta_job_id", lit("glue_" + jobRunId))

        if "filtering" in transformations:
            for condition in transformations['filtering']:
                rawDF = rawDF.filter(f"{condition['column']} {condition['condition']} {condition['predicate']}")

        if "addColumns" in transformations:
            for condition in transformations['addColumns']:
                if condition['supportFunction'] == 'concat_ws':
                    rawDF = rawDF.withColumn(condition['columnName'],
                                             dispatcher[condition['supportFunction']](*condition['value']))
                else:
                    rawDF = rawDF.withColumn(condition['columnName'],
                                             dispatcher[condition['supportFunction']](condition['value']))

        if "renameColumns" in transformations:
            columnMapping = dict()

            for condition in transformations['renameColumns']:
                columnMapping[condition['columnName']] = condition['newColumnName']
            rawDF = rawDF.select([col(c).alias(columnMapping.get(c, c)) for c in rawDF.columns])

        if "castColumns" in transformations:
            columnMapping = dict()

            for condition in transformations['castColumns']:
                if condition['castInto'] == 'date':
                    columnMapping[
                        condition['column']] = f"to_date({condition['column']}, 'yyyyMMdd') {condition['column']}"
                else:
                    columnMapping[condition[
                        'column']] = f"cast({condition['column']} as {condition['castInto']}) {condition['column']}"

            print(columnMapping)
            rawDF = rawDF.selectExpr([columnMapping.get(c, c) for c in rawDF.columns])

        if "enrichTimeDim" in transformations:
            calendarDF = glueContext.sql("""
                            SELECT DATE(bus_day_dt) as biz_dt,{requiredColumns}  
                            FROM {DB}.{table} where bus_day_dt is not null""".format(
                requiredColumns=transformations['enrichTimeDim']['requiredColumns'],
                DB=calendarDimDB, table=calendarDimTable))

            enrichedDF = rawDF.join(broadcast(calendarDF), [transformations['enrichTimeDim']['joinOn'], 'biz_dt'],
                                    "left")
            enrichedDYF = DynamicFrame.fromDF(enrichedDF, glueContext, "enrichedDYF")
            resultDYF = DropFields.apply(frame=enrichedDYF, paths=["biz_dt"])
            rawDF = resultDYF.toDF()

        resultDYF = DynamicFrame.fromDF(rawDF, glueContext, "resultDYF")
        print("Transformtion completed...")

        return resultDYF

    except Exception as e:
        print("ERROR while transforming dataframe: {0}".format(e))
        sys.exit(1)


def writeDataFrame(glueContext, writeConfig, preparedDYF, item, preparedBucket, jobName, dynamoTable, athenaDatabase,
                   athenaCompactedDatabase, athenaTable, athenaCompactedTable, s3AthenaLocation):
    """
    Write data into Prepared layer both incremental and compacted
    param glueContext: glue context
    param preparedDYF: final dataframe after all transformation
    param item: Dictionary contains single record from dynamoDB
    param jobName: glue job name
    param dynamoTable: DynamoDB name
    param athenaDatabase: Athena database name
    param athenaTable: Athena table name
    param s3AthenaLocation: Athena result location
    """

    try:
        print("Write data into Raw layer...")
        preparedDF = preparedDYF.toDF()

        if "repartitionBy" in writeConfig:
            preparedDF = preparedDF.repartition(*writeConfig["repartitionBy"])

        preparedDYF = DynamicFrame.fromDF(preparedDF, glueContext, "preparedDYF")
        preparedEntryCount = preparedDF.count()
        preparedColsCount = len(preparedDF.columns)
        preparedS3Folder = "Incremental" + "/" + item["RawFolder"]
        RawFolder = item["RawFolder"]

        connectionOptions = {
            "path": "s3://" + preparedBucket + "/" + preparedS3Folder,
            "compression": "snappy"
        }

        if "partitionKeys" in writeConfig:
            connectionOptions["partitionKeys"] = writeConfig["partitionKeys"]

        print("Writing to S3 Incremental location")
        glueContext.write_dynamic_frame_from_options(
            frame=preparedDYF, connection_type="s3",
            connection_options=connectionOptions,
            format="parquet")

        print("Written to S3 Incremental location")

        if "hudiConfig" in writeConfig:
            print("Writing to S3 Compacted location")

            ## creating hudi options
            hudiOptions = {
                'hoodie.table.name': writeConfig["hudiConfig"]["tableName"],
                'hoodie.datasource.write.recordkey.field': writeConfig["hudiConfig"]["recordKey"],
                'hoodie.datasource.write.table.name': writeConfig["hudiConfig"]["tableName"],
                'hoodie.datasource.write.precombine.field': writeConfig["hudiConfig"]["precombineField"],
                'hoodie.datasource.write.operation': "upsert",
                'hoodie.datasource.write.table.type': "COPY_ON_WRITE",
                'hoodie.cleaner.commits.retained': 1
            }

            if "partitionKeys" in writeConfig:
                hudiOptions["hoodie.datasource.write.partitionpath.field"] = ",".join(writeConfig["partitionKeys"])
                hudiOptions["hoodie.datasource.write.hive_style_partitioning"] = "true"
                hudiOptions["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.ComplexKeyGenerator"
            else:
                hudiOptions[
                    "hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerato"

            compactedLocation = "s3://" + preparedBucket + "/" + "Compacted" + "/" + item['RawFolder']
            ## write data into compacted layer
            preparedDF.write \
                .format("org.apache.hudi") \
                .options(**hudiOptions) \
                .mode("append") \
                .save(compactedLocation)
            print("Written to S3 compacted location")

        ##Update job details in dynamodb
        # updateDynamoDB(item, preparedBucket, preparedS3Folder, preparedEntryCount, jobName, dynamoTable)

        if "partitionKeys" in writeConfig:
            ##Fetching distinct partition from prepared df
            distinctPartitions = preparedDF.select(*writeConfig["partitionKeys"]).distinct().collect()
            print("printing the distinct paritions")
            print(distinctPartitions)
            ## add athena partition
            addAthenaPartition(distinctPartitions, writeConfig["partitionKeys"], athenaDatabase, athenaCompactedDatabase, athenaTable,
                               athenaCompactedTable, compactedLocation, s3AthenaLocation)

    except Exception as e:
        print("ERROR while writing data into s3: {0}".format(e))
        sys.exit(1)


def addAthenaPartition(distinctPartitions, partitionKeys, athenaDatabase, athenaCompactedDatabase, athenaTable, athenaCompactedTable,
                       compactedLocation, s3AthenaLocation):
    """
    Reload athena partition
    param athenaDatabase: glue catalog table name
    param athenaTable: athena table name
    param athenaCompactedTable: athena compacted layer table
    param s3AthenaLocation: athena s3 query location
    """

    try:
        print("adding athena partitioning...")
        incrQueryHead = incrQuery = f"ALTER TABLE {athenaTable} ADD IF NOT EXISTS \n"
        compcatedQueryHead = compcatedQuery = f"ALTER TABLE {athenaCompactedTable} ADD IF NOT EXISTS \n"
        batchSize = 1000

        for index, partition in enumerate(distinctPartitions, start=1):
            ## building athena reload partition query
            partitionList = []
            locationList = []
            for key in partitionKeys:
                partitionList.append(f"{key}='{partition[key]}'")
                locationList.append(f"{key}={partition[key]}")

            query = f"PARTITION ({','.join(partitionList)})\n"

            ## building athena reload partition for incremental table
            incrQuery = incrQuery + query

            location = compactedLocation + "/".join(partitionList)
            ## building athena reload partition for compcated table
            compcatedQuery = compcatedQuery + query + f" LOCATION '{location}' \n"

            ## Maximum query string length (262,144 bytes), so we are triggering query based on
            ## number of add partition equals to batchSize Or the last element of partition
            if index == len(distinctPartitions) or (index % batchSize) == 0:
                incrQuery = incrQuery + ";"
                compcatedQuery = compcatedQuery + ";"
                ## partition reload on Incremental layer
                runQuery(incrQuery, athenaDatabase, s3AthenaLocation)

                ## partition reload on Compacted layer
                runQuery(compcatedQuery, athenaDatabase, s3AthenaLocation)

                ## partition reload on Compacted layer on PII database
                runQuery(compcatedQuery, athenaCompactedDatabase, s3AthenaLocation)
                print(incrQuery)
                print(compcatedQuery)

                ## reset instance varible
                incrQuery = incrQueryHead
                compcatedQuery = compcatedQueryHead
                time.sleep(60)

        print("Athena table repaired")

    except Exception as e:
        print("ERROR adding partition : {0}".format(e))
        sys.exit(1)


def runQuery(query, athenaDatabase, s3AthenaLocation, maxExecution=50):
    """
    this function dose query on athena
    param athena: Athena bot03 client object
    param query: query
    param athenaDatabase: athena database name
    param s3AthenaLocation: athena s3 query location
    """

    try:
        athena = boto3.client('athena')
        queryResponse = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': athenaDatabase},
            ResultConfiguration={'OutputLocation': s3AthenaLocation}
        )
        print('Execution ID: ' + queryResponse['QueryExecutionId'])

        ## Getting execution from response
        executionId = queryResponse['QueryExecutionId']

        state = 'QUEUED'

        ## Checking for query success
        while maxExecution > 0 and state in ['RUNNING', 'QUEUED']:
            maxExecution = maxExecution - 1
            ## checking query status
            response = athena.get_query_execution(QueryExecutionId=executionId)

            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']

            time.sleep(10)

        return queryResponse
    except Exception as e:
        print("ERROR in querying Athena DB : {0}".format(e))
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
            'FilterExpression': Key('job_src').eq(job_src) & Key('State').eq("RAW COMPLETED") & Key('RawEntryCount').gt(
                0),
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


def updateDynamoDB(item, preparedBucket, preparedS3Folder,
                   preparedEntryCount, jobName, dynamoTable):
    """
    Log pipeline logs into dynamoDB
    param item: Dictionary contains single record from dynamoDB
    param todayDateTimeFormatted: Calculated timestamp
    param preparedBucket: raw s3 bucket
    param preparedEntryCount: number records in loadDataFrame
    param jobName: glue job name
    param dynamoTable: DynamoDB name
    """

    try:
        print("Writing log into dynamoDB...")
        client = boto3.resource('dynamodb')
        tbl = client.Table(dynamoTable)
        input = {
            'partition_key': item['partition_key'],
            'State': 'PREPARED COMPLETED',
            'job_src': item['job_src'],
            'RawBucket': item['RawBucket'],
            'RawEntryCount': item['RawEntryCount'],
            'RawFolder': item['RawFolder'],
            'RawJobName': item['RawJobName'],
            'PreparedBucket': preparedBucket,
            'PreparedFolder': preparedS3Folder,
            'PreparedJobName': jobName,
            'PreparedEntryCount': preparedEntryCount}

        response = tbl.put_item(Item=input)
        print("Writing log into dynamoDB completed.")
    except Exception as e:
        print("ERROR while logging into DynamoDB: {0}".format(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
