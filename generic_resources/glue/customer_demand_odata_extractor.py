# ------------------------
# All Imports
# ------------------------
import boto3
import requests
from requests.auth import HTTPBasicAuth
import json
import os
import traceback
import copy
import uuid
import sys
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pytz import timezone
import pandas as pd
import awswrangler as wr

# ------------------------
# All Constants
# ------------------------
INITLOADING = "InitLoading"
INITLOADED = "InitLoaded"
DELTALOADING = "DeltaLoading"
DELTATOKEN = "!deltatoken="

# ------------------------
# Globals
# ------------------------
sapHostName = ""
sapPort = ""
sapUser = ""
sapPassword = ""
odpServiceName = ""
odpEntitySetName = ""
dataChunkSize = ""
metaDataDDBName = ""
dataS3Bucket = ""
dataS3Folder = ""
selfSignedCertificate = ""
selfSignedCertificateS3Bucket = ""
selfSignedCertificateS3Key = ""
reLoad = False
_allowInValidCerts = False
sapClientId = ""
numberOfRecords = ""
partitionKey = ""
nextLoading = True
inventoryMetaPath = ''
dataSubject = ''
sequenceToken = ''
logStream = ''
logGroup = ''
isDeltaEnabledOdata = "YES"


def pushErrorMessage(message):
    """
    Send error notification to log group
    """
    try:
        global sequenceToken, logGroup, logStream
        ## initializing boto3 log client
        logs = boto3.client('logs')

        ## Create log stream if not exist
        if not logStream:
            logStreamName = "run_time_" + partitionKey
            logs.create_log_stream(logGroupName=logGroup, logStreamName=logStreamName)
            logStream = logStreamName

        todayDate = datetime.utcnow()
        timestamp = int(datetime.timestamp(todayDate) * 1000)
        ## logging configuration
        kwargs = {'logGroupName': logGroup,
                  'logStreamName': logStream,
                  'logEvents': [
                      {
                          'timestamp': timestamp,
                          'message': datetime.strftime(todayDate, '%Y-%m-%d %H:%M:%S') + '\tERROR\t' + str(message)
                      },
                  ]}

        ## Attaching sequence token, if it exist
        if sequenceToken:
            kwargs.update({'sequenceToken': sequenceToken})

        response = logs.put_log_events(**kwargs)

        ## checking for next sequence token
        if 'nextSequenceToken' in response:
            sequenceToken = response['nextSequenceToken']
    except Exception as e:
        print("ERROR while logging message into log group: {0}".format(e))
        sys.exit(1)


def getDateTime():
    """
    Get System Date and Timestamp for creating S3 partition
    """
    global partitionKey
    try:
        est = timezone('EST')
        todayDate = datetime.now(est)
        partitionKey = datetime.strftime(todayDate, '%Y%m%d%H%M%S%f')
    except Exception as e:
        print("ERROR initializing the spark and glue variables: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def insertMetadataRawPreparedPipeline(job_src):
    try:
        client = boto3.resource('dynamodb')
        tbl = client.Table('metadata_raw_prepared_pipeline')
        input = {'partition_key': partitionKey,
                 'State': 'RAW COMPLETED',
                 'RawBucket': dataS3Bucket,
                 'RawFolder': dataS3Folder,
                 'RawJobName': 'inventory_raw_odata_extractor',
                 'RawEntryCount': 0,
                 'job_src': job_src
                 }
        response = tbl.put_item(Item=input)
        return response
    except Exception as e:
        print("Error creating records in dynamo table: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def metadataRawPreparedPipeline(numberOfRecords, job_src):
    """
    Function to Set DDB after job completion
    param numberOfRecords: number of records
    param job_src: Job source
    """

    try:
        client = boto3.resource('dynamodb')
        tbl = client.Table('metadata_raw_prepared_pipeline')
        response = tbl.update_item(
            Key={
                'partition_key': partitionKey
            },
            UpdateExpression="set RawEntryCount =  RawEntryCount + :count",
            ConditionExpression="job_src = :jobsrc",
            ExpressionAttributeValues={
                ':count': numberOfRecords,
                ':jobsrc': job_src
            },
            ReturnValues="UPDATED_NEW"
        )

        return response
    except Exception as e:
        print("Error pushing into dynamo table: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def runPreparedJob(jobName, job_src):
    """
    Function to Run Prepared Glue Job
    """
    try:
        client = boto3.client('glue')
        arguments = {
            "--job_src": job_src
        }

        response: object = client.start_job_run(JobName=jobName, Arguments=arguments)
        return response
    except Exception as e:
        print("ERROR while triggering prepared layer job: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def _setResponse(success, message, data, numberOfRecords, numberOfCols=0):
    """
    Initialize the response
    param success: response
    param message: response message
    param data: data
    param numberOfRecords: number of records
    """
    response = {
        'success': success,
        'message': message,
        'traceback': traceback.format_exc(),
        'data': data,
        'numberOfRecords': numberOfRecords,
        'numberOfCols': numberOfCols
    }
    return response


# ------------------------
# Initializing dynamodb client
# ------------------------
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table(metaDataDDBName)
response = _setResponse(False, "ERROR in fetching data from SAP. Check detailed logs", None, 0)


def extract():
    """
    Main Extract entry point
    """
    global response, nextLoading

    try:

        if reLoad == True:
            _extract(" ", True)
        else:
            metadata = _getMetaData()

            if metadata is None:
                print("inside meta none")
                _extract(" ", True)
            else:
                status = metadata.pop('status', " ")
                numRecords = metadata.get('numRecords')

                if status == INITLOADED or status == DELTALOADING:
                    print("inside initloaded")
                    if isDeltaEnabledOdata == "NO":
                        print("Data is already loaded")
                        response = _setResponse(True, "Data is already loaded and this OData server doesn't have delta load", [], 0)
                        nextLoading = False
                    elif not metadata.get('next'):
                        print("if not metadata.get('next')")
                        delta = metadata.get('delta', " ")
                        print("Delta is: ", delta)
                        print("Calling: _extract(metadata.pop('delta',' '),False)")
                        _extract(delta, False, numRecords, status)
                    elif metadata.get('next'):
                        next = metadata.get('next')
                        print("DELTA has NEXT and SKIPTOKEN...")
                        print("metadata.get('next')")
                        print(next)
                        print("Calling: _extract(metadata.pop('next',' '),False)")
                        _extract(next, False, numRecords, status)

                else:
                    print("inside else")
                    if metadata.get('next'):
                        _extract(metadata.pop('next', " "), True, numRecords, status)

    except Exception as e:
        response = _setResponse(False, str(e), None, 0)
        print("ERROR: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)

    return response


def _extract(link, isInit, numRecords=0, PreviousStatus=''):
    """
    Function implementation main extract logic
    param link: SAP HTTP endpoint
    param isInit: whether is init load or not
    """

    global response, nextLoading
    print("inside extract **")

    try:
        url = link

        if url == " ":
            url = _getBaseUrl() + "/" + odpEntitySetName + "?$format=json" + "&sap-client=" + sapClientId

        ## header for http request
        headers = {
            "prefer": "odata.maxpagesize=" + dataChunkSize + ",odata.track-changes"
        }

        print("created url: ", url)
        sapResponse = _makeHttpCallToSap(url, headers)
        sapResponseBody = json.loads(sapResponse.text)
        _response = copy.deepcopy(sapResponseBody)
        d = sapResponseBody.pop('d', None)

        # print(d)
        print("after http extract **")
        results = d.pop('results', None)

        for result in results:
            _metadata = result.pop('__metadata', None)

        numRecords = numRecords + len(results)
        ## write result into S3
        fileName = writeIntoS3(results, _response)

        if d.get('__next'):
            next = d.get('__next')

            if PreviousStatus == '':
                status = INITLOADING
            elif PreviousStatus == INITLOADED:
                status = DELTALOADING
            else:
                status = PreviousStatus
            print("next token:", next)
            _modifyDdbTable(status, next, "", numRecords)
            nextLoading = True

        elif d.get('__delta'):
            print("Data has DELTA: ")
            delta = d.get('__delta')

            if PreviousStatus == INITLOADING:
                status = INITLOADED
            else:
                status = DELTALOADING
            print("delta token:", delta)
            _modifyDdbTable(status, "", delta, numRecords)
            nextLoading = False
        else:
            print("invalid delta token, creating delta token manually")
            if PreviousStatus == INITLOADING:
                status = INITLOADED
            else:
                status = DELTALOADING

            ## generating delta token
            ## checking for the OData having delta load
            if isDeltaEnabledOdata == "YES":
                delta = _geDeltaLink()
                print("manually created delta token:", delta)
            else:
                delta = ""
                print("manually created delta token:", delta)

            _modifyDdbTable(status, "", delta, numRecords)
            nextLoading = False

        ## update meta data pipeline
        metadataRawPreparedPipeline(len(results), job_src)
    except Exception as e:
        print("number of records:", numRecords)
        print("ERROR while extracting data: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def writeIntoS3(extractedData, APIResponse):
    """
    Function handling writing logic
    param link: SAP HTTP endpoint
    param isInit: whether is init load or not
    """

    global response

    try:
        fileName = None

        if len(extractedData) <= 0:
            print("\n zero records")
            response = _setResponse(True, "No data available to extract from SAP", APIResponse, 0)
        elif (dataS3Bucket != ""):
            print("\n inside s3 bucket........")

            ## creating dataframe from response
            dataframe = pd.DataFrame(extractedData)
            ## Add ETL Partkey
            dataframe['ETL_PART_KEY'] = partitionKey
            est = timezone('EST')
            todayDate = datetime.now(est)
            keyName = datetime.strftime(todayDate, '%Y%m%d%H%M%S%f')

            fileName = ''.join([
                's3://', dataS3Bucket, '/', dataS3Folder, '/',
                partitionKey, '/', keyName, ".parquet"])

            ## writing to S3
            wr.s3.to_parquet(
                df=dataframe,
                path=fileName,
                compression='snappy'
            )

            response = _setResponse(True, "Data successfully extracted and stored in S3 Bucket with key " + fileName,
                                    None, len(extractedData), dataframe.shape[1])
        else:
            response = _setResponse(True, "Data successfully extracted from SAP", APIResponse, len(extractedData))

        return fileName
    except Exception as e:
        print("ERROR while writing into s3: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def _getBaseUrl():
    """
    Get base url for HTTP calls to SAP
    """
    global sapPort
    if sapPort == "":
        sapPort = "443"
    return "https://" + sapHostName + ":" + sapPort + "/sap/opu/odata/SAP/" + odpServiceName


def _makeHttpCallToSap(url, headers):
    """
    Call SAP HTTP endpoint
    param url: HTTP endpoint SAP ODATA
    param headers: SAP request header
    """

    try:
        # global selfSignedCertificate
        certFileName = os.path.join('/tmp/', 'sapcert.crt')
        verify = True

        if selfSignedCertificate != "":
            certfile = open(certFileName, 'w')
            os.write(certfile, selfSignedCertificate)
            verify = certFileName

        elif selfSignedCertificateS3Bucket != "":
            s3 = boto3.client('s3')
            verify = certFileName

            with open(certFileName, 'w') as f:
                s3.download_fileobj(selfSignedCertificateS3Bucket, selfSignedCertificateS3Key, f)

            certfile = open(certFileName, 'r')
            print(certfile.read())

        elif _allowInValidCerts == True:
            verify = False

        return requests.get(url, auth=HTTPBasicAuth(sapUser, sapPassword), headers=headers, verify=verify)
    except Exception as e:
        print("ERROR while making http request: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def _getMetaData():
    """
    Get metadata from DynamoDB
    """
    try:
        ddbResponse = table.get_item(
            TableName=metaDataDDBName,
            Key={
                'odpServiceName': odpServiceName,
                'odpEntitySetName': odpEntitySetName
            },
            ConsistentRead=True
        )

        if 'Item' in ddbResponse:
            return ddbResponse['Item']
        else:
            return None

    except Exception as e:
        print("ERROR while getting metadata from dynamoDB: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def _modifyDdbTable(status, next, delta, numRecords):
    """
    Insert/Update records in DynamoDB
    """

    try:
        table.put_item(
            TableName=metaDataDDBName,
            Item={
                'odpServiceName': odpServiceName,
                'odpEntitySetName': odpEntitySetName,
                'status': status,
                'next': next,
                'delta': delta,
                'numRecords': numRecords,
            }
        )
    except Exception as e:
        print("ERROR while modifying dynamodb: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def _geDeltaLink():
    try:
        url = _getBaseUrl() + "/DeltaLinksOf" + odpEntitySetName + "?$format=json" + "&sap-client=" + sapClientId
        sapresponse = _makeHttpCallToSap(url, None)
        sapresponsebody = json.loads(sapresponse.text)
        d = sapresponsebody.pop('d', None)
        results = d.pop('results', None)
        deltauri = ""

        if len(results) > 0:
            result = results[-1]
            print(results)

            if result.get('DeltaToken'):
                ## fetch delta token
                deltaToken = result.get('DeltaToken')
                ## creating delta url
                deltauri = _getBaseUrl() + "/" + odpEntitySetName + "?$format=json" + "&sap-client=" + sapClientId + f"&!deltatoken='{deltaToken}'"

            else:
                print("The response doesn't have delta token")

        return deltauri
    except Exception as e:
        print("ERROR while extracting delta token: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


def main():
    """
    execute program - consider creating a main method or move this to extract
    """

    global sapHostName, sapPort, sapUser, sapPassword, metaDataDDBName, odpServiceName, odpEntitySetName, dataChunkSize, dataS3Bucket
    global dataS3Folder, sapClientId, job_src, _allowInValidCerts, logGroup, nextLoading, inventoryMetaPath, isDeltaEnabledOdata, dataSubject

    try:
        # ------------------------
        # get Arguments
        # ------------------------
        args = getResolvedOptions(
            sys.argv,
            ['sapHostName', 'sapPort', 'odpServiceName', 'odpEntitySetName','dataChunkSize', 'metaDataDDBName',
             'dataS3Bucket', 'sapAuthSecret', 'sapClientId', 'dataS3Folder', 'job_src', 'LogGroup', 'PreparedJobName',
             'isDeltaEnabledOdata', 'invokePreparedLayerJob'])

        sm = boto3.client('secretsmanager')
        secretResponse = sm.get_secret_value(SecretId=args['sapAuthSecret'])
        sapAuth = json.loads(secretResponse['SecretString'])
        sapHostName = args['sapHostName']
        sapPort = args['sapPort']
        sapUser = sapAuth['user']
        sapPassword = sapAuth['password']
        metaDataDDBName = args['metaDataDDBName']
        odpServiceName = args['odpServiceName']
        odpEntitySetName = args['odpEntitySetName']
        dataChunkSize = args['dataChunkSize']
        dataS3Bucket = args['dataS3Bucket']
        dataS3Folder = args['dataS3Folder']
        sapClientId = args['sapClientId']
        job_src = args['job_src']
        logGroup = args['LogGroup']
        _allowInValidCerts = True  ## Don't ever do this in production
        totalNumberOfRecords = 0
        preparedJobName = args['PreparedJobName']
        isDeltaEnabledOdata = args['isDeltaEnabledOdata']
        invokePreparedLayerJob = args['invokePreparedLayerJob']
        # ------------------------
        # Start execution
        # ------------------------
        getDateTime()
        ## initialize dynamo record
        insertMetadataRawPreparedPipeline(job_src)

        while nextLoading:
            extract()
            print("\nWriting Data at: s3://" + dataS3Bucket + "/" + dataS3Folder + "/")
            totalNumberOfRecords = totalNumberOfRecords + response['numberOfRecords']

        if totalNumberOfRecords > 0 and invokePreparedLayerJob == 'yes':
            runPreparedJob(preparedJobName, job_src)

        print("\njob completed")
    except Exception as e:
        print("ERROR while main function: {0}".format(e))
        pushErrorMessage(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
