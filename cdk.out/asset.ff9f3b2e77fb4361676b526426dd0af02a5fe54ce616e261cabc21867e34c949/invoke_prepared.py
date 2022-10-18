import boto3
import logging
import os
import json


def handler(event, context):

  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logging.basicConfig(level=logging.INFO, format=('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

  logger.info("{} function invoked".format(context.function_name))
  logger.info(f"Event :- {event}")

  try:
      glue = boto3.client('glue')
      sts = boto3.client("sts")

      for item in event['Records']:
          if(item['eventName'] == "INSERT"):
              tmp = item['dynamodb']['NewImage']['job_src']
              job_src = tmp.get('S')

              response = glue.start_job_run(
                    JobName= "prepared_layer_job",
                      Arguments={
                          '--job_src': job_src
                      }
                  )

  except Exception as e:
      logger.error('Error while invoking {}'.format(context.function_name), exc_info=True)
      raise e

  return {
      'statusCode': 200,
      'body': json.dumps('Hello from Lambda!')
  }