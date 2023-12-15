import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job
from pyspark.context import SparkContext
from datetime import datetime, timedelta
glue_client = boto3.client('glue')
sc = SparkContext()
sc._conf.set("spark.driver.maxResultSize", "16g")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket_name = "adh-bag-comparison-scripts-us-east-1-039628302096"

def delete_folders_s3(bucket_name, prefix):
    try:
      s3_client = boto3.client('s3')
      objects = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
      if 'Contents' in objects:
          objects_to_delete = [{'Key': obj['Key']} for obj in objects['Contents']]
          s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
    except Exception as e:
        print(e)
        raise e


def is_in_range(upserttime):
  utc_time = datetime.strptime(upserttime, "%Y-%m-%dT%H:%M:%S.%fZ")
  yesterday = datetime.utcnow().date() - timedelta(days=1)
  start_of_day = datetime(yesterday.year, yesterday.month, yesterday.day)
  end_of_day = start_of_day + timedelta(days=1)
  return start_of_day <= utc_time < end_of_day


def filter_function(record):
  timestamp = record["upsertTimestamp"]
  if is_in_range(timestamp):
    print(timestamp)
    return True
  else: 
    return False

# Filter the DynamicFrame
mapping = [
    ("sk","string", "sk","string"),
    ("pk","string", "pk","string"),
    ("scanResponse","string", "scanResponse","string"),
    ("upsertTimestamp","string", "upsertTimestamp","string"),
    ("scanRequest","string", "scanRequest","string"),
    ("bmRequest", "string", "bmRequest", "string"),
    ("bmResponse", "string", "bmResponse", "string"),
    ("bmPayload", "string", "bmPayload", "string"),
    ("cepPayload", "string", "cepPayload", "string")
]


try:
    # deleting 
    paths_to_delete = ["temporary/", "dynamo_data/", "bmResponse.parquet/", "scanResponse.parquet/"]
    for path in paths_to_delete:
        delete_folders_s3(bucket_name,path)

    AmazonDynamoDB_node1701918537261 = glueContext.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.export": "ddb",
            "dynamodb.s3.bucket": bucket_name,
            "dynamodb.s3.prefix": "temporary/ddbexport/",
            "dynamodb.tableArn": "arn:aws:dynamodb:us-east-1:039628302096:table/bbs-Comparison",
            "dynamodb.unnestDDBJson": True,
        },
        transformation_ctx="AmazonDynamoDB_node1701918537261",
    )

       
    filter = Filter.apply(frame = AmazonDynamoDB_node1701918537261, f = filter_function, transformation_ctx = "filter")
    applymapping2 = ApplyMapping.apply(frame = filter, mappings=mapping, transformation_ctx = "applymapping2")
    
    
    AmazonS3_node1701138180579 = glueContext.write_dynamic_frame.from_options(
        frame=applymapping2,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": f"s3://{bucket_name}/dynamo_data/", "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="AmazonS3_node1701138180579",
    )
  # Invoke Glue Jobs
    bm_cep_response = glue_client.start_job_run(JobName="scan_bm_response_conversion")
    payload_response = glue_client.start_job_run(JobName="cep_bm_payload_conversion")

    pandas_df = applymapping2.toDF.toPandas()
    pandas_df.to_csv(f"s3://{bucket_name}/dynamo_csv_data.csv", index=False)


except Exception as e:
  print(e)
job.commit()

