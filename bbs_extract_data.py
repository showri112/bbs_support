import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job
from pyspark.context import SparkContext
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


try:
    # deleting existing folders
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

       
    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    

    SqlQuery0 = """
      SELECT *
          FROM myDataSource
          WHERE CAST(upsertTimestamp AS TIMESTAMP) BETWEEN 
              CAST(DATE_SUB(CURRENT_DATE(), 1) AS TIMESTAMP) AND 
              CAST(DATE_SUB(CURRENT_DATE(), 1) AS TIMESTAMP) + INTERVAL 1 DAY
        """

    SQLQuery_node1702922733035 = sparkSqlQuery(
        glueContext,
        query=SqlQuery0,
        mapping={"myDataSource": AmazonDynamoDB_node1701918537261},
        transformation_ctx="SQLQuery_node1702922733035",
    )
    
    AmazonS3_node1701138180579 = glueContext.write_dynamic_frame.from_options(
        frame=SQLQuery_node1702922733035,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": f"s3://{bucket_name}/dynamo_data/", "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="AmazonS3_node1701138180579",
    )
    
  # Invoke Glue Jobs
    bm_cep_response = glue_client.start_job_run(JobName="scan_bm_response_conversion")
    payload_response = glue_client.start_job_run(JobName="cep_bm_payload_conversion")


except Exception as e:
  print(e)
job.commit()

