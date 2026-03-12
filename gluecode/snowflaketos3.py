#snowflake to s3 script using glue

import sys

from awsglue.transforms import *

from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from awsglue.context import GlueContext

from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled

DEFAULT_DATA_QUALITY_RULESET = """

   Rules = [

       ColumnCount > 0

   ]

"""

# Script generated for node Snowflake

Snowflake_node1768493518604 = glueContext.create_dynamic_frame.from_options(connection_type="snowflake", connection_options={"autopushdown": "on", "dbtable": "COVID", "connectionName": "Snowflake connection", "sfDatabase": "TSPARKDB", "sfSchema": "DBO"}, transformation_ctx="Snowflake_node1768493518604")


# Reduce to single partition

df = Snowflake_node1768493518604.toDF().coalesce(1)

# Convert back to DynamicFrame

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
# Script generated for node Amazon S3
# EvaluateDataQuality().process_rows(frame=Snowflake_node1768493518604, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768493496559", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1768493541423 = glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3", format="csv", connection_options={"path": "s3://snowflake-tspark-s3-bucket/rawdata/snowflake/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1768493541423")
job.commit()
 