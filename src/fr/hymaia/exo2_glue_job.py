import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PARAM_1", "PARAM_2"])
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    param1 = args["PARAM_1"]
    param2 = args["PARAM_2"]
    print(f"Param1 = {param1}")
    print(f"Param2 = {param2}")

    # Votre code Spark ici...
    job.commit()