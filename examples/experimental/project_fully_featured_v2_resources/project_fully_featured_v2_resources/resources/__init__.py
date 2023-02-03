import os

from dagster._utils import file_relative_path
from dagster_aws.s3 import s3_resource
from dagster_dbt import dbt_cli_resource
from dagster_pyspark import pyspark_resource

from .common_bucket_s3_pickle_io_manager import CommonBucketS3PickleIOManager
from .duckdb_parquet_io_manager import DuckDBPartitionedParquetIOManager
from .hn_resource import HNAPIClient, HNAPISubsampleClient
from .parquet_io_manager import LocalPartitionedParquetIOManager, S3PartitionedParquetIOManager
from .snowflake_io_manager import SnowflakeIOManager

DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"
dbt_local_resource = dbt_cli_resource.configured(
    {"profiles_dir": DBT_PROFILES_DIR, "project_dir": DBT_PROJECT_DIR, "target": "local"}
)
dbt_staging_resource = dbt_cli_resource.configured(
    {"profiles-dir": DBT_PROFILES_DIR, "project-dir": DBT_PROJECT_DIR, "target": "staging"}
)
dbt_prod_resource = dbt_cli_resource.configured(
    {"profiles_dir": DBT_PROFILES_DIR, "project_dir": DBT_PROJECT_DIR, "target": "prod"}
)


configured_pyspark = pyspark_resource.configured(
    {
        "spark_conf": {
            "spark.jars.packages": ",".join(
                [
                    "net.snowflake:snowflake-jdbc:3.8.0",
                    "net.snowflake:spark-snowflake_2.12:2.8.2-spark_3.0",
                    "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7",
                ]
            ),
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
            "spark.hadoop.fs.s3.awsAccessKeyId": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3.awsSecretAccessKey": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "spark.hadoop.fs.s3.buffer.dir": "/tmp",
        }
    }
)

SHARED_SNOWFLAKE_CONF = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": "TINY_WAREHOUSE",
}

RESOURCES_PROD = {
    "io_manager": CommonBucketS3PickleIOManager(
        s3=s3_resource, s3_bucket="hackernews-elementl-prod"
    ),
    "parquet_io_manager": S3PartitionedParquetIOManager(
        pyspark=configured_pyspark, s3_bucket="hackernews-elementl-dev"
    ),
    "warehouse_io_manager": SnowflakeIOManager(database="DEMO_DB", **SHARED_SNOWFLAKE_CONF),
    "hn_client": HNAPISubsampleClient(subsample_rate=10),
    "dbt": dbt_prod_resource,
}


RESOURCES_STAGING = {
    "io_manager": CommonBucketS3PickleIOManager(
        s3=s3_resource, s3_bucket="hackernews-elementl-dev"
    ),
    "parquet_io_manager": S3PartitionedParquetIOManager(
        pyspark=configured_pyspark, s3_bucket="hackernews-elementl-dev"
    ),
    "warehouse_io_manager": SnowflakeIOManager(database="DEMO_DB_STAGING", **SHARED_SNOWFLAKE_CONF),
    "hn_client": HNAPISubsampleClient(subsample_rate=10),
    "dbt": dbt_staging_resource,
}


RESOURCES_LOCAL = {
    "parquet_io_manager": LocalPartitionedParquetIOManager(pyspark=configured_pyspark),
    "warehouse_io_manager": DuckDBPartitionedParquetIOManager(
        duckdb_path=os.path.join(DBT_PROJECT_DIR, "hackernews.duckdb")
    ),
    "hn_client": HNAPIClient(),
    "dbt": dbt_local_resource,
}
