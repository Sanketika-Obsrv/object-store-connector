import json
from typing import List

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from obsrv.common import ObsrvException
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.job.batch import get_base_conf
from obsrv.models import ErrorData
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession

from models.object_info import ObjectInfo, Tag
from provider.blob_provider import BlobProvider


class S3(BlobProvider):
    def __init__(self, connector_config) -> None:
        super().__init__()
        self.connector_config = connector_config
        self.endpoint_url = "s3.amazonaws.com"
        self.is_s3_client = True
        endpoint = connector_config.get("source_credentials_endpoint", None)
        if endpoint is not None:
            self.endpoint_url = f"http://{connector_config["source_credentials_endpoint"]}"
            self.is_s3_client = False
        self.bucket = connector_config["source_bucket"]
        # self.prefix = connector_config.get('prefix', '/') # TODO: Implement partitioning support
        self.prefix = (
            connector_config["source_prefix"]
            if "source_prefix" in connector_config
            else ""
        )
        self.obj_prefix = f"s3a://{self.bucket}/"
        self.s3_client = self._get_client()

    def get_spark_config(self, connector_config) -> SparkConf:
        conf = get_base_conf()
        conf.setAppName("AWSObjectStoreConnector")
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        conf.set(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )  # Use S3A file system
        conf.set(
            "spark.hadoop.fs.s3a.connection.maximum", "100"
        )  # Set maximum S3 connections
        conf.set(
            "spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer"
        )  # Use bytebuffer for fast upload buffer
        conf.set(
            "spark.hadoop.fs.s3a.fast.upload.active.blocks", "4"
        )  # Set number of active blocks for fast upload
        conf.set(
            "spark.hadoop.fs.s3a.fast.upload.buffer.size", "33554432"
        )  # Set buffer size for fast upload
        conf.set(
            "spark.hadoop.fs.s3a.retry.limit", "20"
        )  # Set retry limit for S3 operations
        conf.set(
            "spark.hadoop.fs.s3a.retry.interval", "500ms"
        )  # Set retry interval for S3 operations
        conf.set("spark.hadoop.fs.s3a.endpoint", self.endpoint_url)
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        conf.set(
            "spark.hadoop.fs.s3a.multiobjectdelete.enable", "false"
        )  # Disable multiobject delete
        conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )  # Use simple AWS credentials provider
        conf.set(
            "spark.hadoop.fs.s3a.access.key",
            connector_config["source_credentials_access_key"],
        )  # AWS access key
        conf.set(
            "spark.hadoop.fs.s3a.secret.key",
            connector_config["source_credentials_secret_key"],
        )  # AWS secret key
        conf.set("com.amazonaws.services.s3.enableV4", "true")  # Enable V4 signature

        return conf

    def fetch_tags(
        self, object_path: str, metrics_collector: MetricsCollector
    ) -> List[Tag]:
        bucket_name = self.bucket
        error_code = ""
        fetched_tags = []
        api_calls, errors = 0, 0
        try:
            tags_response = self.s3_client.get_object_tagging(
                Bucket=bucket_name, Key=object_path
            )
            api_calls += 1
            tags = tags_response.get("TagSet", [])
            fetched_tags = [Tag(tag["Key"], tag["Value"]) for tag in tags]
        except (BotoCoreError, ClientError) as exception:
            errors += 1
            error_code = str(exception.response["Error"]["Code"])
            ObsrvException(
                ErrorData(
                    "S3_TAG_READ_ERROR",
                    f"failed to fetch tags from S3: {str(exception)}",
                )
            )
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getObjectTagging"},
            {"key": "object_path", "value": object_path},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return fetched_tags

    def update_tag(
        self, object: ObjectInfo, tags: list, metrics_collector: MetricsCollector
    ) -> bool:
        api_calls, errors = 0, 0
        error_code = ""
        is_tag_updated = False
        bucket_name = self.bucket
        object_key = object.get("key")

        initial_tags = object.get("tags")
        new_tags = list(
            json.loads(t) for t in set([json.dumps(t) for t in initial_tags + tags])
        )
        updated_tags = [
            Tag(tag.get("key"), tag.get("value")).to_aws() for tag in new_tags
        ]

        try:
            self.s3_client.put_object_tagging(
                Bucket=bucket_name, Key=object_key, Tagging={"TagSet": updated_tags}
            )
            api_calls += 1
            is_tag_updated = True
        except (BotoCoreError, ClientError) as exception:
            errors += 1
            error_code = str(exception.response["Error"]["Code"])
            ObsrvException(
                ErrorData(
                    "S3_TAG_UPDATE_ERROR",
                    f"failed to update tags in S3 for object: {str(exception)}",
                )
            )

        labels = [
            {"key": "request_method", "value": "PUT"},
            {"key": "method_name", "value": "setObjectTagging"},
            {"key": "object_path", "value": object.get("location")},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return is_tag_updated

    def fetch_objects(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector, prefix: str = None
    ) -> List[ObjectInfo]:
        if prefix is None:
            prefix = self.prefix
        objects = self._list_objects(prefix=prefix, ctx=ctx, metrics_collector=metrics_collector)
        objects_info = []
        for obj in objects:
            object_info = ObjectInfo(
                location=f"{self.obj_prefix}{obj['Key']}",
                key=obj["Key"],
                format=obj["Key"].split(".")[-1],
                file_size_kb=obj["Size"] // 1024,
                file_hash=obj["ETag"].strip('"'),
                tags=self.fetch_tags(obj["Key"], metrics_collector),
            )
            objects_info.append(object_info.to_json())
        return objects_info

    def read_object(
        self,
        object_path: str,
        sc: SparkSession,
        metrics_collector: MetricsCollector,
        file_format: str,
    ) -> DataFrame:
        api_calls, errors, records_count = 0, 0, 0
        error_code = ""
        df = None
        try:
            df = super().read_file(
                objectPath=object_path,
                sc=sc,
                metrics_collector=metrics_collector,
                file_format=file_format,
            )
            records_count = df.count()
            api_calls += 1
        except (BotoCoreError, ClientError) as exception:
            errors += 1
            error_code = str(exception.response["Error"]["Code"])
            ObsrvException(
                ErrorData(
                    "S3_READ_ERROR", f"failed to read object from S3: {str(exception)}"
                )
            )
        except Exception as exception:
            errors += 1
            error_code = "S3_READ_ERROR"
            ObsrvException(
                ErrorData(
                    "S3_READ_ERROR", f"failed to read object from S3: {str(exception)}"
                )
            )

        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getObject"},
            {"key": "object_path", "value": object_path},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return df

    def _get_client(self):
        session = boto3.Session(
            aws_access_key_id=self.connector_config["source_credentials_access_key"],
            aws_secret_access_key=self.connector_config[
                "source_credentials_secret_key"
            ],
            region_name=self.connector_config["source_credentials_region"],
        )
        if self.is_s3_client is True:
            return session.client("s3")
        return session.client("s3", endpoint_url=self.endpoint_url)

    def _list_objects(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector, prefix: str
    ) -> list:
        bucket_name = self.bucket
        summaries = []
        continuation_token = None
        file_formats = {
            "json": ["json", "json.gz", "json.zip"],
            "jsonl": ["json", "json.gz", "json.zip"],
            "csv": ["csv", "csv.gz", "csv.zip"],
            "parquet": ["parquet", "parquet.gz", "parquet.zip"],
        }
        file_format = self.connector_config["source_data_format"]
        # metrics
        api_calls, errors = 0, 0
        error_code = ""

        while True:
            try:
                if continuation_token:
                    objects = self.s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=prefix,
                        ContinuationToken=continuation_token,
                    )
                else:
                    objects = self.s3_client.list_objects_v2(
                        Bucket=bucket_name, Prefix=prefix
                    )
                api_calls += 1
                if objects.get("Contents") is None:
                    break
                for obj in objects["Contents"]:
                    if file_format in file_formats and any(obj["Key"].endswith(f) for f in file_formats[file_format]):
                        summaries.append(obj)
                if not objects.get("IsTruncated"):
                    break
                continuation_token = objects.get("NextContinuationToken")
            except (BotoCoreError, ClientError) as exception:
                errors += 1
                error_code = str(exception.response["Error"]["Code"])
                ObsrvException(
                    ErrorData(
                        "AWS_S3_LIST_ERROR",
                        f"failed to list objects in S3: {str(exception)}",
                    )
                )
                break

        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "ListObjectsV2"},
            {"key": "object_path", "value": ""},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return summaries

    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()
