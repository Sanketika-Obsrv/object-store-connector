import os
import json
from typing import List, Iterator
from uuid import uuid4

from google.oauth2 import service_account
from google.cloud import storage
from google.api_core.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession
from pyspark.conf import SparkConf

from obsrv.common import ObsrvException
from obsrv.connector import MetricsCollector, ConnectorContext
from obsrv.job.batch import get_base_conf
from obsrv.models import ErrorData

from provider.blob_provider import BlobProvider
from models.object_info import Tag, ObjectInfo


class GCS(BlobProvider):
    def __init__(self, connector_config) -> None:
        super().__init__()
        self.connector_config = connector_config
        # self.credentials = {k[19:]: v for k, v in self.connector_config.items() if "source_credentials" in k}
        self.credentials = json.loads(connector_config["source_credentials"])
        self.key_path = "/tmp/key.json"
        with open(self.key_path, "w") as f:
            f.write(json.dumps(self.credentials))

        self.bucket = connector_config["source_bucket"]
        self.prefix = connector_config.get("source_prefix", "")
        self.obj_prefix = f"gs://{self.bucket}"
        self.gcs_client = self._get_client()

    def _get_client(self) -> storage.Client:
        client = storage.Client(
            credentials=service_account.Credentials.from_service_account_info(self.credentials),
        )
        return client

    def get_spark_config(self, connector_config) -> SparkConf:
        conf = get_base_conf()
        conf.setAppName("GCSObjectStoreConnector")
        # conf.set("spark.jars", os.path.join(os.path.dirname(__file__), "lib/gcs-connector-hadoop3-2.2.22-shaded.jar"))
        conf.set("spark.jars", "libs/gcs-connector-hadoop3-2.2.22-shaded.jar")
        conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", self.key_path)
        conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        return conf

    def fetch_tags(
        self, object_path: str, metrics_collector: MetricsCollector
    ) -> List[Tag]:
        bucket_name = self.bucket
        error_code = ""
        fetched_tags = []
        api_calls, errors = 0, 0

        try:
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.get_blob(object_path)
            if blob is None:
                return []
            blob.reload()
            api_calls += 1
            metadata = blob.metadata
            if metadata:
                fetched_tags = [Tag(key, value) for key, value in metadata.items()]
        except ClientError as exception:
            errors += 1
            error_code = exception.response.json()["error"]["code"]
            ObsrvException(
                ErrorData(
                    "GCS_TAG_READ_ERROR",
                    f"failed to fetch tags from GCS: {str(exception)}",
                )
            )

        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getBlobMetadata"},
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
        # object_path = object.get('location')

        initial_tags = object.get('tags')
        new_tags = list(
            json.loads(t) for t in set([json.dumps(t) for t in initial_tags + tags])
        )
        updated_tags = [
            Tag(tag.get("key"), tag.get("value")).to_gcs() for tag in new_tags
        ]

        try:
            # relative_path = object_path.lstrip("gs://").split("/", 1)[-1]
            bucket = self.gcs_client.bucket(self.bucket)
            blob = bucket.blob(object.get("key"))
            blob.metadata = {tag["Key"]: tag["Value"] for tag in updated_tags}
            blob.patch()
            api_calls += 1
            is_tag_updated = True
        except ClientError as exception:
            errors += 1
            error_code = exception.response.json()["error"]["code"]
            ObsrvException(
                ErrorData(
                    "GCS_TAG_UPDATE_ERROR",
                    f"failed to update tags in GCS for object: {str(exception)}",
                )
            )

        labels = [
            {"key": "request_method", "value": "PUT"},
            {"key": "method_name", "value": "updateBlobMetadata"},
            {"key": "object_path", "value": object.get('location')},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return is_tag_updated

    def fetch_objects(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector, prefix: str = None
    ) -> List[ObjectInfo]:
        if prefix is None:
            prefix = self.prefix
        objects = self._list_objects(ctx, metrics_collector=metrics_collector, prefix=prefix)
        objects_info = []
        if not objects:
            return []
        for obj in objects:
            object_info = ObjectInfo(
                id=str(uuid4()),
                key=obj.name,
                location=f"{self.obj_prefix}/{obj.name}",
                format=obj.name.split(".")[-1],
                file_size_kb=obj.size // 1024,
                file_hash=obj.etag.strip('"'),
                tags=self.fetch_tags(obj.name, metrics_collector)
            )
            objects_info.append(object_info.to_json())
        return objects_info

    def read_object(
        self,
        object_path: str,
        sc: SparkSession,
        metrics_collector: MetricsCollector,
        file_format: str
    ) -> DataFrame:
        api_calls, errors, records_count = 0, 0, 0
        error_code = ""
        df = None
        try:
            df = super().read_file(
                objectPath=object_path,
                sc=sc,
                metrics_collector=metrics_collector,
                file_format=file_format
            )
            records_count = df.count()
            api_calls += 1
        except (ClientError) as exception:
            errors += 1
            error_code = exception.response.json()["error"]["code"]
            ObsrvException(
                ErrorData(
                    "GCS_READ_ERROR", f"failed to read object from GCS: {str(exception)}"
                )
            )
        except Exception as exception:
            errors += 1
            error_code = "GCS_READ_ERROR"
            ObsrvException(
                ErrorData(
                    "GCS_READ_ERROR", f"failed to read object from GCS: {str(exception)}"
                )
            )

        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getBlob"},
            {"key": "object_path", "value": object_path},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return df

    def _list_objects(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector, prefix: str
    ) -> list:
        bucket_name = self.bucket
        summaries = []
        page_token = None
        file_formats = {
            "json": ["json", "json.gz", "json.zip"],
            "jsonl": ["json", "json.gz", "json.zip"],
            "csv": ["csv", "csv.gz", "csv.zip"],
            "parquet": ["parquet", "parquet.gz", "parquet.zip"],
        }
        file_format = self.connector_config["source_data_format"]
        api_calls, errors = 0, 0
        error_code = ""
        page_size = 1000

        while True:
            try:
                bucket = self.gcs_client.bucket(bucket_name)
                if page_token is not None:
                    blobs_response = bucket.list_blobs(
                        prefix=prefix,
                        page_token=page_token,
                        max_results=page_size
                    )
                else:
                    blobs_response: Iterator[storage.Blob] = bucket.list_blobs(
                        prefix=prefix,
                        max_results=page_size
                    )
                api_calls += 1
                for blob in blobs_response:
                    if file_format in file_formats and any(blob.name.endswith(f) for f in file_formats[file_format]):
                        summaries.append(blob)
                if not blobs_response.next_page_token:
                    break
                page_token = blobs_response.next_page_token
            except ClientError as exception:
                errors += 1
                error_code = exception.response.json()["error"]["code"]
                ObsrvException(
                    ErrorData(
                        "GCS_LIST_ERROR",
                        f"failed to list objects in GCS: {str(exception)}",
                    )
                )
                break

        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "listBlobs"},
            {"key": "object_path", "value": ""},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return summaries

    def _get_spark_session(self):
        return SparkSession.builder.config(conf=self.get_spark_config()).getOrCreate()
