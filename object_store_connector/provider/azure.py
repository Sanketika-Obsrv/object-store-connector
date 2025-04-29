from typing import List

from azure.core.exceptions import AzureError
from azure.storage.blob import ContainerClient, BlobClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.conf import SparkConf

from obsrv.job.batch import get_base_conf
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.common import ObsrvException
from obsrv.models import ErrorData

from provider.blob_provider import BlobProvider
from models.object_info import ObjectInfo, Tag


class AzureBlobStorage(BlobProvider):
    def __init__(self, connector_config: dict) -> None:
        super().__init__()
        self.connector_config = connector_config
        self.account_name = connector_config["source_credentials_account_name"]
        self.account_key = connector_config["source_credentials_account_key"]
        self.container_name = connector_config["source_container_name"]
        self.blob_endpoint = connector_config.get("source_blob_endpoint", None)
        self.prefix = connector_config.get("source_prefix", "")

        if self.blob_endpoint == "core.windows.net":
            self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix={self.blob_endpoint}"
        else:
            self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};BlobEndpoint={self.blob_endpoint}"
        self.container_client = ContainerClient.from_connection_string(self.connection_string, self.container_name)

    def get_spark_config(self, connector_config) -> SparkConf:
        conf = get_base_conf()
        conf.setAppName("AzureObjectStoreConnector")
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1")
        conf.set("fs.azure.storage.accountAuthType", "SharedKey")
        conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
        conf.set(f"fs.azure.account.key.{self.account_name}.blob.core.windows.net", self.account_key)
        conf.set("fs.azure.storage.accountKey", self.account_key)
        return conf

    def fetch_objects(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector, prefix: str = None
    ) -> List[ObjectInfo]:
        if prefix is None:
            prefix = self.prefix

        objects = self._list_blobs(ctx=ctx, metrics_collector=metrics_collector, prefix=prefix)

        objects_info = []
        if objects is None:
            raise Exception("No objects found")

        for obj in objects:
            if self.blob_endpoint == ("core.windows.net"):
                blob_location = f"wasbs://{self.container_name}@{self.account_name}.blob.core.windows.net/{obj['name']}"
            else:
                blob_location = f"wasb://{self.container_name}@storageemulator/{obj['name']}"

            object_info = ObjectInfo(
                location=blob_location,
                key=obj["name"],
                format=obj["name"].split(".")[-1],
                file_size_kb=obj["size"] // 1024,
                file_hash=obj["etag"].strip('"'),
                tags=self.fetch_tags(obj['name'], metrics_collector)
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
        except AzureError as exception:
            errors += 1
            error_code = str(exception.exc_msg)
            ObsrvException(
                ErrorData(
                    "AzureBlobStorage_READ_ERROR", f"failed to read object from AzureBlobStorage: {str(exception)}"
                )
            )
        except Exception as exception:
            errors += 1
            error_code = "AzureBlobStorage_READ_ERROR"
            ObsrvException(
                ErrorData(
                    "AzureBlobStorage_READ_ERROR", f"failed to read object from AzureBlobStorage: {str(exception)}"
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

    def _list_blobs(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector, prefix: str
    ) -> list:
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
                    blobs = self.container_client.list_blobs(
                        name_starts_with=prefix,
                        results_per_page=1000
                    ).by_page(continuation_token=continuation_token)
                else:
                    blobs = self.container_client.list_blobs(name_starts_with=prefix)
                api_calls += 1

                for blob in blobs:
                    if file_format in file_formats and any(blob["name"].endswith(f) for f in file_formats[file_format]):
                        summaries.append(blob)
                if not continuation_token:
                    break
                continuation_token = blobs.continuation_token
            except AzureError as exception:
                errors += 1
                error_code = str(exception.exc_msg)
                ObsrvException(
                    ErrorData(
                        "AZURE_BLOB_LIST_ERROR",
                        f"failed to list objects in AzureBlobStorage: {str(exception)}",
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

    def fetch_tags(
        self, object_path: str, metrics_collector: MetricsCollector
    ) -> List[Tag]:
        container_name = self.container_name
        error_code = ""
        fetched_tags = []
        api_calls, errors = 0, 0
        try:
            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=container_name, blob_name=object_path
            )
            tags = blob_client.get_blob_tags()
            api_calls += 1
            fetched_tags = [Tag(key, value) for key, value in tags.items()]
        except AzureError as exception:
            errors += 1
            error_code = str(exception.exc_msg)
            ObsrvException(
                ErrorData(
                    "AzureBlobStorage_TAG_READ_ERROR",
                    f"failed to fetch tags from AzureBlobStorage: {str(exception)}",
                )
            )
        labels = [
            {"key": "request_method", "value": "GET"},
            {"key": "method_name", "value": "getBlobTags"},
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
        try:
            new_dict = {tag['key']: tag['value'] for tag in tags}
            obj = object.get("key")

            blob_client = BlobClient.from_connection_string(
                conn_str=self.connection_string, container_name=self.container_name, blob_name=obj
            )
            existing_tags = blob_client.get_blob_tags() or {}
            existing_tags.update(new_dict)

            blob_client.set_blob_tags(existing_tags)
            api_calls += 1
            is_tag_updated = True
        except AzureError as exception:
            errors += 1
            error_code = str(exception.exc_msg)
            ObsrvException(
                ErrorData(
                    "AzureBlobStorage_TAG_UPDATE_ERROR",
                    f"failed to update tags in AzureBlobStorage for object: {str(exception)}",
                )
            )
        labels = [
            {"key": "request_method", "value": "PUT"},
            {"key": "method_name", "value": "setBlobTags"},
            {"key": "object_path", "value": object.get('location')},
            {"key": "error_code", "value": error_code}
        ]
        metrics_collector.collect({"num_api_calls": api_calls, "num_errors": errors}, addn_labels=labels)
        return is_tag_updated
