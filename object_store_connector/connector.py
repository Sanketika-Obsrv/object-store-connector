import datetime
import json
import time
from typing import Any, Dict, Iterator

from models.object_info import ObjectInfo
from obsrv.common import ObsrvException
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.connector.batch import ISourceConnector
from obsrv.models import ErrorData, ExecutionState, StatusCode
from obsrv.utils import LoggerController
from provider.s3 import S3
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from provider.gcs import GCS

logger = LoggerController(__name__)

MAX_RETRY_COUNT = 10


class ObjectStoreConnector(ISourceConnector):
    def __init__(self):
        self.provider = None
        self.objects = list()
        self.dedupe_tag = None
        self.success_state = StatusCode.SUCCESS.value
        self.error_state = StatusCode.FAILED.value
        self.running_state = ExecutionState.RUNNING.value
        self.not_running_state = ExecutionState.NOT_RUNNING.value
        self.queued_state = ExecutionState.QUEUED.value

    def process(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector,
    ) -> Iterator[DataFrame]:
        if (
            ctx.state.get_state("status", default_value=self.not_running_state)
            == self.running_state
        ):
            logger.info("Connector is already running. Skipping processing.")
            return

        ctx.state.put_state("status", self.running_state)
        ctx.state.save_state()
        self.max_retries = (
            connector_config["source"]["max_retries"]
            if "max_retries" in connector_config["source"]
            else MAX_RETRY_COUNT
        )
        self._get_provider(connector_config)
        self._get_objects_to_process(ctx, metrics_collector)
        for res in self._process_objects(sc, ctx, metrics_collector):
            yield res

        last_run_time = datetime.datetime.now()
        ctx.state.put_state("status", self.not_running_state)
        ctx.state.put_state("last_run_time", last_run_time)
        ctx.state.save_state()

    def get_spark_conf(self, connector_config) -> SparkConf:
        self._get_provider(connector_config)
        if self.provider is not None:
            return self.provider.get_spark_config(connector_config)

        return SparkConf()

    def _get_provider(self, connector_config: Dict[Any, Any]):
        if connector_config["source"]["type"] == "s3":
            self.provider = S3(connector_config)
        elif connector_config["source"]["type"] == "gcs":
            self.provider = GCS(connector_config)
        else:
            ObsrvException(
                ErrorData(
                    "INVALID_PROVIDER",
                    "provider not supported: {}".format(
                        connector_config["source"]["type"]
                    ),
                )
            )

    def _get_objects_to_process(
        self, ctx: ConnectorContext, metrics_collector: MetricsCollector
    ) -> None:
        objects = ctx.state.get_state("to_process", list())
        if ctx.building_block is not None and ctx.env is not None:
            self.dedupe_tag = "{}-{}".format(ctx.building_block, ctx.env)
        else:
            raise ObsrvException(
                ErrorData(
                    "INVALID_CONTEXT", "building_block or env not found in context"
                )
            )

        if not len(objects):
            num_files_discovered = ctx.stats.get_stat("num_files_discovered", 0)
            objects = self.provider.fetch_objects(ctx, metrics_collector)
            objects = self._exclude_processed_objects(ctx, objects)
            metrics_collector.collect("new_objects_discovered", len(objects))
            ctx.state.put_state("to_process", objects)
            ctx.state.save_state()
            num_files_discovered += len(objects)
            ctx.stats.put_stat("num_files_discovered", num_files_discovered)
            ctx.stats.save_stats()

        self.objects = objects

    def _process_objects(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        metrics_collector: MetricsCollector,
    ) -> Iterator[DataFrame]:
        num_files_processed = ctx.stats.get_stat("num_files_processed", 0)
        for i in range(0, len(self.objects)):
            obj = self.objects[i]
            obj["start_processing_time"] = time.time()
            df = self.provider.read_object(
                obj.get("location"),
                sc=sc,
                metrics_collector=metrics_collector,
                file_format=ctx.data_format,
            )

            if df is None:
                obj["num_of_retries"] += 1
                if obj["num_of_retries"] < self.max_retries:
                    ctx.state.put_state("to_process", self.objects[i:])
                    ctx.state.save_state()
                else:
                    if not self.provider.update_tag(
                        object=obj,
                        tags=[{"key": self.dedupe_tag, "value": self.error_state}],
                        metrics_collector=metrics_collector,
                    ):
                        break
                return
            else:
                df = self._append_custom_meta(sc, df, obj)
                obj["download_time"] = time.time() - obj.get("start_processing_time")
                if not self.provider.update_tag(
                    object=obj,
                    tags=[{"key": self.dedupe_tag, "value": self.success_state}],
                    metrics_collector=metrics_collector,
                ):
                    break
                ctx.state.put_state("to_process", self.objects[i + 1 :])
                ctx.state.save_state()
                num_files_processed += 1
                ctx.stats.put_stat("num_files_processed", num_files_processed)
                obj["end_processing_time"] = time.time()
                yield df

        ctx.stats.save_stats()

    def _append_custom_meta(
        self, sc: SparkSession, df: DataFrame, object: ObjectInfo
    ) -> DataFrame:
        addn_meta = {
            "location": object.get("location"),
            "file_size_kb": object.get("file_size_kb"),
            "download_time": object.get("download_time"),
            "start_processing_time": object.get("start_processing_time"),
            "end_processing_time": object.get("end_processing_time"),
            "file_hash": object.get("file_hash"),
            "num_of_retries": object.get("num_of_retries"),
            "in_time": object.get("in_time"),
        }
        df = df.withColumn("_addn_source_meta", lit(json.dumps(addn_meta, default=str)))
        return df

    def _exclude_processed_objects(self, ctx: ConnectorContext, objects):
        to_be_processed = []
        for obj in objects:
            if not any(tag["key"] == self.dedupe_tag for tag in obj.get("tags")):
                to_be_processed.append(obj)

        # return [objects[-1]] #TODO: Remove this line
        return to_be_processed
