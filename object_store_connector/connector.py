import re
import datetime
import json
import time
from typing import Any, Dict, Iterator

from obsrv.common import ObsrvException
from obsrv.connector import ConnectorContext, MetricsCollector
from obsrv.connector.batch import ISourceConnector
from obsrv.models import ErrorData, StatusCode
from obsrv.utils import LoggerController
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from dateutil.relativedelta import relativedelta

from models.object_info import ObjectInfo
from provider.azure import AzureBlobStorage
from provider.gcs import GCS
from provider.s3 import S3


logger = LoggerController(__name__)

MAX_RETRY_COUNT = 10

schedule_dict = {
    "Hourly": {
        "last_processed_partition": "minute=0, second=0, microsecond=0",
        "interval": "hours=1"
    },
    "Daily": {
        "last_processed_partition": "hour=0, minute=0, second=0, microsecond=0",
        "interval": "days=1"
    },
    "Weekly": {
        "last_processed_partition": "days=current_time.weekday(), hour=0, minute=0, second=0, microsecond=0",
        "interval": "days=7"
    },
    "Monthly": {
        "last_processed_partition": "day=1, hour=0, minute=0, second=0, microsecond=0",
        "interval": "months=1"
    },
    "Yearly": {
        "last_processed_partition": "month=1, day=1, hour=0, minute=0, second=0, microsecond=0",
        "interval": "years=1"
    }
}


class ObjectStoreConnector(ISourceConnector):
    def __init__(self):
        self.provider = None
        self.objects = list()
        self.dedupe_tag = None
        self.success_state = StatusCode.SUCCESS.value
        self.error_state = StatusCode.FAILED.value

    def process(
        self,
        sc: SparkSession,
        ctx: ConnectorContext,
        ops_config: Dict[Any, Any],
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector,
    ) -> Iterator[DataFrame]:
        self.max_retries = (
            connector_config["source_max_retries"]
            if "source_max_retries" in connector_config
            else MAX_RETRY_COUNT
        )
        self._get_provider(connector_config)
        self.partition_type = connector_config.get("source_partition_type", None)
        if self.partition_type == "time":
            self.schedule = ops_config.get("schedule", None)
            if self.schedule is None:
                raise ObsrvException(
                    ErrorData(
                        "INVALID_OPERATIONS_CONFIG", f"schedule must be provided in operations config when source_partition_type is time"
                    )
                )
            self.schedule_config = schedule_dict.get(self.schedule, None)
            if self.schedule_config is None:
                raise ObsrvException(
                    ErrorData(
                        "INVALID_OPERATIONS_CONFIG", f"operations config schedule must be one of {schedule_dict.keys()}"
                    )
                )
            self._time_get_objects_to_process(ctx, connector_config, metrics_collector)
        else:
            self._get_objects_to_process(ctx, metrics_collector)
        for res in self._process_objects(sc, ctx, connector_config, metrics_collector):
            yield res

        last_run_time = datetime.datetime.now()
        ctx.state.put_state("last_run_time", last_run_time)
        ctx.state.save_state()

    def get_spark_conf(self, connector_config) -> SparkConf:
        self._get_provider(connector_config)
        if self.provider is not None:
            return self.provider.get_spark_config(connector_config)

        return SparkConf()

    def _get_provider(self, connector_config: Dict[Any, Any]):
        if connector_config["source_type"] == "s3":
            self.provider = S3(connector_config)
        elif connector_config["source_type"] == "azure_blob":
            self.provider = AzureBlobStorage(connector_config)
        elif connector_config["source_type"] == "gcs":
            self.provider = GCS(connector_config)
        else:
            ObsrvException(
                ErrorData(
                    "INVALID_PROVIDER",
                    "provider not supported: {}".format(
                        connector_config["source_type"]
                    ),
                )
            )

    def _time_get_objects_to_process(
        self,
        ctx: ConnectorContext,
        connector_config: Dict[Any, Any],
        metrics_collector: MetricsCollector
    ) -> None:
        # set needed class instance variables
        self.prefix_template = connector_config.get("source_prefix", None)
        if self.prefix_template is None:
            raise ObsrvException(
                ErrorData(
                    "INVALID_CONNECTOR_CONFIG", "source_prefix must be provided in connector config when source_partition_type is time"
                )
            )
        self.prefix_regex = self.prefix_template.replace('%d', '(\d{1,2})').replace('%m', '(\d{1,2})').replace('%Y', '(\d{4})').replace('%H', '(\d{2})')

        objects = ctx.state.get_state("to_process", list())
        self.last_processed_time_partition = ctx.state.get_state("last_processed_time_partition", None)
        if ctx.building_block is not None and ctx.env is not None:
            self.dedupe_tag = "{}-{}".format(ctx.building_block, ctx.env)
        else:
            raise ObsrvException(
                ErrorData(
                    "INVALID_CONTEXT", "building_block or env not found in context"
                )
            )

        if (self.last_processed_time_partition is None) and not (len(objects)):
            # very first run of connector as no last partitioned time
            # need to fetch all objects on first run
            num_files_discovered = ctx.stats.get_stat("num_files_discovered", 0)
            if self.schedule is not None:
                self._set_last_processed_partition()
            objects = self.provider.fetch_objects(prefix="", ctx=ctx, metrics_collector=metrics_collector)

            # exclude processed objects
            objects = self._exclude_processed_objects(ctx, objects)

            # exclude objects without date in prefix (objects without right prefix format)
            # will need to create function for this
            objects = self._filter_object_prefix(objects)

            # Collect metrics and save stats as before
            metrics_collector.collect("new_objects_discovered", len(objects))
            ctx.state.put_state("to_process", objects)
            ctx.state.put_state("last_processed_time_partition", self.last_processed_time_partition)
            ctx.state.save_state()
            num_files_discovered += len(objects)
            ctx.stats.put_stat("num_files_discovered", num_files_discovered)
            ctx.stats.save_stats()

        elif not len(objects):
            num_files_discovered = ctx.stats.get_stat("num_files_discovered", 0)
            # fetch last date prefix.
            partition_date_to_process = datetime.datetime.fromisoformat(self.last_processed_time_partition)

            while True:
                # if new date prefix exceeds current time then break
                # fetch objects with new date prefix
                # extend the objects list with newly fetched objects if any
                # update last date prefix in connector state
                # add interval to last date prefix
                if datetime.datetime.now() < partition_date_to_process:
                    break
                prefix = partition_date_to_process.strftime(self.prefix_template)
                fetched_objects = self.provider.fetch_objects(ctx=ctx, metrics_collector=metrics_collector, prefix=prefix)
                objects += self._exclude_processed_objects(ctx, fetched_objects)
                ctx.state.put_state("last_processed_time_partition", partition_date_to_process)
                partition_date_to_process = self._get_next_partition_date(partition_date_to_process)

            metrics_collector.collect("new_objects_discovered", len(objects))
            ctx.state.put_state("to_process", objects)
            ctx.state.save_state()
            num_files_discovered += len(objects)
            ctx.stats.put_stat("num_files_discovered", num_files_discovered)
            ctx.stats.save_stats()

        self.objects = objects

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
            objects = self.provider.fetch_objects(ctx=ctx, metrics_collector=metrics_collector)
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
        connector_config: Dict[Any, Any],
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
                file_format=connector_config["source_data_format"],
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
                ctx.state.put_state("to_process", self.objects[i + 1:])
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

        return to_be_processed

    def _set_last_processed_partition(self):
        current_time = datetime.datetime.now()
        local_vars = {}
        exec(f"kwargs = dict({self.schedule_config['last_processed_partition']})", {"current_time": current_time}, local_vars)
        self.last_processed_time_partition = current_time - relativedelta(**local_vars["kwargs"])

    def _filter_object_prefix(self, objects: list[ObjectInfo]) -> list[ObjectInfo]:
        to_be_processed = []
        for obj in objects:
            key = obj.get("key")
            match = re.search(pattern=self.prefix_regex, string=key)
            if not match:
                print(f"[INFO] object '{key}'. does not match prefix template: {self.prefix_template}")
                continue
            prefix = match.group(0)
            try:
                object_timestamp = datetime.datetime.strptime(prefix, self.prefix_template)
            except Exception as e:
                print(f"[ERROR] Error stripping time from object '{key}'. Error: {e}")
                continue

            # exclude object if timestamp is in future
            if datetime.datetime.now() < object_timestamp:
                print(f"[INFO] object '{key}' partition is in future.")
                continue
            to_be_processed.append(obj)

        return to_be_processed

    def _get_next_partition_date(self, date: datetime.datetime) -> datetime.datetime:
        local_vars = {}
        exec(f"kwargs = dict({self.schedule_config['interval']})", {}, local_vars)
        return date + relativedelta(**local_vars["kwargs"])
