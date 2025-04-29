import os
import sys
import json
import unittest
import datetime

import yaml
import psycopg2
import psycopg2.extras
import pytest
from dateutil.relativedelta import relativedelta

from kafka import KafkaConsumer, TopicPartition
from minio import Minio
from minio.error import S3Error
from testcontainers.minio import MinioContainer

from obsrv.utils import EncryptionUtil, Config
from obsrv.connector.batch import SourceConnector

sys.path.insert(0, os.path.join(os.getcwd(), "object_store_connector"))

from object_store_connector.connector import ObjectStoreConnector, schedule_dict

from tests.batch_setup import setup_obsrv_database  # noqa


start_time = datetime.datetime.now()
# Test schedules all set to Daily
local_vars = {}
schedule_config = schedule_dict["Daily"]
exec(f"kwargs = dict({schedule_config['last_processed_partition']})", {"current_time": start_time}, local_vars)
start_time_partition = start_time - relativedelta(**local_vars["kwargs"])

@pytest.fixture(scope="module", autouse=True)
def setup(request):
    minio_container = MinioContainer("minio/minio:latest")
    minio_container.start()
    minio_config = {
        "source_bucket": "testing-bucket",
        "source_credentials_region": "us-east-2",
        "source_credentials_secret_key": minio_container.secret_key,
        "source_credentials_access_key": minio_container.access_key,
        "source_credentials_endpoint": f"{minio_container.get_container_host_ip()}:{minio_container.get_exposed_port(9000)}"
    }

    connector_instances = [
        {
            "id": "s3.new-york-taxi-data.1",
            "connector_config": json.dumps({
                "source_prefix": "no-time-partition",
                "source_type": "s3",
                "source_data_format": "json",
            } | minio_config)
        },
        {
            "id": "new-time-partitioned-s3.new-york-taxi-data.1",
            "connector_config": json.dumps({
                "source_prefix": "new-time-partition/%d-%m-%Y/",
                "source_type": "s3",
                "source_data_format": "json",
                "source_partition_type": "time",
            } | minio_config),
            "operations_config": json.dumps({"schedule": "Daily"})
        },
        {
            "id": "old-time-partitioned-s3.new-york-taxi-data.1",
            "connector_config": json.dumps({
                "source_prefix": "old-time-partition/%d-%m-%Y/",
                "source_type": "s3",
                "source_data_format": "json",
                "source_partition_type": "time",
            } | minio_config),
            "operations_config": json.dumps({"schedule": "Daily"}),
            "connector_state": json.dumps({"to_process": [], "last_processed_time_partition": "2025-04-19 00:00:00"}),
        },
    ]

    insert_connectors(connector_instances)
    init_minio(minio_config)

    def cleanup():
        minio_container.stop()

    request.addfinalizer(cleanup)


def init_minio(config):
    minio_client = Minio(
        endpoint=config["source_credentials_endpoint"],
        access_key=config["source_credentials_access_key"],
        secret_key=config["source_credentials_secret_key"],
        region=config["source_credentials_region"],
        secure=False
    )

    bucket_name = config['source_bucket']

    # Create Bucket
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except S3Error as e:
        print(f"[ERROR] Error while creating MINIO bucket: {e}")

    file_path = "./tests/test_data/chunk-1.json"
    c_type = "application/json"

    # Upload Objects
    try:
        minio_client.fput_object(
            bucket_name,
            object_name="no-time-partition/data.json",
            file_path=file_path,
            content_type=c_type,
        )

        minio_client.fput_object(
            bucket_name,
            object_name=start_time.strftime("new-time-partition/%d-%m-%Y/data.json"),
            file_path=file_path,
            content_type=c_type,
        )

        minio_client.fput_object(
            bucket_name,
            object_name=start_time.strftime("old-time-partition/%d-%m-%Y/data.json"),
            file_path=file_path,
            content_type=c_type,
        )

    except S3Error as e:
        print(f"Error uploading objects: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return minio_client


def insert_connectors(connector_instances):
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "r"
    ) as config_file:
        config = yaml.safe_load(config_file)
    
    enc = EncryptionUtil(config["obsrv_encryption_key"])

    ins_cr = """
        INSERT INTO connector_registry (
            id,
            connector_id,
            "name",
            "type",
            category,
            "version",
            description,
            technology,
            runtime,
            licence,
            "owner",
            iconurl,
            status,
            ui_spec,
            source_url,
            "source",
            created_by,
            updated_by,
            created_date,
            updated_date,
            live_date
        ) VALUES (
            'aws-s3-connector-0.1.0',
            'aws-s3-connector',
            'AWS S3',
            'source',
            'batch',
            '0.1.0',
            'test_reader',
            'Python',
            'Spark',
            'MIT',
            'ravi@obsrv.ai',
            'http://localhost',
            'Live',
            '{}',
            'object_store_connector-0.1.0.tar_8b84dc.gz',
            '{}',
            'SYSTEM',
            'SYSTEM',
            now(),
            now(),
            now()
        );
    """

    ins_ci = """
        INSERT INTO connector_instances (
            id,
            dataset_id,
            connector_id,
            connector_config,
            operations_config,
            status,
            connector_state,
            connector_stats,
            created_by,
            updated_by,
            created_date,
            updated_date,
            published_date
        ) VALUES (
            %s,
            'new-york-taxi-data',
            'aws-s3-connector-0.1.0',
            %s,
            %s,
            'Live',
            %s,
            '{}',
            'SYSTEM',
            'SYSTEM',
            now(),
            now(),
            now()
        );
    """

    conn = psycopg2.connect(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        dbname=config["postgres"]["dbname"],
    )

    cur = conn.cursor()

    cur.execute(ins_cr)

    for instance in connector_instances:
        connector_instance_id = instance["id"]
        enc_config = enc.encrypt(instance["connector_config"])
        ops_config = instance.get("operations_config", '{}')
        connector_state = instance.get("connector_state", '{}')
        cur.execute(ins_ci, (connector_instance_id, json.dumps(enc_config), ops_config, connector_state,))

    conn.commit()
    conn.close()


# @pytest.mark.usefixtures("setup_obsrv_database")
class TestBatchConnector(unittest.TestCase):
    record_offset = 0

    def test_1_source_connector(self):
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        with open(config_file_path, "r") as config_file:
            config = yaml.safe_load(config_file)
            config["connector_instance_id"] = "s3.new-york-taxi-data.1"
            config["kafka"]["connector-metrics-topic"] = "dev.metrics.no.tp"
        with open(config_file_path, "w") as config_file:
            yaml.dump(config, config_file)

        config = Config(config_file_path)

        test_raw_topic = "dev.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config.find("kafka.broker-servers"),
            group_id="s3-group",
            enable_auto_commit=True,
            auto_offset_reset='earliest'
        )

        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        # kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        connector_state = None
        connector_stats = None

        postgres_config = config.find("postgres")
        conn = psycopg2.connect(
            host=postgres_config["host"],
            port=postgres_config["port"],
            user=postgres_config["user"],
            password=postgres_config["password"],
            dbname=postgres_config["dbname"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(f"SELECT * FROM connector_instances WHERE id = '{config.find("connector_instance_id")}';")
        connector_instance = cur.fetchone()
        connector_state = connector_instance["connector_state"]
        connector_stats = connector_instance["connector_stats"]
        conn.close

        obj_count = 1
        records_per_obj = 200
        expected_record_count = obj_count * records_per_obj
        expected_record_offset = self.__class__.record_offset + expected_record_count

        # Postgres asserts
        assert connector_stats["num_files_discovered"] == obj_count
        assert connector_stats["num_files_processed"] == obj_count
        assert connector_state["to_process"] == []

        metrics = []
        all_messages = kafka_consumer.poll(timeout_ms=10000)
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    metrics.append(json.loads(message.value))

        api_calls_ListObjectsV2, errors_ListObjectsV2 = 0, 0
        api_calls_getObjectTagging, errors_getObjectTagging = 0, 0
        api_calls_getObject, errors_getObject = 0, 0
        api_calls_setObjectTagging, errors_setObjectTagging = 0, 0

        for metric in metrics:
            for d in metric["edata"]["labels"]:
                if "method_name" != d["key"]:
                    continue
                if d["value"] == "ListObjectsV2":
                    api_calls_ListObjectsV2 += metric["edata"]["metric"]["num_api_calls"]
                    errors_ListObjectsV2 += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObjectTagging":
                    api_calls_getObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObject":
                    api_calls_getObject += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObject += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "setObjectTagging":
                    api_calls_setObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_setObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break

        print(f"ListObjectsV2 requests: {api_calls_ListObjectsV2}, errors: {errors_ListObjectsV2}")
        print(f"getObjectTagging requests: {api_calls_getObjectTagging}, errors: {errors_getObjectTagging}")
        print(f"getObject requests: {api_calls_getObject}, errors: {errors_getObject}")
        print(f"setObjectTagging requests: {api_calls_setObjectTagging}, errors: {errors_setObjectTagging}")
        print(f"Total exec time: {metrics[-1]["edata"]["metric"]["total_exec_time_ms"]}")
        print(f"Framework exec time: {metrics[-1]["edata"]["metric"]["fw_exec_time_ms"]}")
        print(f"Connector exec time: {metrics[-1]["edata"]["metric"]["connector_exec_time_ms"]}")

        # Api call count asserts
        assert api_calls_getObjectTagging   \
            == api_calls_getObject          \
            == api_calls_setObjectTagging   \
            == obj_count

        # Number of times metrics_collector.collect() should be called
        # SDK => 1 execution metric, connector => 1 num_objects_discovered metric, so + 2
        expected_metric_count = api_calls_ListObjectsV2 + 3 * obj_count + 2

        # Kafka asserts
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_record_offset}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: expected_metric_count}

        # Objects discovered metrics
        assert metrics[
            int(api_calls_ListObjectsV2 + api_calls_getObjectTagging)
        ]["edata"]["metric"]["new_objects_discovered"] == obj_count

        # Execution metrics
        assert metrics[-1]["edata"]["metric"]["total_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["success_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["failed_records_count"] == 0
        self.__class__.record_offset += kafka_consumer.end_offsets([trt_consumer])[trt_consumer]

    def test_2_new_tp_source_connector(self):
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        with open(
            config_file_path, "r"
        ) as config_file:
            config = yaml.safe_load(config_file)
            config["connector_instance_id"] = "new-time-partitioned-s3.new-york-taxi-data.1"
            config["kafka"]["connector-metrics-topic"] = "dev.metrics.new.tp"
        with open(
            config_file_path, "w"
        ) as config_file:
            yaml.dump(config, config_file)

        config = Config(config_file_path)

        test_raw_topic = "dev.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config.find("kafka.broker-servers"),
            group_id="s3-group",
            enable_auto_commit=True,
            auto_offset_reset='earliest'
        )

        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        # kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        connector_state = None
        connector_stats = None

        postgres_config = config.find("postgres")
        conn = psycopg2.connect(
            host=postgres_config["host"],
            port=postgres_config["port"],
            user=postgres_config["user"],
            password=postgres_config["password"],
            dbname=postgres_config["dbname"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(f"SELECT * FROM connector_instances WHERE id = '{config.find("connector_instance_id")}';")
        connector_instance = cur.fetchone()
        connector_state = connector_instance["connector_state"]
        connector_stats = connector_instance["connector_stats"]
        conn.close

        obj_count = 1
        records_per_obj = 200
        expected_record_count = obj_count * records_per_obj
        expected_record_offset = self.__class__.record_offset + expected_record_count

        # Postgres asserts
        assert connector_stats["num_files_discovered"] == obj_count
        assert connector_stats["num_files_processed"] == obj_count
        assert connector_state["to_process"] == []
        assert datetime.datetime.fromisoformat(connector_state["last_processed_time_partition"]) >= start_time_partition
        assert datetime.datetime.fromisoformat(connector_state["last_processed_time_partition"]) <= datetime.datetime.now()

        metrics = []
        all_messages = kafka_consumer.poll(timeout_ms=10000)
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    metrics.append(json.loads(message.value))

        api_calls_ListObjectsV2, errors_ListObjectsV2 = 0, 0
        api_calls_getObjectTagging, errors_getObjectTagging = 0, 0
        api_calls_getObject, errors_getObject = 0, 0
        api_calls_setObjectTagging, errors_setObjectTagging = 0, 0

        for metric in metrics:
            for d in metric["edata"]["labels"]:
                if "method_name" != d["key"]:
                    continue
                if d["value"] == "ListObjectsV2":
                    api_calls_ListObjectsV2 += metric["edata"]["metric"]["num_api_calls"]
                    errors_ListObjectsV2 += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObjectTagging":
                    api_calls_getObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObject":
                    api_calls_getObject += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObject += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "setObjectTagging":
                    api_calls_setObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_setObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break

        print(f"ListObjectsV2 requests: {api_calls_ListObjectsV2}, errors: {errors_ListObjectsV2}")
        print(f"getObjectTagging requests: {api_calls_getObjectTagging}, errors: {errors_getObjectTagging}")
        print(f"getObject requests: {api_calls_getObject}, errors: {errors_getObject}")
        print(f"setObjectTagging requests: {api_calls_setObjectTagging}, errors: {errors_setObjectTagging}")
        print(f"Total exec time: {metrics[-1]["edata"]["metric"]["total_exec_time_ms"]}")
        print(f"Framework exec time: {metrics[-1]["edata"]["metric"]["fw_exec_time_ms"]}")
        print(f"Connector exec time: {metrics[-1]["edata"]["metric"]["connector_exec_time_ms"]}")

        # Api call count asserts
        assert api_calls_getObject          \
            == api_calls_setObjectTagging   \
            == obj_count
        # total number of objects uploaded to minio
        assert api_calls_getObjectTagging == 3

        # Number of times metrics_collector.collect() should be called
        # SDK => 1 execution metric, connector => 1 num_objects_discovered metric, so + 2
        expected_metric_count = api_calls_ListObjectsV2 + 2 + 3 * obj_count + 2

        # Kafka asserts
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_record_offset}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: expected_metric_count}

        # Objects discovered metrics
        assert metrics[
            int(api_calls_ListObjectsV2 + api_calls_getObjectTagging)
        ]["edata"]["metric"]["new_objects_discovered"] == obj_count

        # Execution metrics
        assert metrics[-1]["edata"]["metric"]["total_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["success_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["failed_records_count"] == 0
        self.__class__.record_offset += kafka_consumer.end_offsets([trt_consumer])[trt_consumer]

    def test_3_old_tp_source_connector(self):
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        with open(
            config_file_path, "r"
        ) as config_file:
            config = yaml.safe_load(config_file)
            config["connector_instance_id"] = "old-time-partitioned-s3.new-york-taxi-data.1"
            config["kafka"]["connector-metrics-topic"] = "dev.metrics.old.tp"
        with open(
            config_file_path, "w"
        ) as config_file:
            yaml.dump(config, config_file)

        config = Config(config_file_path)

        test_raw_topic = "dev.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config.find("kafka.broker-servers"),
            group_id="s3-group",
            enable_auto_commit=True,
            auto_offset_reset='earliest'
        )

        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

        # kafka_consumer.seek_to_beginning()

        SourceConnector.process(connector=connector, config_file_path=config_file_path)

        connector_state = None
        connector_stats = None

        postgres_config = config.find("postgres")
        conn = psycopg2.connect(
            host=postgres_config["host"],
            port=postgres_config["port"],
            user=postgres_config["user"],
            password=postgres_config["password"],
            dbname=postgres_config["dbname"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(f"SELECT * FROM connector_instances WHERE id = '{config.find("connector_instance_id")}';")
        connector_instance = cur.fetchone()
        connector_state = connector_instance["connector_state"]
        connector_stats = connector_instance["connector_stats"]
        conn.close

        obj_count = 1
        records_per_obj = 200
        expected_record_count = obj_count * records_per_obj
        expected_record_offset = self.__class__.record_offset + expected_record_count

        # Postgres asserts
        assert connector_stats["num_files_discovered"] == obj_count
        assert connector_stats["num_files_processed"] == obj_count
        assert connector_state["to_process"] == []
        assert datetime.datetime.fromisoformat(connector_state["last_processed_time_partition"]) >= start_time_partition
        assert datetime.datetime.fromisoformat(connector_state["last_processed_time_partition"]) <= datetime.datetime.now()

        metrics = []
        all_messages = kafka_consumer.poll(timeout_ms=10000)
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    metrics.append(json.loads(message.value))

        api_calls_ListObjectsV2, errors_ListObjectsV2 = 0, 0
        api_calls_getObjectTagging, errors_getObjectTagging = 0, 0
        api_calls_getObject, errors_getObject = 0, 0
        api_calls_setObjectTagging, errors_setObjectTagging = 0, 0

        for metric in metrics:
            for d in metric["edata"]["labels"]:
                if "method_name" != d["key"]:
                    continue
                if d["value"] == "ListObjectsV2":
                    api_calls_ListObjectsV2 += metric["edata"]["metric"]["num_api_calls"]
                    errors_ListObjectsV2 += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObjectTagging":
                    api_calls_getObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getObject":
                    api_calls_getObject += metric["edata"]["metric"]["num_api_calls"]
                    errors_getObject += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "setObjectTagging":
                    api_calls_setObjectTagging += metric["edata"]["metric"]["num_api_calls"]
                    errors_setObjectTagging += metric["edata"]["metric"]["num_errors"]
                    break

        print(f"ListObjectsV2 requests: {api_calls_ListObjectsV2}, errors: {errors_ListObjectsV2}")
        print(f"getObjectTagging requests: {api_calls_getObjectTagging}, errors: {errors_getObjectTagging}")
        print(f"getObject requests: {api_calls_getObject}, errors: {errors_getObject}")
        print(f"setObjectTagging requests: {api_calls_setObjectTagging}, errors: {errors_setObjectTagging}")
        print(f"Total exec time: {metrics[-1]["edata"]["metric"]["total_exec_time_ms"]}")
        print(f"Framework exec time: {metrics[-1]["edata"]["metric"]["fw_exec_time_ms"]}")
        print(f"Connector exec time: {metrics[-1]["edata"]["metric"]["connector_exec_time_ms"]}")

        # Api call count asserts
        assert api_calls_getObjectTagging   \
            == api_calls_getObject          \
            == api_calls_setObjectTagging   \
            == obj_count

        # Number of times metrics_collector.collect() should be called
        # SDK => 1 execution metric, connector => 1 num_objects_discovered metric, so + 2
        expected_metric_count = api_calls_ListObjectsV2 + 3 * obj_count + 2

        # Kafka asserts
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_record_offset}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: expected_metric_count}

        # Objects discovered metrics
        assert metrics[
            int(api_calls_ListObjectsV2 + api_calls_getObjectTagging)
        ]["edata"]["metric"]["new_objects_discovered"] == obj_count

        # Execution metrics
        assert metrics[-1]["edata"]["metric"]["total_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["success_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["failed_records_count"] == 0
        self.__class__.record_offset += kafka_consumer.end_offsets([trt_consumer])[trt_consumer]
