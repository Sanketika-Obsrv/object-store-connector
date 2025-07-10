import os
import sys
import json
import unittest
import datetime

import pytest
from dateutil.relativedelta import relativedelta

from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error
from testcontainers.minio import MinioContainer

from obsrv.utils import Config
from obsrv.connector.batch import SourceConnector

sys.path.insert(0, os.path.join(os.getcwd(), "object_store_connector"))

from object_store_connector.connector import ObjectStoreConnector, schedule_dict
from object_store_connector.provider.s3 import S3

from tests.batch_setup import setup_obsrv_database, create_spark_session, validate_results, delete_and_create_topic, insert_connectors  # noqa


start_time = datetime.datetime.now()
# Test schedules all set to Daily
local_vars = {}
schedule_config = schedule_dict["Daily"]
exec(f"kwargs = dict({schedule_config['last_processed_partition']})", {"current_time": start_time}, local_vars)
start_time_partition = start_time - relativedelta(**local_vars["kwargs"])

connector_instances = list()

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

    connector_instances.append({
        "id": "s3.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "no-time-partition/",
            "source_type": "s3",
            "source_data_format": "json",
        } | minio_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    connector_instances.append({
        "id": "new-time-partitioned-s3.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "new-time-partition/%d-%m-%Y/",
            "source_type": "s3",
            "source_data_format": "json",
            "source_partition_type": "time",
        } | minio_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    connector_instances.append({
        "id": "old-time-partitioned-s3.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "old-time-partition/%Y-%m-%d/",
            "source_type": "s3",
            "source_data_format": "json",
            "source_partition_type": "time",
        } | minio_config),
        "operations_config": json.dumps({"schedule": "Daily"}),
        "connector_state": json.dumps({"to_process": [], "last_processed_time_partition": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")})
    })

    connector_instances.append({
        "id": "s3.new-york-taxi-data.2",
        "connector_config": json.dumps({
            "source_prefix": "",
            "source_type": "s3",
            "source_data_format": "json",
        } | minio_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

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
        for connector_instance in connector_instances:
            minio_client.fput_object(
                bucket_name,
                object_name=start_time.strftime(json.loads(connector_instance["connector_config"])["source_prefix"]) + "data.json",
                file_path=file_path,
                content_type=c_type,
            )

        tags = Tags(for_object=True)
        tags["{}-{}".format("py-sdk-test", "local")] =  "success"

        minio_client.fput_object(
            bucket_name,
            object_name="processed_data.json",
            file_path=file_path,
            content_type=c_type,
            tags=tags
        )

        ### List minio objects
        print("------- Listing MINIO objects ---------")
        objects = minio_client.list_objects(bucket_name, prefix="", recursive=True)
        for obj in objects:
            print(f"Object: {obj.object_name}, Tags: {obj.tags}")
        print("------- Listing MINIO objects END ---------")

    except S3Error as e:
        print(f"Error uploading objects: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return minio_client


@pytest.mark.usefixtures("setup_obsrv_database", "create_spark_session")
class TestS3BatchConnector(unittest.TestCase):
    def test_1_source_connector_no_time_partition(self):
        connector_instance_id = connector_instances[0]["id"]
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
        validate_results(config, connector_instance_id)

    def test_2_source_connector_new_time_partition(self):
        connector_instance_id = connector_instances[1]["id"]
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
        validate_results(config, connector_instance_id)


    def test_3_source_connector_old_time_partition(self):
        connector_instance_id = connector_instances[2]["id"]
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
        validate_results(config, connector_instance_id)


    def test_4_source_connector_empty_prefix(self):
        connector_instance_id = connector_instances[3]["id"]
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
        validate_results(config, connector_instance_id)
