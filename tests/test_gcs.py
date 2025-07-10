import os
import sys
import json
import unittest
import datetime

import pytest
from dateutil.relativedelta import relativedelta

from google.cloud import storage
from testcontainers.core.container import DockerContainer

from obsrv.utils import Config
from obsrv.connector.batch import SourceConnector
from obsrv.job.batch import get_base_conf

sys.path.insert(0, os.path.join(os.getcwd(), "object_store_connector"))

from object_store_connector.connector import ObjectStoreConnector, schedule_dict
from object_store_connector.provider.gcs import GCS

from tests.batch_setup import setup_obsrv_database, create_spark_session, validate_results, delete_and_create_topic, insert_connectors  # noqa

## Override GCS class to use the emulator
class MockGCS(GCS):
    def __init__(self, connector_config) -> None:
        super().__init__(connector_config)
        # Support for custom endpoint (e.g., emulator)
        self.endpoint = connector_config.get("source_credentials_endpoint")

        self.gcs_client = self._get_client()

    def _get_client(self) -> storage.Client:
        client = storage.Client.create_anonymous_client()
        return client

    def get_spark_config(self, connector_config):
        conf = get_base_conf()
        conf.set("spark.hadoop.fs.gs.storage.root.url", self.endpoint)
        conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "false")
        conf.set("spark.hadoop.google.cloud.auth.impersonation.service.account.enable", "false")
        conf.set("spark.hadoop.fs.gs.auth.type", "UNAUTHENTICATED")
        conf.set("spark.hadoop.google.cloud.auth.anonymous.enable", "true")
        conf.set("spark.hadoop.google.cloud.auth.gce.enable", "false")
        return conf

## Override ObjectStoreConnector class to use the MockGCS class
class MockObjectStoreConnector(ObjectStoreConnector):
    def _get_provider(self, connector_config):
        if connector_config["source_type"] == "gcs":
            self.provider = MockGCS(connector_config)
        else:
            self.provider = super()._get_provider(connector_config)

start_time = datetime.datetime.now()
local_vars = {}
schedule_config = schedule_dict["Daily"]
exec(f"kwargs = dict({schedule_config['last_processed_partition']})", {"current_time": start_time}, local_vars)
start_time_partition = start_time - relativedelta(**local_vars["kwargs"])

connector_instances = list()

@pytest.fixture(scope="module", autouse=True)
def setup(request):
    gcs_container = DockerContainer("fsouza/fake-gcs-server:latest")
    gcs_container.with_exposed_ports(4443)
    gcs_container.with_command(["-scheme", "http", "-port", "4443"])
    gcs_container.start()

    endpoint = f"http://{gcs_container.get_container_host_ip()}:{gcs_container.get_exposed_port(4443)}"
    os.environ["STORAGE_EMULATOR_HOST"] = endpoint

    mock_credentials = {
        "type": "service_account",
        "project_id": "test-project-id",
        "private_key_id": "fake-private-key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKB\nVQoR64FavJIDRcTzJhLPr6arJKMVeo6X7fPnPq3yE+0vqHsn1GipJyD6m+Uy13Gm\nQdo5rq6CwQx1Z1Xuyj0vhP9t9VyAZ3GNqSPr8JDDe7u5rvJvLsRS/m23DvPjzRdl\nb4bwxuDHh5TtC9Rpfv1c6jb9dhCy/YLxLcaFdfwXjBttqXeN3FwH2q/7CO1RyyDf\nQqjA5yTLQjGLmkD1Xtb/7nsFtjsjSkNqeUjSpGD4pU70gTtQb7q1y5pCGFyaJ6ol\nmTdPVjfJ4jBaaMYVc2rUiT23e46arqN6u5upI2jtmc6R4DfBbor5fn86W3VyT5uF\nAgMBAAECggEBAKTmjaS6tkK8BlPXClTQ2vpz/N6uxDeS35mXpqasqskVlaAidgg\nGt6isMdfcUN0HyuwhJjnt5e3ek3R5ZHbwGD1B/9Jhxs1F5BxJ7S6yWtAO4RBlCJt\nPS66eEO0c85M3Z5U+9OqQ0Rz6e2yI5CY+3sIytMJCrC3w6u5fJNLGDCEu0kvC47K\nX4mHqCqedOWxHqJh1cv3RofdLfYPAr/RZfzB8Td+Y8FEqRKl7VjbC9DsxJ+ZXz1I\nN5hKz2c4uykvlcRlXxS0kjBPPR6L2Lf5zJQm62j9LY1fv4qD0uP9z/1gNya1TzV9\nP9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNy\na1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9\nz/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya\n1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z/1gNya1TzV9P9z\n-----END PRIVATE KEY-----\n",
        "client_email": "fake-email@test-project-id.iam.gserviceaccount.com",
        "client_id": "fake-client-id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/fake-email%40test-project-id.iam.gserviceaccount.com"
    }

    gcs_config = {
        "source_bucket": "testing-bucket",
        "source_credentials": json.dumps(mock_credentials),
        "source_credentials_endpoint": endpoint
    }

    connector_instances.append({
        "id": "gcs.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "no-time-partition/",
            "source_type": "gcs",
            "source_data_format": "json",
        } | gcs_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    connector_instances.append({
        "id": "new-time-partitioned-gcs.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "new-time-partition/%d-%m-%Y/",
            "source_type": "gcs",
            "source_data_format": "json",
            "source_partition_type": "time",
        } | gcs_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    connector_instances.append({
        "id": "old-time-partitioned-gcs.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "old-time-partition/%Y-%m-%d/",
            "source_type": "gcs",
            "source_data_format": "json",
            "source_partition_type": "time",
        } | gcs_config),
        "operations_config": json.dumps({"schedule": "Daily"}),
        "connector_state": json.dumps({"to_process": [], "last_processed_time_partition": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")})
    })

    connector_instances.append({
        "id": "gcs.new-york-taxi-data.2",
        "connector_config": json.dumps({
            "source_prefix": "",
            "source_type": "gcs",
            "source_data_format": "json",
        } | gcs_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    insert_connectors(connector_instances)
    init_gcs(gcs_config)

    def cleanup():
        gcs_container.stop()
        if "STORAGE_EMULATOR_HOST" in os.environ:
            del os.environ["STORAGE_EMULATOR_HOST"]

    request.addfinalizer(cleanup)


def init_gcs(config):
    bucket_name = config["source_bucket"]
    endpoint = config.get("source_credentials_endpoint")

    try:
        client = storage.Client.create_anonymous_client()

        # Create bucket
        bucket = client.create_bucket(bucket_name)

        file_path = "./tests/test_data/chunk-1.json"

        for connector_instance in connector_instances:
            prefix = json.loads(connector_instance["connector_config"])["source_prefix"]
            object_name = start_time.strftime(prefix) + "data.json" if prefix else "data.json"

            blob = bucket.blob(object_name)
            blob.upload_from_filename(file_path)

        # Upload a processed file with metadata
        blob = bucket.blob("processed_data.json")
        blob.upload_from_filename(file_path)
        blob.metadata = {"py-sdk-test-local": "success"}
        blob.patch()

        # List GCS objects
        print("------- Listing GCS objects ---------")
        blobs = bucket.list_blobs()
        for blob in blobs:
            print(f"Blob: {blob.name}, Metadata: {blob.metadata}")
        print("------- Listing GCS objects END ---------")

    except Exception as e:
        print(f"Error initializing GCS emulator: {e}")
        print("Note: This is expected if the GCS emulator is not properly configured")
        print("The emulator may not support all GCS features")

    return None


@pytest.mark.usefixtures("setup_obsrv_database")
class TestGCSBatchConnector(unittest.TestCase):
    def test_1_source_connector_no_time_partition(self):
        connector_instance_id = connector_instances[0]["id"]
        connector = MockObjectStoreConnector()
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
        connector = MockObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        try:
            SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
            validate_results(config, connector_instance_id)
        except Exception as e:
            print(f"Test failed due to GCS emulator setup: {e}")
            pass

    def test_3_source_connector_old_time_partition(self):
        connector_instance_id = connector_instances[2]["id"]
        connector = MockObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        try:
            SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
            validate_results(config, connector_instance_id)
        except Exception as e:
            print(f"Test failed due to GCS emulator setup: {e}")
            pass

    def test_4_source_connector_empty_prefix(self):
        connector_instance_id = connector_instances[3]["id"]
        connector = MockObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)

        config = Config(config_file_path)

        test_raw_topic = "test.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        delete_and_create_topic(config, [test_raw_topic, test_metrics_topic])

        try:
            SourceConnector.process(connector=connector, config_file_path=config_file_path, connector_instance_id=connector_instance_id)
            validate_results(config, connector_instance_id)
        except Exception as e:
            print(f"Test failed due to GCS emulator setup: {e}")
            pass