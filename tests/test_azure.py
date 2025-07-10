import os
import sys
import json
import unittest
import datetime

import pytest
from dateutil.relativedelta import relativedelta

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import AzureError
from testcontainers.azurite import AzuriteContainer

from obsrv.utils import Config
from obsrv.connector.batch import SourceConnector

sys.path.insert(0, os.path.join(os.getcwd(), "object_store_connector"))

from object_store_connector.connector import ObjectStoreConnector, schedule_dict
from object_store_connector.provider.azure import AzureBlobStorage

from tests.batch_setup import setup_obsrv_database, create_spark_session, validate_results, delete_and_create_topic, insert_connectors  # noqa


start_time = datetime.datetime.now()
local_vars = {}
schedule_config = schedule_dict["Daily"]
exec(f"kwargs = dict({schedule_config['last_processed_partition']})", {"current_time": start_time}, local_vars)
start_time_partition = start_time - relativedelta(**local_vars["kwargs"])

connector_instances = list()

@pytest.fixture(scope="module", autouse=True)
def setup(request):
    azurite_container = AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:latest")
    azurite_container.with_bind_ports(container=10000, host=10000)
    azurite_container.start()
    azurite_account_name = "devstoreaccount1"
    azurite_account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    azurite_endpoint = f"http://127.0.0.1:{azurite_container.get_exposed_port(10000)}/devstoreaccount1"
    azurite_conn_string = azurite_container.get_connection_string()
    azurite_config = {
        "source_container_name": "testing-container",
        "source_credentials_account_name": azurite_account_name,
        "source_credentials_account_key": azurite_account_key,
        "source_blob_endpoint": azurite_endpoint,
        "source_connection_string": azurite_conn_string,
    }

    connector_instances.append({
        "id": "azure.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "no-time-partition/",
            "source_type": "azure_blob",
            "source_data_format": "json",
        } | azurite_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    connector_instances.append({
        "id": "new-time-partitioned-azure.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "new-time-partition/%d-%m-%Y/",
            "source_type": "azure_blob",
            "source_data_format": "json",
            "source_partition_type": "time",
        } | azurite_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    connector_instances.append({
        "id": "old-time-partitioned-azure.new-york-taxi-data.1",
        "connector_config": json.dumps({
            "source_prefix": "old-time-partition/%Y-%m-%d/",
            "source_type": "azure_blob",
            "source_data_format": "json",
            "source_partition_type": "time",
        } | azurite_config),
        "operations_config": json.dumps({"schedule": "Daily"}),
        "connector_state": json.dumps({"to_process": [], "last_processed_time_partition": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")})
    })

    connector_instances.append({
        "id": "azure.new-york-taxi-data.2",
        "connector_config": json.dumps({
            "source_prefix": "",
            "source_type": "azure_blob",
            "source_data_format": "json",
        } | azurite_config),
        "operations_config": json.dumps({"schedule": "Daily"})
    })

    insert_connectors(connector_instances)
    init_azurite(azurite_config)

    def cleanup():
        azurite_container.stop()

    request.addfinalizer(cleanup)


def init_azurite(config):
    connection_string = config["source_connection_string"]
    container_name = config["source_container_name"]

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string
    )

    # Create Container
    try:
        container_client = blob_service_client.create_container(container_name)
    except AzureError as e:
        print(f"[ERROR] Error while creating AZURITE container: {e}")

    file_path = "./tests/test_data/chunk-1.json"
    content_settings = ContentSettings(content_type="application/json")

    # Upload Objects
    try:
        for connector_instance in connector_instances:
            prefix = json.loads(connector_instance["connector_config"])["source_prefix"]
            object_name = start_time.strftime(prefix) + "data.json" if prefix else "data.json"

            with open(file_path, "rb") as data:
                container_client.upload_blob(
                    name=object_name,
                    data=data,
                    content_settings=content_settings
                )

        # Upload a processed file with tags
        # with open(file_path, "rb") as data:
        #     container_client.upload_blob(
        #         name="processed_data.json",
        #         data=data,
        #         content_settings=content_settings,
        #         metadata={str("py-sdk-test-local"): "success"}
        #     )

        # List azurite objects
        print("------- Listing AZURITE objects ---------")
        blobs = container_client.list_blobs()
        for blob in blobs:
            print(f"Blob: {blob.name}, Tags: {blob.tags}")
        print("------- Listing AZURITE objects END ---------")

    except AzureError as e:
        print(f"Error uploading objects: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return container_client

@pytest.mark.usefixtures("setup_obsrv_database", "create_spark_session")
class TestAzureBatchConnector(unittest.TestCase):
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
