import os
import sys
import json
import unittest

import psycopg2
import pytest
import yaml

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import AzureError
from kafka import KafkaConsumer, TopicPartition
from testcontainers.azurite import AzuriteContainer

from obsrv.utils import EncryptionUtil, Config
from obsrv.connector.batch import SourceConnector

sys.path.insert(0, os.path.join(os.getcwd(), "object_store_connector"))

from object_store_connector.connector import ObjectStoreConnector

from tests.batch_setup import setup_obsrv_database  # noqa


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    azurite_container = AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:latest")
    azurite_container.with_bind_ports(container=10000, host=10000)
    azurite_container.start()
    azurite_account_name = "devstoreaccount1"
    azurite_account_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    azurite_endpoint = f"http://127.0.0.1:{azurite_container.get_exposed_port(10000)}/devstoreaccount1"
    azurite_conn_string = azurite_container.get_connection_string()
    connector_instance_id = "azure.new-york-taxi-data.1"

    connector_config = json.dumps({
        "source_type": "azure_blob",
        "source_container_name": "testing-container",
        "source_credentials_account_name": azurite_account_name,
        "source_credentials_account_key": azurite_account_key,
        "source_blob_endpoint": azurite_endpoint,
        "source_connection_string": azurite_conn_string,
        "source_prefix": "",
        "source_data_format": "json"
    })
    insert_connector(connector_config, connector_instance_id)
    init_azurite(connector_config)
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml")
    ) as config_file:
        config = yaml.safe_load(config_file)
        config["connector_instance_id"] = connector_instance_id
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "w"
    ) as config_file:
        yaml.dump(config, config_file)

    def cleanup():
        azurite_container.stop()

    request.addfinalizer(cleanup)


def init_azurite(connector_config):
    config = json.loads(connector_config)
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

    object_name = "data.json"
    file_path = "./tests/test_data/chunk-1.json"
    content_settings = ContentSettings(content_type="application/json")

    # Upload Objects
    try:
        with open(file_path, "rb") as data:
            container_client.upload_blob(
                name=object_name,
                data=data,
                content_settings=content_settings
            )

    except AzureError as e:
        print(f"Error uploading {object_name} : {e}")
    except FileNotFoundError:
        print(f"File not found: {object_name}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return container_client


def insert_connector(connector_config, connector_instance_id):
    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "r"
    ) as config_file:
        config = yaml.safe_load(config_file)

    enc = EncryptionUtil(config["obsrv_encryption_key"])
    enc_config = enc.encrypt(connector_config)

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
            'azure-connector-0.1.0',
            'azure-connector',
            'Azure connector',
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
            'azure-connector-0.1.0',
            %s,
            '{}',
            'Live',
            '{}',
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
    cur.execute(ins_ci, (connector_instance_id, json.dumps(enc_config),))

    conn.commit()
    conn.close()


# @pytest.mark.usefixtures("setup_obsrv_database")
class TestBatchConnector(unittest.TestCase):

    def test_source_connector(self):
        connector = ObjectStoreConnector()
        config_file_path = os.path.join(os.path.dirname(__file__), "config/config.yaml")

        self.assertEqual(os.path.exists(config_file_path), True)
        config = Config(config_file_path)

        test_raw_topic = "dev.ingest" # default topic from datasets table
        test_metrics_topic = config.find("kafka.connector-metrics-topic")

        kafka_consumer = KafkaConsumer(
            bootstrap_servers=config.find("kafka.broker-servers"),
            group_id="azure-test-group",
            enable_auto_commit=True,
            auto_offset_reset='earliest'
        )

        trt_consumer = TopicPartition(test_raw_topic, 0)
        tmt_consumer = TopicPartition(test_metrics_topic, 0)

        kafka_consumer.assign([trt_consumer, tmt_consumer])

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

        api_calls_list_blobs, errors_list_blobs = 0, 0
        api_calls_get_blob_tags, errors_get_blob_tags = 0, 0
        api_calls_get_blob, errors_get_blob = 0, 0
        api_calls_set_blob_tags, errors_set_blob_tags = 0, 0

        for metric in metrics:
            for d in metric["edata"]["labels"]:
                if "method_name" != d["key"]:
                    continue
                if d["value"] == "listBlobs":
                    api_calls_list_blobs += metric["edata"]["metric"]["num_api_calls"]
                    errors_list_blobs += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getBlobTags":
                    api_calls_get_blob_tags += metric["edata"]["metric"]["num_api_calls"]
                    errors_get_blob_tags += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "getBlob":
                    api_calls_get_blob += metric["edata"]["metric"]["num_api_calls"]
                    errors_get_blob += metric["edata"]["metric"]["num_errors"]
                    break
                if d["value"] == "setBlobTags":
                    api_calls_set_blob_tags += metric["edata"]["metric"]["num_api_calls"]
                    errors_set_blob_tags += metric["edata"]["metric"]["num_errors"]
                    break

        print(f"listBlobs requests: {api_calls_list_blobs}, errors: {errors_list_blobs}")
        print(f"getBlobTags requests: {api_calls_get_blob_tags}, errors: {errors_get_blob_tags}")
        print(f"getBlob requests: {api_calls_get_blob}, errors: {errors_get_blob}")
        print(f"setBlobTags requests: {api_calls_set_blob_tags}, errors: {errors_set_blob_tags}")
        print(f"Total exec time: {metrics[-1]["edata"]["metric"]["total_exec_time_ms"]}")
        print(f"Framework exec time: {metrics[-1]["edata"]["metric"]["fw_exec_time_ms"]}")
        print(f"Connector exec time: {metrics[-1]["edata"]["metric"]["connector_exec_time_ms"]}")

        # Api call count asserts
        assert api_calls_get_blob_tags  \
            == api_calls_get_blob       \
            == api_calls_set_blob_tags  \
            == obj_count

        # Number of times metrics_collector.collect() should be called
        # SDK => 1 execution metric, connector => 1 num_objects_discovered metric, so + 2
        expected_metric_count = api_calls_list_blobs + 3 * obj_count + 2

        # Kafka asserts
        assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_record_count}
        assert kafka_consumer.end_offsets([tmt_consumer]) == {tmt_consumer: expected_metric_count}

        # Objects discovered metrics
        assert metrics[
            int(api_calls_list_blobs + api_calls_get_blob_tags)
        ]["edata"]["metric"]["new_objects_discovered"] == obj_count

        # Execution metrics
        assert metrics[-1]["edata"]["metric"]["total_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["success_records_count"] == expected_record_count
        assert metrics[-1]["edata"]["metric"]["failed_records_count"] == 0
