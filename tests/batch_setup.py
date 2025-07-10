import json
from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient
from kafka.admin import NewTopic
import os
import pytest
import time
import yaml
import psycopg2
import psycopg2.extras

from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

from pyspark.sql import SparkSession

from obsrv.job.batch import get_base_conf
from obsrv.utils import EncryptionUtil

def create_tables(config):
    datasets = """
        CREATE TABLE public.datasets (
            id text NOT NULL,
            dataset_id text NULL,
            "type" text NOT NULL,
            "name" text NULL,
            validation_config json NULL,
            extraction_config json NULL,
            dedup_config json NULL,
            data_schema json NULL,
            denorm_config json NULL,
            router_config json NULL,
            dataset_config json NULL,
            tags _text NULL,
            data_version int4 NULL,
            status text NULL,
            created_by text NULL,
            updated_by text NULL,
            created_date timestamp DEFAULT now() NOT NULL,
            updated_date timestamp NOT NULL,
            published_date timestamp DEFAULT now() NOT NULL,
            api_version varchar(255) DEFAULT 'v1'::character varying NOT NULL,
            "version" int4 DEFAULT 1 NOT NULL,
            sample_data json DEFAULT '{}'::json NULL,
            entry_topic text DEFAULT 'test.ingest'::text NOT NULL,
            CONSTRAINT datasets_pkey PRIMARY KEY (id)
        );
    """

    connector_registry = """
        CREATE TABLE public.connector_registry (
            id text NOT NULL,
            connector_id text NOT NULL,
            "name" text NOT NULL,
            "type" text NOT NULL,
            category text NOT NULL,
            "version" text NOT NULL,
            description text NULL,
            technology text NOT NULL,
            runtime text NOT NULL,
            licence text NOT NULL,
            "owner" text NOT NULL,
            iconurl text NULL,
            status text NOT NULL,
            ui_spec json DEFAULT '{}'::json NOT NULL,
            source_url text NOT NULL,
            "source" json NOT NULL,
            created_by text NOT NULL,
            updated_by text NOT NULL,
            created_date timestamp NOT NULL,
            updated_date timestamp NOT NULL,
            live_date timestamp NULL,
            CONSTRAINT connector_registry_connector_id_version_key UNIQUE (connector_id, version),
            CONSTRAINT connector_registry_pkey PRIMARY KEY (id)
        );
    """

    connector_instances = """
        CREATE TABLE public.connector_instances (
            id text NOT NULL,
            dataset_id text NOT NULL,
            connector_id text NOT NULL,
            connector_config text NOT NULL,
            operations_config json NOT NULL,
            status text NOT NULL,
            connector_state json DEFAULT '{}'::json NOT NULL,
            connector_stats json DEFAULT '{}'::json NOT NULL,
            created_by text NOT NULL,
            updated_by text NOT NULL,
            created_date timestamp NOT NULL,
            updated_date timestamp NOT NULL,
            published_date timestamp NOT NULL,
            "name" text NULL,
            CONSTRAINT connector_instances_pkey PRIMARY KEY (id),
            CONSTRAINT connector_instances_connector_id_fkey FOREIGN KEY (connector_id) REFERENCES public.connector_registry(id),
            CONSTRAINT connector_instances_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES public.datasets(id)
        );
    """

    ins_ds = """
        INSERT INTO datasets (
            id,
            dataset_id,
            "type",
            "name",
            validation_config,
            extraction_config,
            dedup_config,
            data_schema,
            denorm_config,
            router_config,
            dataset_config,
            tags,
            data_version,
            status,
            created_by,
            updated_by,
            created_date,
            updated_date,
            published_date,
            api_version,
            sample_data)
        VALUES(
            'new-york-taxi-data',
            'new-york-taxi-data',
            'event',
            'New York Taxi Data',
            '{"validate": true, "mode": "Strict", "validation_mode": "Strict"}',
            '{"is_batch_event": true}',
            '{"drop_duplicates": true, "dedup_key": "tripID", "dedup_period": 604800}',
            '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"tripID":{"type":"string","suggestions":[{"message":"The Property tripID appears to be uuid format type.","advice":"Suggest to not to index the high cardinal columns","resolutionType":"DEDUP","severity":"LOW","path":"properties.tripID"}],"arrival_format":"text","data_type":"string"}},"additionalProperties":false}',
            '{}',
            '{"topic": "new-york-taxi-data"}',
            '{"data_key": "", "timestamp_key": "tpep_pickup_datetime", "exclude_fields": [], "entry_topic": "azure.ingest", "redis_db_host": "obsrv-dedup-redis-master.redis.svc.cluster.local", "redis_db_port": 6379, "index_data": true, "redis_db": 0}',
            '{}',
            '2',
            'Live',
            'SYSTEM',
            'SYSTEM',
            now(),
            now(),
            now(),
            'v2',
            '{"mergedEvent": {"tripID": "4c77e9d5-538d-4eb7-8db1-4c2c32860aa8", "VendorID": "2", "tpep_pickup_datetime": "2024-09-23 00:18:42", "tpep_dropoff_datetime": "2024-09-23 00:24:38", "passenger_count": "1", "trip_distance": "1.60", "RatecodeID": "1", "store_and_fwd_flag": "N", "PULocationID": "236", "DOLocationID": "239", "payment_type": "2", "primary_passenger": {"email": "Willa67@gmail.com", "mobile": "1-720-981-6399 x77055"}, "fare_details": {"fare_amount": 7, "extra": 0.5, "mta_tax": 0.5, "tip_amount": 0, "tolls_amount": 0, "improvement_surcharge": 0.3, "total_amount": 8.3, "congestion_surcharge": 0}}}');
    """

    conn = psycopg2.connect(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        dbname=config["postgres"]["dbname"],
    )

    cur = conn.cursor()

    cur.execute(datasets)
    cur.execute(connector_registry)
    cur.execute(connector_instances)

    cur.execute(ins_ds)

    conn.commit()
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def setup_obsrv_database(request):
    postgres = PostgresContainer("postgres:latest")
    kafka = KafkaContainer("confluentinc/cp-kafka:latest")

    postgres.start()
    kafka.start()

    with open(
        os.path.join(os.path.dirname(__file__), "config/config.template.yaml")
    ) as config_file:
        config = yaml.safe_load(config_file)

        config["postgres"]["host"] = postgres.get_container_host_ip()
        config["postgres"]["port"] = postgres.get_exposed_port(5432)
        config["postgres"]["user"] = postgres.username
        config["postgres"]["password"] = postgres.password
        config["postgres"]["dbname"] = postgres.dbname
        config["kafka"]["broker-servers"] = kafka.get_bootstrap_server()

    with open(
        os.path.join(os.path.dirname(__file__), "config/config.yaml"), "w"
    ) as config_file:
        yaml.dump(config, config_file)

    create_tables(config)

    # clean up
    def remove_container():
        postgres.stop()
        kafka.stop()
        try:
            os.remove(os.path.join(os.path.dirname(__file__), "config/config.yaml"))
        except FileNotFoundError:
            print("config file already removed")

    request.addfinalizer(remove_container)

@pytest.fixture(scope="session", autouse=True)
def create_spark_session(request):
    spark_conf = get_base_conf()
    spark_conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-azure:3.3.1")

    assert os.path.exists("libs/gcs-connector-3.1.3-shaded.jar")

    spark_conf.set("spark.jars", "libs/gcs-connector-3.1.3-shaded.jar")

    sc = SparkSession.builder.appName("object-store-connector-test").config(conf=spark_conf).getOrCreate()
    sc.stop()

def validate_results(config, connector_instance_id):
    test_raw_topic = "test.ingest"
    test_metrics_topic = config.find("kafka.connector-metrics-topic")

    kafka_consumer = KafkaConsumer(
        bootstrap_servers=config.find("kafka.broker-servers"),
        group_id="azure-group",
        enable_auto_commit=True,
        auto_offset_reset='earliest'
    )

    trt_consumer = TopicPartition(test_raw_topic, 0)
    tmt_consumer = TopicPartition(test_metrics_topic, 0)

    kafka_consumer.assign([trt_consumer, tmt_consumer])

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
    cur.execute(f"SELECT * FROM connector_instances WHERE id = '{connector_instance_id}';")
    connector_instance = cur.fetchone()
    connector_state = connector_instance["connector_state"]
    connector_stats = connector_instance["connector_stats"]
    conn.close()

    obj_count = 1
    records_per_obj = 200
    expected_record_count = obj_count * records_per_obj

    metrics = []
    all_messages = kafka_consumer.poll(timeout_ms=10000)
    while len(metrics) < kafka_consumer.end_offsets([tmt_consumer])[tmt_consumer]:
        print(f"polling metrics for instance {connector_instance_id} ....")
        for topic_partition, messages in all_messages.items():
            for message in messages:
                if topic_partition.topic == test_metrics_topic:
                    metrics.append(json.loads(message.value))
        time.sleep(1)
        all_messages = kafka_consumer.poll(timeout_ms=10000)

    api_calls_list_blobs, errors_list_blobs = 0, 0
    api_calls_get_blob_tags, errors_get_blob_tags = 0, 0
    api_calls_get_blob, errors_get_blob = 0, 0
    api_calls_set_blob_tags, errors_set_blob_tags = 0, 0

    final_metric_object = dict()
    num_objects_discovered = 0
    for metric in metrics:
        if "metric" in metric["edata"] and "total_exec_time_ms" in metric["edata"]["metric"]:
            final_metric_object = metric

        if "new_objects_discovered" in metric["edata"]["metric"]:
            num_objects_discovered += metric["edata"]["metric"]["new_objects_discovered"]

        for d in metric["edata"]["labels"]:
            if "method_name" != d["key"]:
                continue
            if d["value"] == "listBlobs" or d["value"] == "ListObjectsV2" or d["value"] == "listObjects":
                api_calls_list_blobs += metric["edata"]["metric"]["num_api_calls"]
                errors_list_blobs += metric["edata"]["metric"]["num_errors"]
                break
            if d["value"] == "getBlobTags" or d["value"] == "getObjectTagging" or d["value"] == "getBlobMetadata":
                api_calls_get_blob_tags += metric["edata"]["metric"]["num_api_calls"]
                errors_get_blob_tags += metric["edata"]["metric"]["num_errors"]
                break
            if d["value"] == "getBlob" or d["value"] == "getObject" or d["value"] == "getBlob":
                api_calls_get_blob += metric["edata"]["metric"]["num_api_calls"]
                errors_get_blob += metric["edata"]["metric"]["num_errors"]
                break
            if d["value"] == "setBlobTags" or d["value"] == "setObjectTagging" or d["value"] == "updateBlobMetadata":
                api_calls_set_blob_tags += metric["edata"]["metric"]["num_api_calls"]
                errors_set_blob_tags += metric["edata"]["metric"]["num_errors"]
                break

    print("Connector State: ", connector_state)
    print("Connector Stats: ", connector_stats)
    print("Metrics: ", json.dumps(metrics))
    print(f"listBlobs/ListObjectsV2/listObjects requests: {api_calls_list_blobs}, errors: {errors_list_blobs}")
    print(f"getBlobTags/getObjectTagging/getBlobMetadata requests: {api_calls_get_blob_tags}, errors: {errors_get_blob_tags}")
    print(f"getBlob/getObject/getBlob requests: {api_calls_get_blob}, errors: {errors_get_blob}")
    print(f"setBlobTags/setObjectTagging/updateBlobMetadata requests: {api_calls_set_blob_tags}, errors: {errors_set_blob_tags}")
    print(f"Total exec time: {final_metric_object["edata"]["metric"]["total_exec_time_ms"]}")
    print(f"Framework exec time: {final_metric_object["edata"]["metric"]["fw_exec_time_ms"]}")
    print(f"Connector exec time: {final_metric_object["edata"]["metric"]["connector_exec_time_ms"]}")
    print(f"Total records count: {final_metric_object["edata"]["metric"]["total_records_count"]}")

    # Postgres asserts
    assert connector_stats["num_files_discovered"] == obj_count
    assert connector_stats["num_files_processed"] == obj_count
    assert connector_state["to_process"] == []

    # Kafka asserts
    assert kafka_consumer.end_offsets([trt_consumer]) == {trt_consumer: expected_record_count}

    # Execution metrics
    assert final_metric_object["edata"]["metric"]["total_records_count"] == expected_record_count

    time.sleep(10)

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
            'object-store-connector',
            'azure-connector',
            'Azure Blob Storage',
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
            'object-store-connector',
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
        operations_config = instance.get("operations_config", '{}')
        connector_state = instance.get("connector_state", '{}')
        cur.execute(ins_ci, (connector_instance_id, json.dumps(enc_config), operations_config, connector_state,))

    conn.commit()
    conn.close()


def delete_and_create_topic(config, topics):
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.find("kafka.broker-servers"),
        client_id="test-client"
    )
    try:
        admin_client.delete_topics(topics)
        time.sleep(10)
    except Exception as e:
        # print("Error deleting topics: ", e)
        pass

    admin_client.create_topics([NewTopic(topic, 1, 1) for topic in topics])
    admin_client.close()