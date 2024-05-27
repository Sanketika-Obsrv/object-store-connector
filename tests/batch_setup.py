import os
import pytest
import yaml
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from tests.create_tables import create_tables

# import psycopg2


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

        config["connector-instance-id"] = "s3.new-york-taxi-data.1"

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
