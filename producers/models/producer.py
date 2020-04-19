"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": "0.0.0.0:9092"
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        avro_producer_config = self.broker_properties
        avro_producer_config["schema.registry.url"] = "http://0.0.0.0:8081"
        self.producer = AvroProducer(
            config=avro_producer_config
        )

    def do_create_topic(self, client, topic_name):
        futures = client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=6, replication_factor=1)]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} created")
            except Exception as e:
                print(f"Failed to create topic {topic_name}: {e}")
                raise

    def delete_topic(self, client, topic_name):
        futures = client.delete_topics([topic_name])

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} deleted")
            except Exception as e:
                print(f"Failed to delete topic {topic_name}: {e}")
                raise

    def topic_exists(self, client, topic_name):
        topics_metadata = client.list_topics(timeout=5)
        return topics_metadata.topics.get(topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(self.broker_properties)

        exists = self.topic_exists(client, self.topic_name)
        print(f"Topic {self.topic_name} exists: {exists}")

        if exists is False:
            self.do_create_topic(client, self.topic_name)

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("producer flushed and ready for closing")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
