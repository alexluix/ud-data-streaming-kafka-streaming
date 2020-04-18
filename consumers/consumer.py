"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
            self,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://0.0.0.0:9092,PLAINTEXT://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9094",
            "group.id": "website-consumer-0"
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://0.0.0.0:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(
                {
                    "bootstrap.servers": self.broker_properties["bootstrap.servers"],
                    "group.id": self.broker_properties["group.id"]
                }
            )

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        logger.info("assigning partitions..")
        for partition in partitions:
            if self.offset_earliest is True:
                partition.offset = OFFSET_BEGINNING

        consumer.assign(partitions)
        logger.info("partitions assigned for %s", self.topic_name_pattern)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        while True:

            try:
                message = self.consumer.poll(timeout=self.consume_timeout)

                if message is None:
                    return 0
                elif message.error():
                    logger.error(f"Unable to consume message: {message.error()}")
                else:
                    self.message_handler(message)
                    return 1
            except Exception as e:
                logger.error(f"failed to consume message: {e}")

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
