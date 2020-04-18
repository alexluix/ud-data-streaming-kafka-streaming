"""Creates a turnstile data producer"""
import logging

from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""

        super().__init__(
            f"cta.data2.station.turnstiles",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=6,
            num_replicas=1
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    key_schema=self.key_schema,
                    value={
                        "station_id": int(self.station.station_id),
                        "station_name": str(self.station.name),
                        "line": str(self.station.color.name)
                    },
                    value_schema=self.value_schema
                )

                logger.info(f"Produced event: turnstile entry of station {self.station.name}, "
                            f"line {self.station.color.name} : "
                            f"published to topic {self.topic_name}")

            except Exception as e:
                logger.error("Unable to produce turnstile event: station_id {self.station.station_id}: ", e)
                raise e
