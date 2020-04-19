"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://0.0.0.0:9092", store="memory://")
topic = app.topic("cta.data2.connect.stations", value_type=Station)
out_topic = app.topic("cta.data2.stations.table", partitions=1)

table = app.Table(
    "stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)


@app.agent(topic)
async def transform_station(stream):
    async for event in stream:
        """Transform station data"""

        transformed_station = TransformedStation(
            station_id=event.station_id,
            station_name=event.station_name,
            order=event.order,
            line=get_line(event)
        )
        table[event.station_id] = transformed_station


def get_line(event):
    if event.red is True:
        return 'red'
    elif event.blue is True:
        return 'blue'
    elif event.green is True:
        return 'green'
    else:
        logger.info(f"No line color for {event.station_id}")
    return None


if __name__ == "__main__":
    app.main()
