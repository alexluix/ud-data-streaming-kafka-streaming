"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import logging.config
from pathlib import Path

import requests

import topic_check

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

logger = logging.getLogger(__name__)

KSQL_URL = "http://0.0.0.0:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'cta.data2.station.turnstiles',
    VALUE_FORMAT = 'AVRO',
    KEY = 'station_id'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT = 'JSON') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        logger.info("topic already exists, KSQL execution skipped")
        return

    logger.info("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
    except:
        logger.critical(f"Failed to run KSQL queries: {json.dumps(resp.json(), indent=2)}")
        raise

    logger.info("KSQL queries executed successfully")


if __name__ == "__main__":
    execute_statement()
