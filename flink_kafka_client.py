#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Kafka consumer client for reading JSON / Avro data products directly
from Confluent Cloud with Schema Registry support.

Supports:
1. avro          -> Confluent Schema Registry Avro
2. json_schema   -> Confluent Schema Registry JSON Schema
3. json          -> Plain raw JSON bytes
4. auto          -> Try Avro -> JSON Schema -> raw JSON
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_API_KEY,
    KAFKA_API_SECRET,
    SCHEMA_REGISTRY_URL as SR_SERVER_URL,
    SCHEMA_REGISTRY_API_KEY as SR_API_KEY,
    SCHEMA_REGISTRY_API_SECRET as SR_API_SECRET,
)

log = logging.getLogger(__name__)


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def dict_to_obj(obj, ctx):
    return obj


def build_schema_registry_client():
    return SchemaRegistryClient({
        "url": SR_SERVER_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}"
    })


def build_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "group.id": f"python_mcp_{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.metrics.push": False,
    }
    return Consumer(conf)


def get_avro_deserializer(sr):
    return AvroDeserializer(
        schema_registry_client=sr,
        from_dict=dict_to_obj
    )


def get_json_schema_deserializer(sr):
    return JSONDeserializer(
        None,
        dict_to_obj,
        sr
    )


def deserialize_record(msg, topic, fmt, sr_client):
    ctx = SerializationContext(topic, MessageField.VALUE)
    raw = msg.value()

    if fmt == "avro":
        return get_avro_deserializer(sr_client)(raw, ctx)

    if fmt == "json_schema":
        return get_json_schema_deserializer(sr_client)(raw, ctx)

    if fmt == "json":
        return json.loads(raw.decode("utf-8"))

    # auto mode
    try:
        return get_avro_deserializer(sr_client)(raw, ctx)
    except Exception:
        pass

    try:
        return get_json_schema_deserializer(sr_client)(raw, ctx)
    except Exception:
        pass

    return json.loads(raw.decode("utf-8"))


# -------------------------------------------------------------------
# MAIN METHOD
# -------------------------------------------------------------------

def consume_topic_data(
    topic_name: str,
    limit: int = 10,
    format_type: str = "auto",
    timeout_seconds: int = 10
) -> List[Dict[str, Any]]:

    consumer = None

    try:
        sr_client = build_schema_registry_client()
        consumer = build_consumer()
        consumer.subscribe([topic_name])

        rows = []
        empty_polls = 0

        while len(rows) < limit and empty_polls < timeout_seconds:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                raise KafkaException(msg.error())

            record = deserialize_record(
                msg=msg,
                topic=msg.topic(),
                fmt=format_type.lower(),
                sr_client=sr_client
            )

            if record is not None:
                rows.append(record)

        return rows

    except Exception as e:
        log.exception("consume_topic_data failed")
        return [{"error": str(e)}]

    finally:
        if consumer:
            consumer.close()


# -------------------------------------------------------------------
# CLI TEST
# -------------------------------------------------------------------

if __name__ == "__main__":
    topic = "dp_high_temp_devices"

    rows = consume_topic_data(
        topic_name=topic,
        limit=5,
        format_type="auto"   # auto / avro / json_schema / json
    )

    print(json.dumps(rows, indent=2))