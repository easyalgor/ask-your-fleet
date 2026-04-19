"""
SQL query builders for the Confluent Cloud Flink tables.

All functions return a plain SQL string that is submitted to the Flink
REST API via flink_client.execute_query().
"""

from __future__ import annotations

import re

from config import DEFAULT_CATALOG, DEFAULT_DATABASE

# Fully qualified reference to the source table
CATALOG = DEFAULT_CATALOG
DATABASE = DEFAULT_DATABASE
TABLE = f"`{CATALOG}`.`{DATABASE}`.`device_health_5min`"


def _sanitize_str(value: str) -> str:
    """Allow only alphanumerics, hyphens and underscores in identifiers."""
    return re.sub(r"[^a-zA-Z0-9_\-]", "", value.strip())


def _sanitize_limit(limit: int, default: int = 10) -> int:
    try:
        limit = int(limit)
    except Exception:
        return default
    return max(1, min(limit, 100))


def latest_device_health(device_id: str) -> str:
    device_id = _sanitize_str(device_id)

    return (
        f"SELECT deviceId, avg_temp, avg_vibration, health_status, window_start "
        f"FROM {TABLE} "
        f"WHERE deviceId = '{device_id}' "
        f"LIMIT 20"
    )


def device_trend(device_id: str, limit: int = 10) -> str:
    device_id = _sanitize_str(device_id)
    limit = _sanitize_limit(limit)

    return (
        f"SELECT window_start, avg_temp, avg_vibration, health_status "
        f"FROM {TABLE} "
        f"WHERE deviceId = '{device_id}' "
        f"ORDER BY window_start DESC "
        f"LIMIT {limit}"
    )


def detect_anomalies(limit: int = 20) -> str:
    limit = _sanitize_limit(limit, default=20)

    return (
        f"SELECT deviceId, window_start, avg_temp, avg_vibration, health_status "
        f"FROM {TABLE} "
        f"WHERE health_status IN ('WARNING', 'CRITICAL') "
        f"ORDER BY window_start DESC "
        f"LIMIT {limit}"
    )


def list_tables() -> str:
    return f"SHOW TABLES IN `{CATALOG}`.`{DATABASE}`"


def show_schema(table_name: str) -> str:
    table_name = table_name.replace("`", "").strip()
    return f"DESCRIBE `{CATALOG}`.`{DATABASE}`.`{table_name}`"


def create_data_product_table(topic_name: str, schema_definitions: str) -> str:
    topic_name = _sanitize_str(topic_name).replace("`", "")
    return (
        f"CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{DATABASE}`.`{topic_name}` (\n"
        f"  {schema_definitions}\n"
        f")"
    )


def insert_device_health(topic_name: str, device_id: str) -> str:
    topic_name = _sanitize_str(topic_name).replace("`", "")
    device_id = _sanitize_str(device_id)
    return (
        f"INSERT INTO `{CATALOG}`.`{DATABASE}`.`{topic_name}` "
        f"SELECT deviceId, avg_temp, avg_vibration, health_status, window_start "
        f"FROM {TABLE} "
        f"WHERE deviceId = '{device_id}'"
    )

def insert_data_product(topic_name: str, select_sql: str) -> str:
    topic_name = _sanitize_str(topic_name).replace("`", "")
    return (
        f"INSERT INTO `{CATALOG}`.`{DATABASE}`.`{topic_name}` \n"
        f"{select_sql.strip()}"
    )


def read_data_product(topic_name: str, limit: int = 10) -> str:
    topic_name = _sanitize_str(topic_name).replace("`", "")
    limit = _sanitize_limit(limit, default=10)
    return f"SELECT * FROM `{CATALOG}`.`{DATABASE}`.`{topic_name}` LIMIT {limit}"


def drop_data_product(topic_name: str) -> str:
    topic_name = _sanitize_str(topic_name).replace("`", "")
    return f"DROP TABLE IF EXISTS `{CATALOG}`.`{DATABASE}`.`{topic_name}`"