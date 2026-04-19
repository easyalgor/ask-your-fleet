"""
MCP Tool definitions for Watson Orchestrate ↔ Confluent Cloud Flink.

Each public function is an individual tool that Watson Orchestrate can call.
The docstrings serve as the tool descriptions visible to the LLM.
"""

from __future__ import annotations

import json
import logging

from flink_client import execute_query
from flink_kafka_client import consume_topic_data
from query_builder import (
    latest_device_health,
    device_trend,
    detect_anomalies,
    list_tables,
    show_schema,
    create_data_product_table,
    insert_device_health,
    insert_data_product,
    read_data_product,
    drop_data_product,
)

log = logging.getLogger(__name__)


# ─── helpers ──────────────────────────────────────────────────────────────────

def _format_result(result) -> str:
    """Safely format Flink response into readable text."""

    # ✅ If already string → return directly
    if isinstance(result, str):
        return result

    # ✅ If dict → try structured parsing
    try:
        status = result.get("status", "UNKNOWN")
        detail = result.get("detail", "")

        if status == "FAILED":
            return f"❌ Query failed: {detail}"

        columns = result.get("columns", [])
        rows = result.get("rows", [])

        if not rows:
            return "✅ Query completed — no rows returned."

        header = " | ".join(columns) if columns else "result"
        sep = " | ".join(["---"] * (len(columns) or 1))

        lines = [header, sep]
        for row in rows:
            lines.append(" | ".join(str(v) for v in row))

        return f"✅ {len(rows)} row(s) returned:\n\n" + "\n".join(lines)

    except Exception:
        # ✅ Fallback — never crash MCP
        return f"Raw result:\n{str(result)}"

# ─── MCP tools ────────────────────────────────────────────────────────────────

def get_latest_device_health(device_id: str) -> str:
    """
    Return the most-recent health snapshot for a specific IoT device.
    Instead of just running a SELECT query, it generates a data product
    (a Flink table and underlying Kafka topic) for the device's health,
    continuously streams data into it, and returns the topic name and
    initial data.

    Args:
        device_id: The unique device identifier (e.g. "device-42").
    """
    import time
    topic_name = f"dp_health_{device_id.replace('-', '_')}"
    log.info("Tool: get_latest_device_health | device_id=%s creating dp=%s", device_id, topic_name)
    
    # 1. Create table
    schema_definitions = (
        "deviceId STRING, "
        "avg_temp DOUBLE, "
        "avg_vibration DOUBLE, "
        "health_status STRING, "
        "window_start TIMESTAMP(3)"
    )
    create_sql = create_data_product_table(topic_name, schema_definitions)
    execute_query(create_sql)
    
    # 2. Insert into table (does not wait for completion, only RUNNING)
    insert_sql = insert_device_health(topic_name, device_id)
    execute_query(insert_sql, wait_for_running=True)

    # 3. Fetch initial data from the product directly via Kafka Consumer
    # Pause lightly so Flink can ingest some data
    time.sleep(3)
    read_result = read_kafka_topic(topic_name, limit=10)
    
    return f"Data Product '{topic_name}' created and streaming data.\n\nInitial Results:\n{read_result}"


def get_device_trend(device_id: str, limit: int = 10) -> str:
    """
    Return recent temperature and vibration trend data for a device.

    Args:
        device_id: The unique device identifier.
        limit:     Number of time-window rows to return (default 10, max 100).

    Returns time-series rows from device_health_5min ordered by window_start.
    """
    limit = min(max(1, limit), 100)
    log.info("Tool: get_device_trend | device_id=%s limit=%d", device_id, limit)
    sql    = device_trend(device_id, limit)
    result = execute_query(sql)
    return _format_result(result)


def get_anomalies(limit: int = 20) -> str:
    """
    Detect devices currently in WARNING or CRITICAL health status.

    Args:
        limit: Maximum number of anomaly records to return (default 20).

    Scans device_health_5min for recent abnormal readings across all devices.
    """
    limit = min(max(1, limit), 100)
    log.info("Tool: get_anomalies | limit=%d", limit)
    sql    = detect_anomalies(limit)
    result = execute_query(sql)
    return _format_result(result)


def list_flink_tables() -> str:
    """
    List all available tables in the current Flink catalog and database.

    Useful for discovering which tables can be queried.
    """
    log.info("Tool: list_flink_tables")
    sql    = list_tables()
    result = execute_query(sql)
    return _format_result(result)


def describe_table(table_name: str) -> str:
    """
    Show the schema (column names and types) for a Flink table.

    Args:
        table_name: The name of the table to describe (e.g. "device_health_5min").
    """
    log.info("Tool: describe_table | table=%s", table_name)
    sql    = show_schema(table_name)
    result = execute_query(sql)
    return _format_result(result)


def run_flink_sql(sql: str) -> str:
    """
    Execute an arbitrary Flink SQL SELECT or SHOW statement. If you are facing issue, build your own statement and submit it.
    """

    if not sql or not sql.strip():
        return "❌ Empty SQL query."

    sql_stripped = sql.strip().upper()

    # ✅ Only allow safe queries
    if not (sql_stripped.startswith("SELECT") or sql_stripped.startswith("SHOW")):
        return "❌ Only SELECT and SHOW statements are permitted."

    log.info("Tool: run_flink_sql | sql=%s", sql[:100])

    try:
        result = execute_query(sql)

        # ✅ Always return string
        if not isinstance(result, str):
            result = json.dumps(result, indent=2)

        return result

    except Exception as exc:
        log.exception("run_flink_sql failed")
        return f"❌ Error executing query: {str(exc)}"


def read_kafka_topic(topic_name: str, limit: int = 10) -> str:
    """
    Read latest messages from a specific data product or Kafka topic created by Flink.
    Reads natively using a Kafka Consumer instead of Flink SQL.

    Args:
        topic_name: The name of the table/topic to read from.
        limit: Max rows to return.
    """
    limit = min(max(1, limit), 100)
    log.info("Tool: read_kafka_topic | topic=%s limit=%d", topic_name, limit)
    
    try:
        messages = consume_topic_data(topic_name, limit=limit)
        
        if not messages:
            return f"✅ Topic '{topic_name}' is currently empty or no new messages arrived in time."
            
        if "error" in messages[0]:
            return f"❌ Error reading topic: {messages[0]['error']}"
            
        formatted = "\n".join([json.dumps(msg) for msg in messages])
        return f"✅ {len(messages)} message(s) read directly from Kafka topic '{topic_name}':\n\n{formatted}"
        
    except Exception as exc:
        log.exception("read_kafka_topic failed")
        return f"❌ Error executing consumer: {str(exc)}"


def delete_data_product(topic_name: str) -> str:
    """
    Delete a data product created for an adhoc query.
    Dropping the Flink table does not automatically clean up the underlying topic unless it's managed fully,
    but it removes it from the catalog.

    Args:
        topic_name: The name of the adhoc table/topic to drop.
    """
    log.info("Tool: delete_data_product | topic=%s", topic_name)
    sql = drop_data_product(topic_name)
    result = execute_query(sql)
    # The statement might return no rows, so we wrap it
    return f"Data Product '{topic_name}' deletion requested. Status:\n{_format_result(result)}"

def create_custom_data_product(topic_name: str, schema_definitions: str, select_sql: str) -> str:
    """
    Create a custom data product (Flink table + Kafka topic) dynamically based on an existing table's type setup.
    This lets the AI build completely custom data products for the user.

    Args:
        topic_name: The name for the new table/topic.
        schema_definitions: The column definitions (e.g., 'id STRING, value INT').
        select_sql: The SELECT statement used to continually stream and populate the new table.
    """
    log.info("Tool: create_custom_data_product | topic=%s", topic_name)
    
    # 1. Create table using the provided schema definitions
    create_sql = create_data_product_table(topic_name, schema_definitions)
    execute_query(create_sql)
    
    # 2. Insert into table (runs constantly in background)
    insert_sql = insert_data_product(topic_name, select_sql)
    result = execute_query(insert_sql, wait_for_running=True)
    
    if result.get("status") == "FAILED":
        return f"❌ Failed to start ingestion for '{topic_name}': {result.get('detail')}"
        
    return f"Custom Data Product '{topic_name}' created successfully and is now ingesting data."

def insert_to_data_product(topic_name: str, select_sql: str) -> str:
    """
    Start a continuous ingestion job into an existing data product table.
    
    Args:
        topic_name: The name of the existing table/topic.
        select_sql: The SELECT statement used to source data.
    """
    log.info("Tool: insert_to_data_product | topic=%s", topic_name)
    insert_sql = insert_data_product(topic_name, select_sql)
    result = execute_query(insert_sql, wait_for_running=True)
    
    status = result.get("status")
    detail = result.get("detail", "")
    
    if status == "FAILED":
        return f"❌ Ingestion job failed for '{topic_name}': {detail}"
        
    return f"Ingestion job started for '{topic_name}'. Status: {status}"