"""
MCP server entry point for the Flink agent.

Watson Orchestrate communicates with this server over stdio using the
Model Context Protocol (MCP).  The server advertises its tools via
`tools/list` and executes them on `tools/call`.

Run locally:
    python -m agents.flink_mcp.main
or via the orchestrate CLI after importing the agent definition.
"""

from __future__ import annotations

import json
import logging
import sys
import os

# Configure logging to stderr so it does not pollute the stdio MCP channel
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)

log = logging.getLogger(__name__)

# ── import tools ──────────────────────────────────────────────────────────────
# We use relative imports when run as a module (-m flag).
# When executed directly (`python main.py`) we adjust sys.path first.

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import validate_flink_config, validate_kafka_config
from tools import (
        get_latest_device_health,
        get_device_trend,
        get_anomalies,
        list_flink_tables,
        describe_table,
        run_flink_sql,
        read_kafka_topic,
        delete_data_product,
        create_custom_data_product,
        insert_to_data_product,
    )

# ── tool registry ─────────────────────────────────────────────────────────────
TOOLS = {
    "get_latest_device_health": {
        "fn":          get_latest_device_health,
        "description": "Return the most-recent health snapshot for an IoT device.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "device_id": {"type": "string", "description": "Unique device identifier"}
            },
            "required":   ["device_id"],
        },
    },
    "get_device_trend": {
        "fn":          get_device_trend,
        "description": "Return recent temperature and vibration trend data for a device.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "device_id": {"type": "string",  "description": "Unique device identifier"},
                "limit":     {"type": "integer", "description": "Number of rows (default 10, max 100)"},
            },
            "required": ["device_id"],
        },
    },
    "get_anomalies": {
        "fn":          get_anomalies,
        "description": "Detect devices currently in WARNING or CRITICAL health status.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max records to return (default 20)"}
            },
        },
    },
    "list_flink_tables": {
        "fn":          list_flink_tables,
        "description": "List all available tables in the Flink catalog.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "describe_table": {
        "fn":          describe_table,
        "description": "Show the schema (column names and types) for a Flink table.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "table_name": {"type": "string", "description": "Table to describe"}
            },
            "required": ["table_name"],
        },
    },
    "run_flink_sql": {
        "fn":          run_flink_sql,
        "description": "Execute an arbitrary read-only Flink SQL SELECT or SHOW statement.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "sql": {"type": "string", "description": "Flink SQL SELECT or SHOW statement"}
            },
            "required": ["sql"],
        },
    },
    "read_kafka_topic": {
        "fn":          read_kafka_topic,
        "description": "Read latest messages from a specific data product or Kafka topic created by Flink.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "topic_name": {"type": "string", "description": "The name of the table/topic to read from"},
                "limit":      {"type": "integer", "description": "Max rows to return (default 10)"}
            },
            "required": ["topic_name"],
        },
    },
    "delete_data_product": {
        "fn":          delete_data_product,
        "description": "Delete a data product created for an adhoc query.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "topic_name": {"type": "string", "description": "The name of the adhoc table/topic to drop"}
            },
            "required": ["topic_name"],
        },
    },
    "create_custom_data_product": {
        "fn":          create_custom_data_product,
        "description": "Create a custom data product (Flink table + Kafka topic) dynamically based on an existing table's type setup. Schema definitions specify the columns, and select sql specifies the continuous ingestion logic.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "topic_name":         {"type": "string", "description": "The name for the new table/topic."},
                "schema_definitions": {"type": "string", "description": "The column definitions safely formatted (e.g. 'id STRING, value INT')."},
                "select_sql":         {"type": "string", "description": "The SELECT statement used to source data continually into the table."},
            },
            "required": ["topic_name", "schema_definitions", "select_sql"],
        },
    },
    "insert_to_data_product": {
        "fn":          insert_to_data_product,
        "description": "Start a continuous ingestion job into an existing data product table.",
        "inputSchema": {
            "type":       "object",
            "properties": {
                "topic_name": {"type": "string", "description": "The name of the existing table/topic."},
                "select_sql": {"type": "string", "description": "The SELECT statement used to source data continually into the table."},
            },
            "required": ["topic_name", "select_sql"],
        },
    },
}

# ── MCP protocol handlers ─────────────────────────────────────────────────────

def handle_tools_list() -> dict:
    return {
        "tools": [
            {
                "name":        name,
                "description": meta["description"],
                "inputSchema": meta["inputSchema"],
            }
            for name, meta in TOOLS.items()
        ]
    }


def handle_tools_call(name: str, arguments: dict) -> dict:
    if name not in TOOLS:
        return {
            "content": [
                {"type": "text", "text": f"Error: Unknown tool '{name}'"}
            ]
        }

    try:
        result = TOOLS[name]["fn"](**arguments)

        if not isinstance(result, str):
            result = json.dumps(result, indent=2)

        return {
            "content": [
                {"type": "text", "text": result}
            ]
        }

    except Exception as exc:
        log.exception("Tool '%s' raised an exception", name)

        return {
            "content": [
                {"type": "text", "text": f"Error: {str(exc)}"}
            ]
        }


def handle_request(req: dict) -> dict | None:
    method = req.get("method", "")
    req_id = req.get("id")

    if method == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities":    {"tools": {}},
            "serverInfo":      {"name": "flink-mcp", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = handle_tools_list()
    elif method == "tools/call":
        params = req.get("params", {})
        result = handle_tools_call(
            params.get("name", ""),
            params.get("arguments", {}),
        )
    elif method == "notifications/initialized":
        return None   # no response expected for notifications
    else:
        return {
            "jsonrpc": "2.0",
            "id":      req_id,
            "error":   {"code": -32601, "message": f"Method not found: {method}"},
        }

    return {
        "jsonrpc": "2.0",
        "id":      req_id,
        "result":  result,
    }


# ── stdio event loop ──────────────────────────────────────────────────────────

def main():
    # ── pre-flight check ─────────────────────────────────────────────────────
    missing_flink = validate_flink_config()
    if missing_flink:
        log.error(
            "Missing required Flink config: %s  — "
            "copy .env.example → .env and fill in the values.",
            ", ".join(missing_flink),
        )
        sys.exit(1)

    missing_kafka = validate_kafka_config()
    if missing_kafka:
        log.warning(
            "Missing Kafka config: %s  — "
            "read_kafka_topic tool will not work until these are set.",
            ", ".join(missing_kafka),
        )

    log.info("Flink MCP server starting (stdio mode)…")
    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            req = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            log.error("Invalid JSON: %s", exc)
            continue

        response = handle_request(req)
        if response is not None:
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()