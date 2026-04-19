import os
from pathlib import Path

# Auto-load .env if it exists next to this file (development convenience)
try:
    from dotenv import load_dotenv
    _env_file = Path(__file__).parent / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
except ImportError:
    pass  # python-dotenv not installed; rely on real environment variables

# ─── Confluent Cloud Flink REST API ─────────────────────────────────────────
# Required identifiers – set via environment variables (recommended) or
# override the fallback strings with your actual values.

FLINK_CLOUD_REGION   = os.getenv("FLINK_CLOUD_REGION",   "us-east-2")
FLINK_CLOUD_PROVIDER = os.getenv("FLINK_CLOUD_PROVIDER", "aws")

# Base URL is derived from region + provider (standard Confluent pattern)
FLINK_API_URL = os.getenv(
    "FLINK_API_URL",
    f"https://flink.{FLINK_CLOUD_REGION}.{FLINK_CLOUD_PROVIDER}.confluent.cloud"
)

# Confluent Cloud org / environment / compute-pool  ← required by the REST API
ORG_ID           = os.getenv("CONFLUENT_ORG_ID",          "")
ENV_ID           = os.getenv("CONFLUENT_ENV_ID",          "")
COMPUTE_POOL_ID  = os.getenv("CONFLUENT_COMPUTE_POOL_ID", "")

# API Key & Secret for the Flink service account
API_KEY    = os.getenv("CONFLUENT_FLINK_API_KEY",    "")
API_SECRET = os.getenv("CONFLUENT_FLINK_API_SECRET", "")

# ─── Flink SQL Catalog / Database ────────────────────────────────────────────
DEFAULT_CATALOG  = os.getenv("FLINK_DEFAULT_CATALOG",  "kt_environment")
DEFAULT_DATABASE = os.getenv("FLINK_DEFAULT_DATABASE", "fms-tableflow-demo")

# ─── Confluent Cloud Kafka Cluster credentials ───────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_API_KEY           = os.getenv("KAFKA_API_KEY", "")
KAFKA_API_SECRET        = os.getenv("KAFKA_API_SECRET", "")

# ─── Confluent Schema Registry credentials ───────────────────────────────────
# Supports both canonical and short env var names.
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", os.getenv("SR_SERVER_URL", ""))
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY", os.getenv("SR_API_KEY", ""))
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET", os.getenv("SR_API_SECRET", ""))


# ─── Validation ──────────────────────────────────────────────────────────────

def validate_flink_config() -> list[str]:
    """Return a list of missing-but-required Flink config variables."""
    required = {
        "CONFLUENT_ORG_ID":          ORG_ID,
        "CONFLUENT_ENV_ID":          ENV_ID,
        "CONFLUENT_COMPUTE_POOL_ID": COMPUTE_POOL_ID,
        "CONFLUENT_FLINK_API_KEY":   API_KEY,
        "CONFLUENT_FLINK_API_SECRET": API_SECRET,
    }
    return [k for k, v in required.items() if not v]


def validate_kafka_config() -> list[str]:
    """Return a list of missing-but-required Kafka config variables."""
    required = {
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
        "KAFKA_API_KEY":           KAFKA_API_KEY,
        "KAFKA_API_SECRET":        KAFKA_API_SECRET,
    }
    return [k for k, v in required.items() if not v]