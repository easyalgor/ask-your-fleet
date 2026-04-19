"""
Confluent Cloud Flink REST API client.

Flow (per Confluent docs):
  1. POST   /sql/v1/organizations/{org}/environments/{env}/statements
            → returns a Statement object with status.phase = PENDING / RUNNING
  2. GET    .../statements/{name}           (poll until COMPLETED or FAILED)
  3. GET    .../statements/{name}/results   (paginate via metadata.next)

Authentication: HTTP Basic with API Key : API Secret  (Base-64 encoded).
"""

import time
import uuid
import base64
import logging
import requests

from config import (
    FLINK_API_URL,
    API_KEY,
    API_SECRET,
    ORG_ID,
    ENV_ID,
    COMPUTE_POOL_ID,
    DEFAULT_CATALOG,
    DEFAULT_DATABASE,
)

log = logging.getLogger(__name__)

# ── tunables ─────────────────────────────────────────────────────────────────
POLL_INTERVAL_SEC  = 2      # seconds between status polls
POLL_TIMEOUT_SEC   = 120    # give up after 2 minutes
MAX_RESULT_PAGES   = 20     # max pages of results to fetch (safety cap)

# ── helpers ───────────────────────────────────────────────────────────────────

def _auth_header() -> dict:
    """Return the Authorization header required by the Flink REST API."""
    token = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
    }


def _statements_url() -> str:
    """Base statements endpoint."""
    if not ORG_ID or not ENV_ID:
        raise ValueError(
            "CONFLUENT_ORG_ID and CONFLUENT_ENV_ID must be set. "
            "Export them as environment variables or fill them in config.py."
        )
    return (
        f"{FLINK_API_URL}/sql/v1/organizations/{ORG_ID}"
        f"/environments/{ENV_ID}/statements"
    )


def _raise_for_status(resp: requests.Response, context: str):
    """Raise a readable RuntimeError on non-2xx responses."""
    if not resp.ok:
        raise RuntimeError(
            f"{context} failed [{resp.status_code}]: {resp.text[:500]}"
        )


# ── public API ────────────────────────────────────────────────────────────────

def execute_query(sql: str, wait_for_running: bool = False) -> dict:
    """
    Submit *sql* to Confluent Cloud Flink, wait for it to finish (or reach
    RUNNING if wait_for_running is True), and return a dict with keys:

        {
          "columns": ["col1", "col2", ...],
          "rows":    [[val, val], ...],
          "status":  "COMPLETED" | "FAILED",
          "detail":  "<status detail message>",
        }

    Raises RuntimeError on API errors or if ORG_ID / ENV_ID are unset.
    """
    statement_name = f"watson-{uuid.uuid4().hex[:12]}"

    # ── 1. Submit statement ──────────────────────────────────────────────────
    payload = {
        "name": statement_name,
        "spec": {
            "statement":       sql.strip(),
            "compute_pool_id": COMPUTE_POOL_ID,
            "properties": {
                "sql.current-catalog":  DEFAULT_CATALOG,
                "sql.current-database": DEFAULT_DATABASE,
            },
        },
    }

    log.debug("Submitting Flink statement '%s': %s", statement_name, sql[:120])
    resp = requests.post(
        _statements_url(),
        headers=_auth_header(),
        json=payload,
        timeout=30,
    )
    _raise_for_status(resp, "Statement submission")
    stmt = resp.json()
    log.debug("Statement submitted. Initial phase: %s", stmt.get("status", {}).get("phase"))

    # ── 2. Poll until terminal ───────────────────────────────────────────────
    status_url = f"{_statements_url()}/{statement_name}"
    deadline   = time.time() + POLL_TIMEOUT_SEC

    while True:
        if time.time() > deadline:
            raise RuntimeError(
                f"Flink statement '{statement_name}' did not complete "
                f"within {POLL_TIMEOUT_SEC}s."
            )

        time.sleep(POLL_INTERVAL_SEC)
        resp = requests.get(status_url, headers=_auth_header(), timeout=30)
        _raise_for_status(resp, "Statement status poll")
        stmt   = resp.json()
        status = stmt.get("status", {})
        phase  = status.get("phase", "")
        detail = status.get("detail", "")
        log.debug("Statement phase: %s | %s", phase, detail)

        if phase in ("COMPLETED", "FAILED"):
            break
        if wait_for_running and phase == "RUNNING":
            log.debug("Statement has reached RUNNING phase, returning early.")
            break
        # PENDING / RUNNING / DEGRADED → keep polling

    if phase == "FAILED":
        return {
            "columns": [],
            "rows":    [],
            "status":  "FAILED",
            "detail":  detail,
        }

    if wait_for_running and phase == "RUNNING":
        return {
            "columns": [],
            "rows":    [],
            "status":  "RUNNING",
            "detail":  "Statement is executing in the background.",
        }

    # ── 3. Fetch results (paginated) ─────────────────────────────────────────
    results_url = f"{status_url}/results"
    columns: list[str] = []
    rows:    list[list] = []
    pages = 0

    while results_url and pages < MAX_RESULT_PAGES:
        resp = requests.get(results_url, headers=_auth_header(), timeout=30)
        _raise_for_status(resp, "Result fetch")
        body = resp.json()

        # Extract schema columns on first page
        if not columns:
            results_dict = body.get("results") or {}
            schema_cols = (
                results_dict
                    .get("schema", {})
                    .get("columns", [])
            )
            columns = [c.get("name", f"col{i}") for i, c in enumerate(schema_cols)]

        # Extract data rows
        results_dict = body.get("results") or {}
        for data_row in results_dict.get("data") or []:
            row = data_row.get("row", [])
            rows.append(row)

        # Follow pagination
        results_url = body.get("metadata", {}).get("next") or ""
        pages += 1

    log.info(
        "Statement '%s' completed: %d rows, %d columns.",
        statement_name, len(rows), len(columns)
    )

    return {
        "columns": columns,
        "rows":    rows,
        "status":  "COMPLETED",
        "detail":  detail,
    }