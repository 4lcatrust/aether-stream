"""Smoke tests against a running docker-compose stack.

These query the ClickHouse operational-monitoring views over HTTP and skip
cleanly when ClickHouse is unreachable, so the suite is safe to run anywhere.

Run against a live stack with:  pytest -m smoke
Override the endpoint/creds via CLICKHOUSE_HTTP / CLICKHOUSE_USER /
CLICKHOUSE_PASSWORD environment variables.
"""
import os

import pytest
import requests

pytestmark = pytest.mark.smoke

CLICKHOUSE_HTTP = os.environ.get("CLICKHOUSE_HTTP", "http://localhost:8123")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "aether")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "aether")


def _query(sql: str) -> str:
    try:
        resp = requests.post(
            CLICKHOUSE_HTTP,
            params={"query": sql},
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
            timeout=5,
        )
    except requests.exceptions.RequestException as exc:
        pytest.skip(f"ClickHouse not reachable at {CLICKHOUSE_HTTP}: {exc}")
    if resp.status_code != 200:
        pytest.fail(f"ClickHouse query failed ({resp.status_code}): {resp.text.strip()}")
    return resp.text.strip()


def test_clickhouse_reachable():
    assert _query("SELECT 1") == "1"


def test_pipeline_health_view_queryable():
    rows = _query(
        "SELECT table_name, pipeline_status FROM omt.pipeline_health FORMAT TSV"
    )
    statuses = {line.split("\t")[1] for line in rows.splitlines() if line}
    # The view always reports a known status; 'stale' means the pipeline stalled.
    assert statuses <= {"healthy", "delayed", "stale", "empty"}
    assert "stale" not in statuses, f"pipeline is stale: {rows}"


def test_sink_validation_view_queryable():
    rows = _query(
        "SELECT table_name, validation_status FROM omt.sink_validation FORMAT TSV"
    )
    statuses = {line.split("\t")[1] for line in rows.splitlines() if line}
    assert statuses <= {"HEALTHY", "MISMATCH"}
