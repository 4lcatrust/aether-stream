"""Unit tests for the ingestion service utilities (no DB or network)."""
from datetime import timezone

import pytest

import service_ingest_aether as svc


class TestParseMarketTs:
    def test_z_suffix_is_utc(self):
        dt = svc.parse_market_ts("2024-01-02T03:04:05Z")
        assert dt.tzinfo is not None
        assert dt.utcoffset() == timezone.utc.utcoffset(None)
        assert (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) == (
            2024, 1, 2, 3, 4, 5,
        )

    def test_explicit_offset_is_preserved(self):
        dt = svc.parse_market_ts("2024-01-02T03:04:05+07:00")
        assert dt.utcoffset().total_seconds() == 7 * 3600

    def test_fractional_seconds(self):
        dt = svc.parse_market_ts("2024-01-02T03:04:05.123456Z")
        assert dt.microsecond == 123456


class TestLoadDbConfig:
    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "db")
        monkeypatch.setenv("PGPORT", "6543")
        monkeypatch.setenv("PGDATABASE", "aether")
        monkeypatch.setenv("PGUSER", "u")
        monkeypatch.setenv("PGPASSWORD", "p")
        cfg = svc.load_db_config()
        assert cfg == {
            "host": "db",
            "port": 6543,
            "dbname": "aether",
            "user": "u",
            "password": "p",
        }

    def test_port_defaults_to_5432(self, monkeypatch):
        for k in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
            monkeypatch.setenv(k, "x")
        monkeypatch.delenv("PGPORT", raising=False)
        assert svc.load_db_config()["port"] == 5432

    def test_missing_required_var_raises(self, monkeypatch):
        monkeypatch.delenv("PGHOST", raising=False)
        with pytest.raises(KeyError):
            svc.load_db_config()
