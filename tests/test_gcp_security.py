from __future__ import annotations

import json
import sys
import types
from datetime import UTC, datetime

from architron_monitoring_airflow.gcp_inventory import GCPMonitoringConfig
from architron_monitoring_airflow.gcp_security import GCPSecurityScanner
from architron_monitoring_airflow.production import SQLCommand


class _Response:
    def __init__(self, payload: dict):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class _Session:
    def __init__(self, credentials: object):
        self.calls = []

    def request(self, method: str, url: str, **kwargs: object) -> _Response:
        self.calls.append((method, url, kwargs))
        return _Response({
            "listFindingsResults": [{
                "finding": {
                    "name": "projects/demo-12345/sources/source-1/findings/finding-1",
                    "category": "PUBLIC_BUCKET_ACL",
                    "state": "ACTIVE",
                    "severity": "HIGH",
                    "mute": "UNDEFINED",
                    "findingClass": "MISCONFIGURATION",
                    "resourceName": "//storage.googleapis.com/projects/_/buckets/demo",
                    "eventTime": "2026-09-04T01:02:03Z",
                    "createTime": "2026-09-01T01:02:03Z",
                    "sourceProperties": {"secretToken": "must-not-persist", "control": "CIS"},
                },
                "resource": {"name": "//storage.googleapis.com/projects/_/buckets/demo", "type": "storage.googleapis.com/Bucket"},
            }],
        })


class _Postgres:
    def __init__(self) -> None:
        self.commands: list[SQLCommand] = []

    def transaction(self, commands: list[SQLCommand]) -> None:
        self.commands.extend(commands)


def test_scanner_lists_findings_and_closes_missing(monkeypatch) -> None:
    google = types.ModuleType("google")
    auth = types.ModuleType("google.auth")
    transport = types.ModuleType("google.auth.transport")
    requests = types.ModuleType("google.auth.transport.requests")
    requests.AuthorizedSession = _Session
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.auth", auth)
    monkeypatch.setitem(sys.modules, "google.auth.transport", transport)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", requests)

    postgres = _Postgres()
    scanner = GCPSecurityScanner(object(), postgres, GCPMonitoringConfig(("demo-12345",), page_size=100))
    counts = scanner.scan("scheduled__2026-09-04", datetime(2026, 9, 4, tzinfo=UTC))
    assert counts == {"findings": 1, "projects": 1}
    assert len(postgres.commands) == 2
    assert "projects/demo-12345/sources/-/findings" in scanner.session.calls[0][1]
    record = postgres.commands[0]
    assert "record_gcp_security_finding_snapshot" in record.sql
    assert "secretToken" not in json.dumps(record.parameters, default=str)
    assert "close_gcp_security_finding_missing" in postgres.commands[1].sql
