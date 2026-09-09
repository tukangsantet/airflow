from __future__ import annotations

import json
import sys
import types
from datetime import UTC, datetime

from architron_monitoring_airflow.gcp_inventory import (
    GCPInventoryScanner,
    GCPMonitoringConfig,
    _clean,
    _fingerprint,
)
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
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, url: str, **kwargs: object) -> _Response:
        self.calls.append((method, url))
        if "cloudasset" in url:
            return _Response(
                {
                    "assets": [
                        {
                            "name": "//compute.googleapis.com/projects/demo-12345/zones/a/instances/vm-1",
                            "assetType": "compute.googleapis.com/Instance",
                            "resource": {"location": "asia-southeast2", "data": {"name": "vm-1"}},
                        }
                    ]
                }
            )
        if "getIamPolicy" in url:
            return _Response({"bindings": [{"role": "roles/viewer", "members": ["group:da@example.com"]}]})
        if url.endswith("/keys"):
            return _Response(
                {
                    "keys": [
                        {
                            "name": "projects/demo-12345/serviceAccounts/sa@example.com/keys/key-1",
                            "keyType": "USER_MANAGED",
                            "keyOrigin": "GOOGLE_PROVIDED",
                            "validAfterTime": "2026-01-01T00:00:00Z",
                            "privateKeyData": "must-never-be-persisted",
                        }
                    ]
                }
            )
        if "serviceAccounts" in url:
            return _Response({"accounts": [{"name": "projects/demo-12345/serviceAccounts/sa@example.com", "email": "sa@example.com", "uniqueId": "1"}]})
        return _Response({"services": [{"name": "projects/demo-12345/services/compute.googleapis.com", "state": "ENABLED", "config": {"name": "compute.googleapis.com"}}]})


class _Postgres:
    def __init__(self) -> None:
        self.commands: list[SQLCommand] = []

    def transaction(self, commands: list[SQLCommand]) -> None:
        self.commands.extend(commands)


def test_clean_redacts_secret_fields_and_fingerprint_is_deterministic() -> None:
    payload = {"name": "resource", "secret": "must-not-persist", "nested": {"password": "x", "ok": 1}}
    clean = _clean(payload)
    assert clean == {"name": "resource", "nested": {"ok": 1}}
    assert _fingerprint(payload) == _fingerprint({"nested": {"ok": 1}, "name": "resource"})


def test_scanner_collects_four_views_and_uses_parameterized_version_function(monkeypatch) -> None:
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
    scanner = GCPInventoryScanner(
        object(),
        postgres,
        GCPMonitoringConfig(("demo-12345",), page_size=100),
    )
    counts = scanner.scan("scheduled__2026-09-04", datetime(2026, 9, 4, tzinfo=UTC))
    assert counts == {"RESOURCE": 1, "IAM_BINDING": 1, "SERVICE_ACCOUNT": 1, "ENABLED_API": 1}
    assert len(postgres.commands) == 8
    assert sum("monitoring.record_gcp_inventory_snapshot" in command.sql for command in postgres.commands) == 4
    assert sum("monitoring.close_gcp_inventory_missing" in command.sql for command in postgres.commands) == 4
    record_commands = [command for command in postgres.commands if "monitoring.record_gcp_inventory_snapshot" in command.sql]
    assert all("payload" in command.parameters for command in record_commands)
    service_account_payload = next(
        json.loads(str(command.parameters["payload"]))
        for command in record_commands
        if command.parameters["asset_type"] == "SERVICE_ACCOUNT"
    )
    assert service_account_payload["key_count"] == 1
    assert service_account_payload["has_keys"] is True
    assert service_account_payload["keys"][0]["keyType"] == "USER_MANAGED"
    assert "privateKeyData" not in service_account_payload["keys"][0]
    assert any(
        url.endswith("/serviceAccounts/sa%40example.com/keys")
        for _, url in getattr(scanner.session, "calls", [])
    )
    assert all("secret" not in json.dumps(command.parameters, default=str).lower() for command in postgres.commands)
