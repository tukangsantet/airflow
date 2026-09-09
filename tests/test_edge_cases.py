from decimal import Decimal

import pytest

from architron_monitoring_airflow.azure import validate_schema_headers
from architron_monitoring_airflow.gcp import compute_net_actual_cost
from architron_monitoring_airflow.idempotency import merge_by_key


def test_gcp_net_cost_requires_verified_credit_convention() -> None:
    with pytest.raises(ValueError, match="verified"):
        compute_net_actual_cost(Decimal("100"), [Decimal("-10")], convention="UNKNOWN")
    assert compute_net_actual_cost(
        Decimal("100"), [Decimal("-10"), Decimal("-5")], convention="SIGNED_ADDITIVE"
    ) == Decimal("85")
    # Refund/correction rows remain valid negative actual cost.
    assert compute_net_actual_cost(Decimal("-20"), [], convention="SIGNED_ADDITIVE") == Decimal(
        "-20"
    )


def test_azure_schema_change_is_quarantined_not_silently_accepted() -> None:
    required = {"SubscriptionId", "Date", "CostInBillingCurrency", "BillingCurrencyCode"}
    assert validate_schema_headers(required, required, "2025-03-01").accepted is True
    changed = validate_schema_headers(required - {"Date"}, required, "2025-03-01")
    assert changed.accepted is False
    assert changed.action == "QUARANTINE"
    assert changed.missing == ("Date",)


def test_deterministic_merge_makes_rerun_idempotent_and_applies_late_correction() -> None:
    existing = [{"key": "a", "amount": Decimal("10")}, {"key": "b", "amount": Decimal("5")}]
    rerun = [{"key": "a", "amount": Decimal("10")}, {"key": "b", "amount": Decimal("-2")}]
    merged = merge_by_key(existing, rerun, key=lambda row: row["key"])
    assert merged == [{"key": "a", "amount": Decimal("10")}, {"key": "b", "amount": Decimal("-2")}]
    assert merge_by_key(merged, rerun, key=lambda row: row["key"]) == merged
