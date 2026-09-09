from datetime import date
from decimal import Decimal

import pytest

from architron_monitoring_airflow.fx import parse_ecb_usd_idr_rates

ECB_FIXTURE = b"""<?xml version="1.0" encoding="UTF-8"?>
<Envelope xmlns="urn:gesmes">
  <Cube>
    <Cube time="2026-08-07">
      <Cube currency="USD" rate="1.25"/>
      <Cube currency="IDR" rate="20000"/>
    </Cube>
    <Cube time="2026-08-10">
      <Cube currency="USD" rate="1.20"/>
      <Cube currency="IDR" rate="19800"/>
    </Cube>
  </Cube>
</Envelope>
"""


def test_ecb_cross_rate_and_weekend_carry_forward() -> None:
    rates = parse_ecb_usd_idr_rates(ECB_FIXTURE, start=date(2026, 8, 8), end=date(2026, 8, 11))

    assert [item.effective_date for item in rates] == [
        date(2026, 8, 8),
        date(2026, 8, 9),
        date(2026, 8, 10),
    ]
    assert rates[0].observed_on == date(2026, 8, 7)
    assert rates[0].rate == Decimal("16000")
    assert rates[1].rate == Decimal("16000")
    assert rates[2].rate == Decimal("16500")


def test_ecb_parser_fails_when_no_prior_rate_exists() -> None:
    with pytest.raises(ValueError, match="no prior USD/IDR rate"):
        parse_ecb_usd_idr_rates(ECB_FIXTURE, start=date(2026, 8, 6), end=date(2026, 8, 7))
