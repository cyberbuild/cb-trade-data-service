import pytest
from exchange_source.models import Metadata
from storage.partition_strategy import (
    YearMonthDayPartitionStrategy,
    NoPartitionStrategy,
)

pytestmark = pytest.mark.unit


def test_year_month_day_partition_cols() -> None:
    strategy = YearMonthDayPartitionStrategy()
    metadata = Metadata({"any": "value"})
    assert strategy.get_partition_cols(metadata) == ["year", "month", "day"]


def test_no_partition_strategy_returns_none() -> None:
    strategy = NoPartitionStrategy()
    metadata = Metadata({"any": "value"})
    assert strategy.get_partition_cols(metadata) is None
