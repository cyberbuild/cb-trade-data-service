import pytest
from exchange_source.models import Metadata
from storage.path_strategy import OHLCVPathStrategy


def test_generate_base_path_success() -> None:
    strategy = OHLCVPathStrategy()
    context = {"exchange": "Binance", "coin": "btc/usd", "interval": "1h"}
    path = strategy.generate_base_path(context)
    assert path == "ohlcv/binance/BTC_USD/1h"


@pytest.mark.parametrize(
    "context",
    [
        {},
        {"exchange": "Binance", "coin": "BTC"},
        {"exchange": "", "coin": "BTC", "interval": "1m"},
        {"exchange": "Binance", "coin": "", "interval": "1m"},
        {"exchange": "Binance", "coin": "BTC", "interval": ""},
    ],
)
def test_generate_base_path_invalid_context(context) -> None:
    strategy = OHLCVPathStrategy()
    with pytest.raises(ValueError):
        strategy.generate_base_path(context)


def test_get_metadata_success() -> None:
    strategy = OHLCVPathStrategy()
    path = "ohlcv/coinex/ETH_USD/4h"
    metadata = strategy.get_metadata(path)
    expected = Metadata(
        {
            "data_type": "ohlcv",
            "exchange": "coinex",
            "coin": "ETH_USD",
            "interval": "4h",
        }
    )
    assert dict(metadata) == dict(expected)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "ohlcv/onlythree/parts",
        "notohlcv/binance/BTC_USD/1h",
    ],
)
def test_get_metadata_invalid_path(path) -> None:
    strategy = OHLCVPathStrategy()
    with pytest.raises(ValueError):
        strategy.get_metadata(path)
