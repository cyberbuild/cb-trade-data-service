import pytest
from storage.strategy_factory import DataTypeRegistry, PathStrategyFactory
from storage.path_strategy import OHLCVPathStrategy, IStoragePathStrategy

pytestmark = pytest.mark.unit

def test_get_strategy_class_registered() -> None:
    cls = DataTypeRegistry.get_strategy_class("ohlcv")
    assert cls is OHLCVPathStrategy

@ pytest.mark.parametrize("data_type", ["unknown", "invalid"])
def test_get_strategy_class_unregistered(data_type) -> None:
    with pytest.raises(ValueError):
        DataTypeRegistry.get_strategy_class(data_type)

def test_create_strategy_returns_instance() -> None:
    strategy = DataTypeRegistry.create_strategy("ohlcv")
    assert isinstance(strategy, OHLCVPathStrategy)

def test_create_strategy_from_context_success() -> None:
    context = {"data_type": "ohlcv"}
    strategy = PathStrategyFactory.create_strategy_from_context(context)
    assert isinstance(strategy, OHLCVPathStrategy)

@ pytest.mark.parametrize("context", [{}, {"data_type": ""}])
def test_create_strategy_from_context_missing_key(context) -> None:
    with pytest.raises(ValueError):
        PathStrategyFactory.create_strategy_from_context(context)
