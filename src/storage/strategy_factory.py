from typing import Dict, Any, Type
from storage.path_strategy import IStoragePathStrategy, OHLCVPathStrategy


class DataTypeRegistry:
    """Registry mapping data types to their appropriate path strategies"""

    _registry = {}

    @classmethod
    def register(cls, data_type: str, strategy_class: Type[IStoragePathStrategy]):
        """Register a path strategy class for a specific data type"""
        cls._registry[data_type] = strategy_class

    @classmethod
    def get_strategy_class(cls, data_type: str) -> Type[IStoragePathStrategy]:
        """Get the strategy class for a given data type"""
        if data_type not in cls._registry:
            raise ValueError(f"No path strategy registered for data type: {data_type}")
        return cls._registry[data_type]

    @classmethod
    def create_strategy(cls, data_type: str) -> IStoragePathStrategy:
        """Create a strategy instance for a given data type"""
        strategy_class = cls.get_strategy_class(data_type)
        return strategy_class()


class PathStrategyFactory:
    """Factory for creating path strategies based on context or data"""

    @staticmethod
    def create_strategy_from_context(context: Dict[str, Any]) -> IStoragePathStrategy:
        """Create appropriate strategy based on context"""
        if "data_type" not in context or not context["data_type"]:
            raise ValueError("Context must contain 'data_type'")
        data_type = context["data_type"]
        return DataTypeRegistry.create_strategy(data_type)


# Register known path strategies
DataTypeRegistry.register("ohlcv", OHLCVPathStrategy)
