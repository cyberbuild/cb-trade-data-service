import pyarrow as pa
from typing import Dict, Any, List, Union, TypeVar, Generic, Optional, Type
import pandas as pd
import pyarrow as pa
import copy
from abc import ABC, abstractmethod

# --- Interface Definition ---
class IExchangeRecord(ABC, dict):

    @abstractmethod
    def _validate(self):
        """Subclasses must implement validation logic for record content."""
        pass

    @property
    @abstractmethod
    def timestamp(self) -> int:
        """All records must have a timestamp (milliseconds preferred)."""
        pass

    @classmethod
    @abstractmethod
    def get_arrow_schema(cls, records: list = None):
        """Return the PyArrow schema for this record type, optionally including extra fields from records."""
        pass

    @staticmethod
    @abstractmethod
    def _infer_pyarrow_type(value):
        """Infer a pyarrow type from a Python value."""
        pass

    @abstractmethod
    def to_arrow(self):
        """Return a pyarrow Table for this record."""
        pass

# --- Implementation ---
class BaseExchangeRecord(IExchangeRecord):

    def __init__(self, data: dict):
        super().__init__(data)
        self._validate()
    
    @classmethod
    def get_arrow_schema(cls, records: list = None) -> pa.Schema:
        if not records or len(records) == 0:
            raise ValueError(f"{cls.__name__}.get_arrow_schema requires at least one record to infer schema.")
        fields = []
        seen_keys = set()
        for rec in records:
            for k, v in rec.items():
                if k not in seen_keys:
                    pa_type = cls._infer_pyarrow_type(v)
                    if pa_type is not None:
                        fields.append(pa.field(k, pa_type))
                        seen_keys.add(k)
        if not fields:
            raise ValueError(f"Could not infer any fields for {cls.__name__} from provided records.")
        return pa.schema(fields)


    @staticmethod
    def _infer_pyarrow_type(value):
        import pyarrow as pa
        if isinstance(value, bool):
            return pa.bool_()
        if isinstance(value, int):
            return pa.int64()
        if isinstance(value, float):
            return pa.float64()
        if isinstance(value, str):
            return pa.string()
        if value is None:
            return pa.null()
        return pa.string()
    
    REQUIRED_KEYS = {'timestamp'}
    TYPE_MAP = dict()

    @property
    def timestamp(self) -> int:
        return self['timestamp']

    def _validate(self):
        # Example validation (can be expanded)
        if 'timestamp' not in self:
            raise ValueError("Record missing required key: 'timestamp'")
        if not isinstance(self['timestamp'], int):
             raise TypeError("Timestamp must be an integer (milliseconds)")

    def to_arrow(self) -> pa.Table:
        # Assuming validation happens before this if needed
        # Convert self (dict) to a list containing self for from_pylist
        schema = self.get_arrow_schema([self]) # Infer schema from self
        return pa.Table.from_pylist([self], schema=schema)


class OHLCVRecord(BaseExchangeRecord):

    def __init__(self, data: dict):
        super().__init__(data)

    @property
    def open(self):
        return self['open']

    @property
    def high(self):
        return self['high']

    @property
    def low(self):
        return self['low']

    @property
    def close(self):
        return self['close']

    @property
    def volume(self):
        return self['volume']
    
    REQUIRED_KEYS = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
    
    TYPE_MAP = {
        'timestamp': int,
        'open': (int, float),
        'high': (int, float),
        'low': (int, float),
        'close': (int, float),
        'volume': (int, float),
    }

    def _validate(self):
        super()._validate() # Validate base requirements first
        missing_keys = self.REQUIRED_KEYS - set(self.keys())
        if missing_keys:
            raise ValueError(f"OHLCVRecord missing required keys: {missing_keys}")
        for key, expected_type in self.TYPE_MAP.items():
            if key in self: # Check if key exists before type checking
                 if not isinstance(self[key], expected_type):
                     raise TypeError(f"Key '{key}' has incorrect type {type(self[key])}. Expected {expected_type}.")


TExchangeRecord = TypeVar('TRecord', bound=IExchangeRecord)


class Metadata(dict):
    @property
    def data_type(self):
        return self.get('data_type')

    @property
    def exchange(self):
        return self.get('exchange')

    @property
    def coin_symbol(self):
        return self.get('coin')

    @property
    def interval(self):
        return self.get('interval')

    @property
    def exchange(self):
        return self.get('exchange')

    @property
    def coin_symbol(self):
        return self.get('coin')

    @property
    def interval(self):
        return self.get('interval')

class ExchangeData(Generic[TExchangeRecord]):
    def __init__(self, data: Union[List[TExchangeRecord], TExchangeRecord], metadata: Dict[str, Any]):
        if not data:
            raise ValueError(f"{self.__class__.__name__} cannot be initialized with empty data.")

        if isinstance(data, list):
            records = data
        else:
            records = [data]
   
        record_type = type(records[0])

        self._data = records
        self._metadata = Metadata(metadata)
        self._record_type = record_type


    @property
    def record_type(self) -> Type[TExchangeRecord]:
        return self._record_type

    @property
    def data(self) -> List[TExchangeRecord]:
        return self._data

    @property
    def metadata(self) -> Metadata:
        return self._metadata

    @property
    def record_type(self) -> Optional[Type[TExchangeRecord]]:
        return self._record_type

    def to_arrow(self) -> pa.Table:
        if not self._record_type:
            if not self._data:
                raise ValueError("Cannot determine Arrow schema for empty data without a record type.")
            else:
                raise RuntimeError("Record type not determined despite having data.")
        schema = self.data[0].to_arrow().schema
        df = pd.DataFrame(self._data)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            # Adjust schema to ensure timestamp[ms, tz=UTC]
            fields = []
            for field in schema:
                if field.name == 'timestamp':
                    fields.append(pa.field('timestamp', pa.timestamp('ms', tz='UTC'), field.nullable, field.metadata))
                else:
                    fields.append(field)
            schema = pa.schema(fields)
        return pa.Table.from_pandas(df, schema=schema, preserve_index=False)

import logging
logger = logging.getLogger(__name__)
