import pyarrow as pa
from typing import Dict, Any, List, Union, TypeVar, Generic, Optional, Type, Literal
import pandas as pd
import pyarrow as pa
import copy
from abc import ABC, abstractmethod
from enum import Enum, auto

class Format(Enum):
    """Enum for data format conversion options"""
    EXCHANGE_DATA = auto()
    DATAFRAME = auto()
    ARROW = auto()

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

class ExchangeData(Generic[TExchangeRecord]):
    def __len__(self):
        return len(self._data)
        
    def __init__(self, data: Union[List[TExchangeRecord], TExchangeRecord, None], metadata: Dict[str, Any]):
        # Handle empty data case
        if not data or (isinstance(data, list) and len(data) == 0):
            self._data = []
            self._metadata = Metadata(metadata)
            self._record_type = None
            return
            
        # Handle single item vs list
        if isinstance(data, list):
            records = data
        else:
            records = [data]
   
        # Get record type from first item
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
    def convert(self, output_format: Format) -> Union[pd.DataFrame, pa.Table]:
        """
        Convert ExchangeData to the requested output format.
        
        Args:
            output_format: Format enum specifying the desired output format
            
        Returns:
            Union[pd.DataFrame, pa.Table]: Data in the requested format
            
        Raises:
            ValueError: If the format is not supported or if conversion can't be performed
        """
        # Special handling for empty data case
        if not self._data:
            if output_format == Format.DATAFRAME:
                return pd.DataFrame()
            elif output_format == Format.ARROW:
                # Create an empty arrow table with minimal schema
                return pa.Table.from_pandas(pd.DataFrame())
            elif output_format == Format.EXCHANGE_DATA:
                return self
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
                
        # Check record type for non-empty data
        if not self._record_type and self._data:
            raise RuntimeError("Record type not determined despite having data.")
                
        if output_format == Format.DATAFRAME:
            # Convert records to dict list for DataFrame
            records = [dict(record) for record in self._data]
            df = pd.DataFrame(records)
            
            # Convert timestamp to datetime for better pandas handling
            if 'timestamp' in df.columns and len(df) > 0 and pd.api.types.is_integer_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                
            return df
            
        elif output_format == Format.ARROW:
            # Get schema from first record
            schema = self.data[0].to_arrow().schema
            
            # Convert to DataFrame first (easier to handle)
            df = pd.DataFrame([dict(record) for record in self._data])
            
            # Handle timestamp conversion
            if 'timestamp' in df.columns and len(df) > 0:
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
            
        elif output_format == Format.EXCHANGE_DATA:
            return self
        
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
            
    def to_arrow(self) -> pa.Table:
        """
        Convert ExchangeData to PyArrow Table.
        
        Returns:
            pa.Table: Data as a PyArrow table
        """
        return self.convert(Format.ARROW)
        
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert ExchangeData to pandas DataFrame.
        
        Returns:
            pd.DataFrame: Data as a pandas DataFrame
        """
        return self.convert(Format.DATAFRAME)

import logging
logger = logging.getLogger(__name__)
