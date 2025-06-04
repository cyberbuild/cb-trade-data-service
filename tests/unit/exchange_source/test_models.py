import pytest
import pyarrow as pa
import pandas as pd
from datetime import datetime, timezone

# Use PEP 420 compliant imports
from exchange_source.models import (
    BaseExchangeRecord,
    OHLCVRecord,
    Metadata,
    ExchangeData,
    IExchangeRecord,
    Format,
)

# --- Fixtures ---


@pytest.fixture
def sample_base_record_data():
    # Provides valid data for BaseExchangeRecord
    return {
        "timestamp": 1678886400000,
        "value": 100.5,
        "label": "test",
        "flag": True,
        "id": 123,
    }


@pytest.fixture
def sample_base_record(sample_base_record_data):
    # Instantiate BaseExchangeRecord for tests that need an instance
    return BaseExchangeRecord(sample_base_record_data)


@pytest.fixture
def sample_ohlcv_data():
    # Provides valid data for OHLCVRecord
    return {
        "timestamp": 1678886400000,
        "open": 100.0,
        "high": 105.0,
        "low": 98.5,
        "close": 102.5,
        "volume": 5000.0,
    }


@pytest.fixture
def sample_ohlcv_record(sample_ohlcv_data):
    # Instantiate OHLCVRecord for tests that need an instance
    return OHLCVRecord(sample_ohlcv_data)


@pytest.fixture
def sample_metadata_dict():
    return {
        "data_type": "ohlcv",
        "exchange": "test_exchange",
        "coin": "BTC/USD",
        "interval": "1h",
    }


@pytest.fixture
def sample_metadata(sample_metadata_dict):
    return Metadata(sample_metadata_dict)


# --- Test IExchangeRecord (Abstract Base Class) ---
# Cannot directly instantiate ABC, test through implementations

# --- Test BaseExchangeRecord ---


def test_base_record_instantiation(sample_base_record_data):
    # Test successful instantiation with valid data
    record = BaseExchangeRecord(sample_base_record_data)
    assert isinstance(record, BaseExchangeRecord)
    assert isinstance(record, IExchangeRecord)
    assert record["timestamp"] == sample_base_record_data["timestamp"]


def test_base_record_timestamp_property(sample_base_record):
    # Test timestamp property access (already implemented in BaseExchangeRecord)
    assert sample_base_record.timestamp == 1678886400000


def test_base_record_validate_success(sample_base_record):
    # Test that _validate passes with good data (implicitly called on init)
    try:
        sample_base_record._validate()  # Can call explicitly too
    except (ValueError, TypeError) as e:
        pytest.fail(f"Validation failed unexpectedly: {e}")


def test_base_record_validate_wrong_timestamp_type():
    with pytest.raises(TypeError, match="Timestamp must be an integer"):
        BaseExchangeRecord({"timestamp": "not-an-int"})  # Wrong type


def test_base_record_to_arrow(sample_base_record, sample_base_record_data):
    # Test the implemented to_arrow method
    arrow_table = sample_base_record.to_arrow()
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 1
    # Check schema inference matches get_arrow_schema
    expected_schema = BaseExchangeRecord.get_arrow_schema([sample_base_record_data])
    assert arrow_table.schema.equals(expected_schema)
    # Check data
    assert arrow_table.to_pylist() == [sample_base_record_data]


def test_base_record_infer_pyarrow_type():
    assert BaseExchangeRecord._infer_pyarrow_type(True) == pa.bool_()
    assert BaseExchangeRecord._infer_pyarrow_type(10) == pa.int64()
    assert BaseExchangeRecord._infer_pyarrow_type(10.5) == pa.float64()
    assert BaseExchangeRecord._infer_pyarrow_type("test") == pa.string()
    assert BaseExchangeRecord._infer_pyarrow_type(None) == pa.null()
    assert (
        BaseExchangeRecord._infer_pyarrow_type(b"bytes") == pa.string()
    )  # Example non-standard type


def test_base_record_get_arrow_schema(sample_base_record_data):  # Use data dict
    records = [
        sample_base_record_data,
        {
            "timestamp": 1678886400001,
            "value": 101.0,
            "label": "test2",
            "flag": False,
            "id": 124,
        },
    ]
    schema = BaseExchangeRecord.get_arrow_schema(records)
    expected_fields = {
        "timestamp": pa.int64(),
        "value": pa.float64(),
        "label": pa.string(),
        "flag": pa.bool_(),
        "id": pa.int64(),
    }
    assert len(schema) == len(expected_fields)
    for field in schema:
        assert field.name in expected_fields
        assert field.type == expected_fields[field.name]


def test_base_record_get_arrow_schema_empty():
    with pytest.raises(ValueError, match="requires at least one record"):
        BaseExchangeRecord.get_arrow_schema([])
    with pytest.raises(ValueError, match="requires at least one record"):
        BaseExchangeRecord.get_arrow_schema()  # Test default None case


# --- Test OHLCVRecord ---


def test_ohlcv_record_instantiation(sample_ohlcv_data):
    # Test successful instantiation
    record = OHLCVRecord(sample_ohlcv_data)
    assert isinstance(record, OHLCVRecord)
    assert isinstance(record, BaseExchangeRecord)  # Check inheritance
    assert isinstance(record, IExchangeRecord)


def test_ohlcv_record_properties(sample_ohlcv_record, sample_ohlcv_data):
    # Test property accessors
    assert sample_ohlcv_record.timestamp == sample_ohlcv_data["timestamp"]
    assert sample_ohlcv_record.open == sample_ohlcv_data["open"]
    assert sample_ohlcv_record.high == sample_ohlcv_data["high"]
    assert sample_ohlcv_record.low == sample_ohlcv_data["low"]
    assert sample_ohlcv_record.close == sample_ohlcv_data["close"]
    assert sample_ohlcv_record.volume == sample_ohlcv_data["volume"]


def test_ohlcv_record_validate_success(sample_ohlcv_record):
    # Test that _validate passes with good data (implicitly called on init)
    try:
        sample_ohlcv_record._validate()  # Can call explicitly too
    except (ValueError, TypeError) as e:
        pytest.fail(f"OHLCV validation failed unexpectedly: {e}")


def test_ohlcv_record_validate_missing_key(sample_ohlcv_data):
    incomplete_data = sample_ohlcv_data.copy()
    del incomplete_data["close"]
    with pytest.raises(ValueError, match="missing required keys: {'close'}"):
        OHLCVRecord(incomplete_data)


def test_ohlcv_record_validate_wrong_type(sample_ohlcv_data):
    invalid_data = sample_ohlcv_data.copy()
    invalid_data["open"] = "not-a-number"
    with pytest.raises(TypeError, match="Key 'open' has incorrect type"):
        OHLCVRecord(invalid_data)


def test_ohlcv_record_validate_wrong_timestamp_type(sample_ohlcv_data):
    invalid_data = sample_ohlcv_data.copy()
    invalid_data["timestamp"] = 1678886400000.5  # Float instead of int
    # This will fail the base class validation first
    with pytest.raises(TypeError, match="Timestamp must be an integer"):
        OHLCVRecord(invalid_data)


def test_ohlcv_record_to_arrow(sample_ohlcv_record, sample_ohlcv_data):
    # Test the inherited to_arrow method works correctly for OHLCV
    arrow_table = sample_ohlcv_record.to_arrow()
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 1
    # Check schema inference matches get_arrow_schema
    expected_schema = OHLCVRecord.get_arrow_schema([sample_ohlcv_data])
    assert arrow_table.schema.equals(expected_schema)
    # Check data
    assert arrow_table.to_pylist() == [sample_ohlcv_data]


def test_ohlcv_record_required_keys():
    assert OHLCVRecord.REQUIRED_KEYS == {
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }


def test_ohlcv_record_type_map():
    assert OHLCVRecord.TYPE_MAP["timestamp"] == int
    assert OHLCVRecord.TYPE_MAP["open"] == (int, float)


# --- Test Metadata ---


def test_metadata_properties(sample_metadata, sample_metadata_dict):
    assert sample_metadata.data_type == sample_metadata_dict["data_type"]
    assert sample_metadata.exchange == sample_metadata_dict["exchange"]
    assert sample_metadata.coin_symbol == sample_metadata_dict["coin"]
    assert sample_metadata.interval == sample_metadata_dict["interval"]


def test_metadata_missing_key(sample_metadata):
    assert sample_metadata.get("non_existent_key") is None
    # Test property access for a potentially missing key
    temp_metadata = Metadata({"exchange": "xyz"})
    assert temp_metadata.data_type is None


# --- Test ExchangeData ---


@pytest.fixture
def sample_ohlcv_records_list(sample_ohlcv_data):
    # Create a list of valid OHLCVRecord instances
    record1 = OHLCVRecord(sample_ohlcv_data)
    record2_data = sample_ohlcv_data.copy()
    record2_data["timestamp"] += 60000  # 1 minute later
    record2_data["close"] += 1.0
    record2 = OHLCVRecord(record2_data)
    return [record1, record2]


def test_exchange_data_init_list(sample_ohlcv_records_list, sample_metadata_dict):
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata_dict)
    assert isinstance(exchange_data.data, list)
    assert len(exchange_data.data) == 2
    assert all(isinstance(r, OHLCVRecord) for r in exchange_data.data)  # Check type
    assert exchange_data.data == sample_ohlcv_records_list
    assert isinstance(exchange_data.metadata, Metadata)
    assert exchange_data.metadata == sample_metadata_dict
    assert exchange_data.record_type == OHLCVRecord


def test_exchange_data_init_single(sample_ohlcv_record, sample_metadata_dict):
    exchange_data = ExchangeData(sample_ohlcv_record, sample_metadata_dict)
    assert isinstance(
        exchange_data.data, list
    )  # Should always store as list internally
    assert len(exchange_data.data) == 1
    assert isinstance(exchange_data.data[0], OHLCVRecord)  # Check type
    assert exchange_data.data[0] == sample_ohlcv_record
    assert isinstance(exchange_data.metadata, Metadata)
    assert exchange_data.metadata == sample_metadata_dict
    assert exchange_data.record_type == OHLCVRecord


def test_exchange_data_init_empty_list():
    """Test that empty lists no longer raise an exception."""
    metadata = {"data_type": "ohlcv", "exchange": "test_exchange", "coin": "BTC/USD"}
    exchange_data = ExchangeData([], metadata)
    assert len(exchange_data.data) == 0
    assert exchange_data.record_type is None


def test_exchange_data_init_empty_list_allowed():
    """Test that ExchangeData can now be initialized with empty data."""
    metadata = {"data_type": "ohlcv", "exchange": "test_exchange", "coin": "BTC/USD"}
    exchange_data = ExchangeData([], metadata)
    assert len(exchange_data.data) == 0
    assert exchange_data.record_type is None
    assert exchange_data.metadata == metadata


def test_exchange_data_properties(sample_ohlcv_records_list, sample_metadata):
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata)
    assert exchange_data.data == sample_ohlcv_records_list
    assert exchange_data.metadata == sample_metadata
    assert exchange_data.record_type == OHLCVRecord


# Test Format enum
def test_format_enum():
    """Test that the Format enum has the expected values."""
    assert Format.EXCHANGE_DATA.name == "EXCHANGE_DATA"
    assert Format.DATAFRAME.name == "DATAFRAME"
    assert Format.ARROW.name == "ARROW"
    # Check ordering (implementation detail but useful for debugging)
    assert Format.EXCHANGE_DATA.value < Format.DATAFRAME.value < Format.ARROW.value


# Test convert method
def test_exchange_data_convert_to_dataframe(
    sample_ohlcv_records_list, sample_metadata_dict
):
    """Test converting ExchangeData to DataFrame."""
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata_dict)

    # Test convert method with Format.DATAFRAME
    df = exchange_data.convert(Format.DATAFRAME)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(sample_ohlcv_records_list)

    # Check that timestamp is converted to datetime
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    # Check that other fields are correctly converted
    assert df["open"].iloc[0] == sample_ohlcv_records_list[0]["open"]
    assert df["high"].iloc[0] == sample_ohlcv_records_list[0]["high"]
    assert df["low"].iloc[0] == sample_ohlcv_records_list[0]["low"]
    assert df["close"].iloc[0] == sample_ohlcv_records_list[0]["close"]
    assert df["volume"].iloc[0] == sample_ohlcv_records_list[0]["volume"]

    # Check timestamps are converted to datetime correctly
    expected_ts1 = pd.Timestamp(
        sample_ohlcv_records_list[0]["timestamp"], unit="ms", tz="UTC"
    )
    expected_ts2 = pd.Timestamp(
        sample_ohlcv_records_list[1]["timestamp"], unit="ms", tz="UTC"
    )
    assert df["timestamp"].iloc[0] == expected_ts1
    assert df["timestamp"].iloc[1] == expected_ts2


def test_exchange_data_to_dataframe(sample_ohlcv_records_list, sample_metadata_dict):
    """Test the to_dataframe convenience method."""
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata_dict)

    df = exchange_data.to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(sample_ohlcv_records_list)
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    # Verify it's equivalent to using convert method
    df_convert = exchange_data.convert(Format.DATAFRAME)
    pd.testing.assert_frame_equal(df, df_convert)


def test_exchange_data_convert_to_arrow(
    sample_ohlcv_records_list, sample_metadata_dict
):
    """Test converting ExchangeData to PyArrow Table."""
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata_dict)

    # Test convert method with Format.ARROW
    table = exchange_data.convert(Format.ARROW)
    assert isinstance(table, pa.Table)
    assert table.num_rows == len(sample_ohlcv_records_list)

    # Check that timestamp is converted to timestamp[ms] with UTC timezone
    assert "timestamp" in table.column_names
    timestamp_field = table.schema.field("timestamp")
    assert timestamp_field.type == pa.timestamp("ms", tz="UTC")

    # Convert to pandas and verify data
    df = table.to_pandas()
    assert len(df) == len(sample_ohlcv_records_list)
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    # Check that values are preserved
    expected_ts1 = pd.Timestamp(
        sample_ohlcv_records_list[0]["timestamp"], unit="ms", tz="UTC"
    )
    assert df["timestamp"].iloc[0] == expected_ts1
    assert df["open"].iloc[0] == sample_ohlcv_records_list[0]["open"]


def test_exchange_data_convert_unsupported_format(
    sample_ohlcv_records_list, sample_metadata_dict
):
    """Test that convert raises ValueError for unsupported format."""
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata_dict)

    # Mock an unsupported format value
    class MockFormat:
        name = "UNSUPPORTED"

    with pytest.raises(ValueError, match="Unsupported output format"):
        exchange_data.convert(MockFormat())


def test_exchange_data_to_arrow(sample_ohlcv_records_list, sample_metadata_dict):
    exchange_data = ExchangeData(sample_ohlcv_records_list, sample_metadata_dict)
    arrow_table = exchange_data.to_arrow()

    assert isinstance(arrow_table, pa.Table)
    assert len(arrow_table) == len(sample_ohlcv_records_list)

    # Check schema - Use the record_type from the instance
    expected_schema = exchange_data.record_type.get_arrow_schema(
        sample_ohlcv_records_list
    )
    # Adjust expected schema for timestamp conversion to datetime
    expected_fields_adjusted = []
    for field in expected_schema:
        if field.name == "timestamp":
            # The to_arrow method converts timestamp to pa.timestamp
            expected_fields_adjusted.append(
                pa.field(
                    field.name,
                    pa.timestamp("ms", tz="UTC"),
                    field.nullable,
                    field.metadata,
                )
            )
        else:
            expected_fields_adjusted.append(field)
    expected_schema_adjusted = pa.schema(expected_fields_adjusted)

    assert arrow_table.schema.equals(
        expected_schema_adjusted, check_metadata=False
    )  # Metadata might differ slightly

    # Check data
    df = arrow_table.to_pandas()
    assert df["open"].iloc[0] == sample_ohlcv_records_list[0]["open"]
    # Compare timestamps correctly (Pandas Timestamp vs int)
    expected_ts = pd.Timestamp(
        sample_ohlcv_records_list[0]["timestamp"], unit="ms", tz="UTC"
    )
    assert df["timestamp"].iloc[0] == expected_ts
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])


def test_exchange_data_to_arrow_timestamp_conversion(sample_metadata_dict):
    # Test with integer timestamps
    # Ensure OHLCVRecord validation passes
    data_int = [
        OHLCVRecord(
            {
                "timestamp": 1678886400000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10.0,
            }
        )
    ]
    exchange_data_int = ExchangeData(data_int, sample_metadata_dict)
    arrow_table_int = exchange_data_int.to_arrow()
    df_int = arrow_table_int.to_pandas()
    assert pd.api.types.is_datetime64_any_dtype(df_int["timestamp"])
    assert df_int["timestamp"].iloc[0] == pd.Timestamp(
        1678886400000, unit="ms", tz="UTC"
    )


def test_exchange_data_convert_empty():
    """Test that converting empty ExchangeData works properly."""
    metadata = {"data_type": "ohlcv", "exchange": "test_exchange", "coin": "BTC/USD"}
    exchange_data = ExchangeData([], metadata)

    # Test converting empty data to DataFrame
    df = exchange_data.convert(Format.DATAFRAME)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

    # Test converting empty data to Arrow
    table = exchange_data.convert(Format.ARROW)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 0

    # Test that to_dataframe and to_arrow convenience methods work
    df2 = exchange_data.to_dataframe()
    assert isinstance(df2, pd.DataFrame)
    assert len(df2) == 0

    table2 = exchange_data.to_arrow()
    assert isinstance(table2, pa.Table)
    assert table2.num_rows == 0

    # Test EXCHANGE_DATA format returns self
    result = exchange_data.convert(Format.EXCHANGE_DATA)
    assert result is exchange_data
