from exchange_source.models import ExchangeData, IExchangeRecord


class DummyRecord(IExchangeRecord, dict):
    @property
    def timestamp(self):
        return self['timestamp']
    def _validate(self):
        pass
    @classmethod
    def get_arrow_schema(cls, records=None):
        pass
    @staticmethod
    def _infer_pyarrow_type(value):
        pass
    def to_arrow(self):
        pass


class DummyStorage:
    def __init__(self):
        self.records = []
    def get_latest_record(self, symbol, interval):
        recs = [r for r in self.records if r['symbol'] == symbol and r['interval'] == interval]
        return max(recs, key=lambda r: r.timestamp) if recs else None
    def get_data(self, symbol, interval, start, end):
        return ExchangeData([r for r in self.records if r['symbol'] == symbol and r['interval'] == interval and start <= r.timestamp <= end], {'symbol': symbol, 'interval': interval})
    def save_records(self, symbol, interval, new_data):
        self.records.extend(new_data)


class DummyCCXTClient:
    def fetch(self, symbol, interval, start, end):
        return [DummyRecord({'symbol': symbol, 'interval': interval, 'timestamp': start, 'price': 100.0})]
