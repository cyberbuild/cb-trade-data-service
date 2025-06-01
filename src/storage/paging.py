from typing import Optional
from dataclasses import dataclass


@dataclass
class Paging:
    """
    Represents pagination parameters for data retrieval.

    Attributes:
        limit: Maximum number of records to return (None means no limit)
        offset: Number of records to skip from the beginning (default: 0)
    """
    limit: Optional[int] = None
    offset: int = 0

    @classmethod
    def all_records(cls) -> 'Paging':
        """Returns a Paging object that retrieves all records (no pagination)."""
        return cls(limit=None, offset=0)

    @classmethod
    def create(cls, limit: Optional[int] = None, offset: int = 0) -> 'Paging':
        """Creates a Paging object with specified parameters."""
        return cls(limit=limit, offset=offset)

    def has_pagination(self) -> bool:
        """Returns True if this Paging object applies any pagination constraints."""
        return self.limit is not None or self.offset > 0

    def __str__(self) -> str:
        if not self.has_pagination():
            return "Paging(all records)"
        return f"Paging(limit={self.limit}, offset={self.offset})"
