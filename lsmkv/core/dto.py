"""
Data Transfer Objects for the LSM KV Store.
"""
import json
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class OperationType(Enum):
    """Types of operations in the WAL."""
    PUT = "PUT"
    DELETE = "DELETE"


@dataclass
class Entry:
    """Represents a key-value entry in the store."""
    key: str
    value: Optional[str]
    timestamp: int
    is_deleted: bool = False
    
    def __lt__(self, other):
        """Compare entries by key for skiplist ordering."""
        if not isinstance(other, Entry):
            return NotImplemented
        return self.key < other.key
    
    def __eq__(self, other):
        """Compare entries by key."""
        if not isinstance(other, Entry):
            return NotImplemented
        return self.key == other.key


@dataclass
class WALRecord:
    """Represents a record in the Write-Ahead Log."""
    operation: OperationType
    key: str
    value: Optional[str]
    timestamp: int
    
    def serialize(self) -> str:
        """Serialize the WAL record to a JSON string."""
        record = {
            "op": self.operation.value,
            "key": self.key,
            "value": self.value,
            "ts": self.timestamp
        }
        return json.dumps(record, separators=(',', ':')) + '\n'

    @staticmethod
    def deserialize(line: str) -> 'WALRecord':
        """Deserialize a WAL record from a JSON string."""
        stripped = line.strip()
        if not stripped:
            raise ValueError("Empty WAL record")

        record = json.loads(stripped)

        return WALRecord(
            operation=OperationType(record["op"]),
            key=record["key"],
            value=record["value"],
            timestamp=record["ts"]
        )


@dataclass
class GetResult:
    """Result of a GET operation."""
    key: str
    value: Optional[str]
    found: bool
    
    def __str__(self):
        if self.found:
            return f"{self.key} = {self.value}"
        else:
            return f"Key '{self.key}' not found"
