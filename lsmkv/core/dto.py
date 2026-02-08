"""
Data Transfer Objects for the LSM KV Store.
"""
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
        """Serialize the WAL record to a string."""
        return f"{self.operation.value}|{self.key}|{self.value or ''}|{self.timestamp}\n"
    
    @staticmethod
    def deserialize(line: str) -> 'WALRecord':
        """Deserialize a WAL record from a string."""
        parts = line.strip().split('|')
        if len(parts) != 4:
            raise ValueError(f"Invalid WAL record format: {line}")
        
        operation = OperationType(parts[0])
        key = parts[1]
        value = parts[2] if parts[2] else None
        timestamp = int(parts[3])
        
        return WALRecord(operation, key, value, timestamp)


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
