"""
Write-Ahead Log implementation for durability.
"""
import os
import threading
from typing import List
from lsmkv.core.dto import WALRecord, OperationType


class WAL:
    """Write-Ahead Log for ensuring durability of operations."""

    def __init__(self, filepath: str):
        """
        Initialize the WAL.

        Args:
            filepath: Path to the WAL file
        """
        self.filepath = filepath
        self._lock = threading.Lock()
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create the WAL file if it doesn't exist."""
        if not os.path.exists(self.filepath):
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            open(self.filepath, 'w').close()
    
    def append(self, record: WALRecord):
        """
        Append a record to the WAL.

        Args:
            record: The WAL record to append
        """
        with self._lock:
            with open(self.filepath, 'a') as f:
                f.write(record.serialize())
                f.flush()
                os.fsync(f.fileno())

    def read_all(self) -> List[WALRecord]:
        """
        Read all records from the WAL.

        Returns:
            List of WAL records
        """
        with self._lock:
            records = []
            with open(self.filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(WALRecord.deserialize(line))
                        except ValueError as e:
                            print(f"Warning: Skipping corrupted WAL record: {e}")
            return records

    def clear(self):
        """Clear the WAL file."""
        with self._lock:
            open(self.filepath, 'w').close()

    def replace_with_filtered(self, filter_fn):
        """
        Atomically read, filter, and rewrite the WAL.
        Entire operation held under lock - prevents race with concurrent append().
        
        Args:
            filter_fn: Callable(record) -> bool. Records where filter_fn returns True are KEPT.
        """
        with self._lock:
            records = []
            with open(self.filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(WALRecord.deserialize(line))
                        except ValueError as e:
                            print(f"Warning: Skipping corrupted WAL record: {e}")
            
            records_to_keep = [r for r in records if filter_fn(r)]
            
            tmp_path = self.filepath + ".tmp"
            with open(tmp_path, 'w') as f:
                for record in records_to_keep:
                    f.write(record.serialize())
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.filepath)

    def delete(self):
        """Delete the WAL file."""
        with self._lock:
            if os.path.exists(self.filepath):
                os.remove(self.filepath)
