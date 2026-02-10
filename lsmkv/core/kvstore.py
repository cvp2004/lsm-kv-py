"""
Main LSM-based Key-Value Store implementation with MemtableManager and SSTableManager.
"""
import os
import threading
import time
from typing import Optional, List
from lsmkv.storage.memtable import Memtable
from lsmkv.storage.wal import WAL
from lsmkv.storage.sstable import SSTableMetadata
from lsmkv.core.memtable_manager import MemtableManager
from lsmkv.core.sstable_manager import SSTableManager
from lsmkv.core.dto import Entry, WALRecord, OperationType, GetResult


class LSMKVStore:
    """LSM-based Key-Value Store with MemtableManager and SSTableManager."""
    
    def __init__(
        self,
        data_dir: str = "./data",
        memtable_size: int = 10,
        max_immutable_memtables: int = 4,
        max_memory_mb: int = 10,
        flush_workers: int = 2,
        # Compaction settings
        level_ratio: int = 10,
        base_level_size_mb: float = 1.0,
        base_level_entries: int = 1000,
        max_l0_sstables: int = 4,
        soft_limit_ratio: float = 0.85
    ):
        """
        Initialize the KV store with leveled compaction.
        
        Args:
            data_dir: Directory to store data files
            memtable_size: Maximum entries per memtable
            max_immutable_memtables: Max immutable memtables in queue
            max_memory_mb: Max memory for immutable queue (MB)
            flush_workers: Number of flush worker threads
            level_ratio: Size multiplier between levels (default: 10)
            base_level_size_mb: L0 max size in MB (default: 1.0)
            base_level_entries: L0 max entries (default: 1000)
            max_l0_sstables: Max SSTables in L0 before compaction (default: 4)
            soft_limit_ratio: Trigger compaction at % of hard limit (default: 0.85 = 85%)
        """
        self.data_dir = data_dir
        self.sstables_dir = os.path.join(data_dir, "sstables")
        self.memtable_size = memtable_size
        
        # Initialize storage components
        self.wal = WAL(f"{data_dir}/wal.log")
        
        # Initialize SSTableManager with leveled compaction
        self.sstable_manager = SSTableManager(
            sstables_dir=self.sstables_dir,
            manifest_path=f"{data_dir}/manifest.json",
            level_ratio=level_ratio,
            base_level_size_mb=base_level_size_mb,
            base_level_entries=base_level_entries,
            max_l0_sstables=max_l0_sstables,
            soft_limit_ratio=soft_limit_ratio
        )
        
        # Initialize MemtableManager with thread pool
        self.memtable_manager = MemtableManager(
            memtable_size=memtable_size,
            max_immutable=max_immutable_memtables,
            max_memory_bytes=max_memory_mb * 1024 * 1024,
            flush_workers=flush_workers,
            on_flush_callback=self._flush_memtable_to_sstable
        )
        
        # Write lock: ensures WAL and memtable ordering stay consistent.
        # Without this, concurrent put() can have different order in WAL vs memtable.
        self._write_lock = threading.Lock()

        # Shutdown flag: rejects new writes during/after close()
        self._closed = False

        # Timestamp generation: wall clock + guard against backward/same-microsecond
        self._last_timestamp = 0
        self._timestamp_lock = threading.Lock()

        # Load existing data
        self.sstable_manager.load_from_manifest()
        self._recover_from_wal()
    
    
    def _recover_from_wal(self):
        """Recover memtables from the WAL on startup."""
        print("Recovering from WAL...")
        records = self.wal.read_all()

        # Seed _last_timestamp so new writes win over both WAL and SSTable data.
        # WAL may be empty after clean shutdown; SSTables retain prior timestamps.
        # If clock drifted backward between sessions, new writes must still win.
        max_ts = 0
        if records:
            max_ts = max(r.timestamp for r in records)
        sstable_entries = self.sstable_manager.get_all_entries()
        if sstable_entries:
            max_ts = max(max_ts, max(e.timestamp for e in sstable_entries))
        if max_ts > 0:
            with self._timestamp_lock:
                self._last_timestamp = max(self._last_timestamp, max_ts)

        for record in records:
            entry = Entry(
                key=record.key,
                value=record.value,
                timestamp=record.timestamp,
                is_deleted=(record.operation == OperationType.DELETE)
            )
            
            if record.operation == OperationType.PUT:
                self.memtable_manager.put(entry)
            elif record.operation == OperationType.DELETE:
                self.memtable_manager.delete(entry)
        
        print(f"Recovered {len(records)} records from WAL")
    
    MAX_KEY_SIZE = 1024        # 1 KB
    MAX_VALUE_SIZE = 1048576   # 1 MB

    def _validate_key(self, key: str):
        """Validate key type and size."""
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
        if not key:
            raise ValueError("Key cannot be empty")
        if len(key) > self.MAX_KEY_SIZE:
            raise ValueError(f"Key exceeds max size ({len(key)} > {self.MAX_KEY_SIZE} bytes)")

    def _validate_value(self, value: str):
        """Validate value type and size."""
        if not isinstance(value, str):
            raise TypeError(f"Value must be a string, got {type(value).__name__}")
        if len(value) > self.MAX_VALUE_SIZE:
            raise ValueError(f"Value exceeds max size ({len(value)} > {self.MAX_VALUE_SIZE} bytes)")

    def put(self, key: str, value: str) -> bool:
        """
        Insert or update a key-value pair.

        Args:
            key: The key to insert
            value: The value to insert

        Returns:
            True if successful

        Raises:
            TypeError: If key or value is not a string
            ValueError: If key is empty or exceeds size limits
        """
        self._validate_key(key)
        self._validate_value(value)
        if self._closed:
            raise RuntimeError("KV store is closed")
        with self._write_lock:
            timestamp = self._get_timestamp()
            wal_record = WALRecord(
                operation=OperationType.PUT,
                key=key,
                value=value,
                timestamp=timestamp
            )
            self.wal.append(wal_record)
            entry = Entry(
                key=key,
                value=value,
                timestamp=timestamp,
                is_deleted=False
            )
            self.memtable_manager.put(entry)
        return True
    
    def get(self, key: str) -> GetResult:
        """
        Retrieve a value by key.

        Optimized read path:
        1. Active memtable
        2. Immutable memtable queue (newest to oldest)
        3. SSTables (newest to oldest)

        Args:
            key: The key to look up

        Returns:
            GetResult containing the value if found

        Raises:
            TypeError: If key is not a string
            ValueError: If key is empty
        """
        self._validate_key(key)
        if self._closed:
            raise RuntimeError("KV store is closed")
        # 1. Check memtable manager (active + immutable queue)
        # NOTE: MemtableManager now returns tombstones to stop search propagation
        entry = self.memtable_manager.get(key)
        
        if entry:
            # Check if it's a tombstone (delete marker)
            if entry.is_deleted:
                return GetResult(key=key, value=None, found=False)
            return GetResult(key=key, value=entry.value, found=True)
        
        # 2. Check SSTables (newest to oldest) via SSTableManager
        entry = self.sstable_manager.get(key)
        if entry:
            # Check if it's a tombstone
            if entry.is_deleted:
                return GetResult(key=key, value=None, found=False)
            return GetResult(key=key, value=entry.value, found=True)
        
        return GetResult(key=key, value=None, found=False)
    
    def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.

        Args:
            key: The key to delete

        Returns:
            True if successful

        Raises:
            TypeError: If key is not a string
            ValueError: If key is empty
        """
        self._validate_key(key)
        if self._closed:
            raise RuntimeError("KV store is closed")
        with self._write_lock:
            timestamp = self._get_timestamp()
            wal_record = WALRecord(
                operation=OperationType.DELETE,
                key=key,
                value=None,
                timestamp=timestamp
            )
            self.wal.append(wal_record)
            entry = Entry(
                key=key,
                value=None,
                timestamp=timestamp,
                is_deleted=True
            )
            self.memtable_manager.delete(entry)
        return True
    
    def _flush_memtable_to_sstable(self, memtable: Memtable):
        """
        Flush a memtable to SSTable (called by MemtableManager).
        
        Args:
            memtable: The memtable to flush
        """
        # Get all entries
        entries = memtable.get_all_entries()
        
        if not entries:
            return
        
        # Delegate SSTable creation to SSTableManager
        self.sstable_manager.add_sstable(entries)
        
        # Clear corresponding WAL entries
        # Note: For simplicity, we clear entire WAL after flush
        # In production, would track WAL offsets per memtable
        self._clear_wal_for_flushed_data(entries)
    
    def _clear_wal_for_flushed_data(self, flushed_entries: List[Entry]):
        """
        Clear WAL entries for flushed data.
        Uses WAL.replace_with_filtered for atomic read-filter-replace under single lock.
        Prevents race where concurrent put() appends are lost.

        Args:
            flushed_entries: Entries that were flushed
        """
        flushed_ts = {}
        for entry in flushed_entries:
            if entry.key not in flushed_ts or entry.timestamp > flushed_ts[entry.key]:
                flushed_ts[entry.key] = entry.timestamp

        def keep_record(r):
            return r.key not in flushed_ts or r.timestamp > flushed_ts[r.key]

        self.wal.replace_with_filtered(keep_record)
    
    def flush(self) -> SSTableMetadata:
        """
        Manually flush active memtable to SSTable.
        Thread-safe: atomically rotates active memtable, keeps it in read path
        until flush completes (no visibility gap).

        Returns:
            Metadata about the created SSTable
        """
        immutable = self.memtable_manager.flush_active_sync()
        if immutable is None:
            raise ValueError("Cannot flush empty memtable")

        try:
            entries = immutable.get_all_entries()
            metadata = self.sstable_manager.add_sstable(entries)
            self._clear_wal_for_flushed_data(entries)
            return metadata
        finally:
            self.memtable_manager.remove_flushed_immutable(immutable)
    
    def compact(self) -> SSTableMetadata:
        """
        Compact all SSTables into a single SSTable.
        Preserves only the latest entry for each key.
        
        Returns:
            Metadata about the compacted SSTable
        """
        # Wait for any in-flight background compactions first, to avoid race
        self.sstable_manager.wait_for_compaction(timeout=30.0)
        return self.sstable_manager.compact()
    
    def _get_timestamp(self) -> int:
        """Get monotonically increasing timestamp in microseconds.
        Uses time.time() (wall clock) so timestamps survive reboots and
        newer-wins semantics work across restarts. Guards against NTP
        clock going backward and same-microsecond collisions.
        """
        now = int(time.time() * 1000000)
        with self._timestamp_lock:
            if now <= self._last_timestamp:
                now = self._last_timestamp + 1
            self._last_timestamp = now
            return now
    
    def close(self):
        """Clean shutdown of the store. Flushes all pending data before shutdown."""
        print("Closing KV store...")
        self._closed = True

        # 1. Flush all in-memory data (active + immutable queue) to SSTables
        self.memtable_manager.force_flush_all()

        # 2. Shutdown memtable manager FIRST — wait for all async flush workers.
        #    They may call _clear_wal_for_flushed_data; WAL must not be cleared yet.
        self.memtable_manager.close()

        # 3. NOW clear WAL — all flush workers are done, safe to clear
        self.wal.clear()

        # 4. Shutdown SSTableManager (waits for pending compactions)
        self.sstable_manager.shutdown(wait=True, timeout=30.0)

        # 5. Close all SSTables (cleanup mmap) via SSTableManager
        self.sstable_manager.close()

        print("KV store closed.")
    
    def stats(self) -> dict:
        """
        Get statistics about the store including level information.
        
        Returns:
            Dictionary with store statistics
        """
        # Get memtable manager stats
        manager_stats = self.memtable_manager.stats()
        
        # Get SSTable stats from SSTableManager
        sstable_stats = self.sstable_manager.stats()
        
        # Combine stats
        return {
            # Memtable stats
            "active_memtable_size": manager_stats["active_memtable_size"],
            "active_memtable_full": manager_stats["active_memtable_full"],
            "memtable_max_size": self.memtable_size,
            
            # Immutable memtable queue stats
            "immutable_memtables": manager_stats["immutable_count"],
            "immutable_queue_full": manager_stats["immutable_queue_full"],
            "max_immutable_memtables": manager_stats["max_queue_size"],
            "immutable_memory_bytes": manager_stats["total_memory_bytes"],
            "immutable_memory_limit_bytes": manager_stats["memory_limit_bytes"],
            
            # SSTable stats (from SSTableManager) - includes per-level stats
            **sstable_stats,
            
            # Performance stats
            "total_memtable_rotations": manager_stats["total_rotations"],
            "total_async_flushes": manager_stats["total_async_flushes"],
        }
    
    def get_level_info(self) -> dict:
        """
        Get detailed information about each SSTable level.
        
        Returns:
            Dictionary mapping level to detailed stats
        """
        return self.sstable_manager.get_level_info()

