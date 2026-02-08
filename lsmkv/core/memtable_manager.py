"""
MemtableManager - Manages active and immutable memtables with async flushing.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Callable
from collections import deque
from lsmkv.storage.memtable import Memtable
from lsmkv.core.dto import Entry


class ImmutableMemtable:
    """Wrapper for immutable memtable awaiting flush."""
    
    def __init__(self, memtable: Memtable, sequence_number: int):
        """
        Initialize immutable memtable.
        
        Args:
            memtable: The memtable to make immutable
            sequence_number: Sequence number for ordering
        """
        self.memtable = memtable
        self.sequence_number = sequence_number
        self.created_at = time.time()
        self.size_bytes = self._estimate_size()
    
    def _estimate_size(self) -> int:
        """Estimate memory size of this memtable in bytes."""
        # Rough estimate: 100 bytes per entry (key + value + overhead)
        return len(self.memtable) * 100
    
    def get(self, key: str) -> Optional[Entry]:
        """Get entry from immutable memtable."""
        return self.memtable.get(key)
    
    def get_all_entries(self) -> List[Entry]:
        """Get all entries in sorted order."""
        return self.memtable.get_all_entries()
    
    def __len__(self) -> int:
        """Return number of entries."""
        return len(self.memtable)


class MemtableManager:
    """
    Manages active and immutable memtables with intelligent flushing.
    
    Features:
    - One active (mutable) memtable for writes
    - Queue of immutable memtables (up to max_immutable) for reads
    - Dynamic queue sizing based on memory limits
    - Priority flushing (oldest first)
    - Thread pool for concurrent flushing
    - Optimized read path (memory before disk)
    """
    
    def __init__(
        self,
        memtable_size: int = 10,
        max_immutable: int = 4,
        max_memory_bytes: int = 10 * 1024 * 1024,  # 10MB
        flush_workers: int = 2,
        on_flush_callback: Optional[Callable[[Memtable], None]] = None
    ):
        """
        Initialize the memtable manager.
        
        Args:
            memtable_size: Max entries per memtable
            max_immutable: Max immutable memtables to keep
            max_memory_bytes: Max memory for immutable queue
            flush_workers: Number of flush worker threads
            on_flush_callback: Callback when memtable needs flushing
        """
        self.memtable_size = memtable_size
        self.max_immutable = max_immutable
        self.max_memory_bytes = max_memory_bytes
        self.on_flush_callback = on_flush_callback
        
        # Active memtable (mutable)
        self.active = Memtable(max_size=memtable_size)
        
        # Queue of immutable memtables (oldest to newest)
        self.immutable_queue = deque(maxlen=max_immutable)
        
        # Sequence number for ordering
        self.sequence_number = 0
        
        # Thread pool for async flushing
        self.flush_executor = ThreadPoolExecutor(
            max_workers=flush_workers,
            thread_name_prefix="flush-worker"
        )
        
        # Synchronization
        self.lock = threading.RLock()
        
        # Stats
        self.total_flushes = 0
        self.total_rotations = 0
    
    def put(self, entry: Entry):
        """
        Insert or update an entry.
        
        Args:
            entry: The entry to insert
        """
        with self.lock:
            self.active.put(entry)
            
            # Check if rotation needed
            if self.active.is_full():
                self._rotate_memtable()
    
    def get(self, key: str) -> Optional[Entry]:
        """
        Get an entry by key.
        Search order: active → immutable queue (newest to oldest).
        
        Args:
            key: The key to look up
            
        Returns:
            Entry if found, None otherwise
        """
        with self.lock:
            # 1. Check active memtable first (most recent data)
            entry = self.active.get(key)
            if entry:
                return entry
            
            # 2. Check immutable queue (newest to oldest)
            for immutable in reversed(self.immutable_queue):
                entry = immutable.get(key)
                if entry:
                    return entry
            
            # 3. Not in memory (caller should check SSTables)
            return None
    
    def delete(self, entry: Entry):
        """
        Delete an entry (add tombstone).
        
        Args:
            entry: The entry to delete (with is_deleted=True)
        """
        with self.lock:
            self.active.delete(entry)
            
            # Check if rotation needed
            if self.active.is_full():
                self._rotate_memtable()
    
    def _rotate_memtable(self):
        """
        Rotate active memtable to immutable queue and create new active.
        Triggers flush if queue is full or memory limit exceeded.
        """
        # Move active to immutable
        immutable = ImmutableMemtable(
            memtable=self.active,
            sequence_number=self.sequence_number
        )
        self.sequence_number += 1
        self.total_rotations += 1
        
        # Add to queue
        self.immutable_queue.append(immutable)
        
        # Create new active memtable
        self.active = Memtable(max_size=self.memtable_size)
        
        print(f"[MemtableManager] Rotated to immutable queue (size={len(self.immutable_queue)})")
        
        # Check if we need to flush
        self._check_and_flush()
    
    def _check_and_flush(self):
        """Check if flushing is needed and trigger if necessary."""
        should_flush = False
        reason = ""
        
        # Reason 1: Queue size limit
        if len(self.immutable_queue) >= self.max_immutable:
            should_flush = True
            reason = f"queue full ({len(self.immutable_queue)} >= {self.max_immutable})"
        
        # Reason 2: Memory limit
        total_memory = sum(im.size_bytes for im in self.immutable_queue)
        if total_memory >= self.max_memory_bytes:
            should_flush = True
            reason = f"memory limit ({total_memory} >= {self.max_memory_bytes} bytes)"
        
        if should_flush:
            # Flush oldest memtable (priority flushing)
            oldest = self.immutable_queue.popleft()
            print(f"[MemtableManager] Flushing oldest memtable ({reason})")
            
            # Submit to thread pool
            future = self.flush_executor.submit(self._async_flush, oldest)
            self.total_flushes += 1
    
    def _async_flush(self, immutable: ImmutableMemtable):
        """
        Async worker to flush an immutable memtable.
        
        Args:
            immutable: The immutable memtable to flush
        """
        try:
            start_time = time.time()
            
            # Call the flush callback if provided
            if self.on_flush_callback:
                self.on_flush_callback(immutable.memtable)
            
            elapsed = time.time() - start_time
            print(f"[Flush-Worker] Flushed memtable seq={immutable.sequence_number} "
                  f"({len(immutable)} entries) in {elapsed:.3f}s")
            
        except Exception as e:
            print(f"[Flush-Worker] Error flushing memtable: {e}")
            import traceback
            traceback.print_exc()
    
    def force_flush_all(self):
        """
        Force flush all immutable memtables synchronously.
        Useful before shutdown or manual flush.
        """
        with self.lock:
            # Flush all immutable memtables
            while self.immutable_queue:
                immutable = self.immutable_queue.popleft()
                if self.on_flush_callback:
                    self.on_flush_callback(immutable.memtable)
            
            # Flush active memtable if not empty
            if len(self.active) > 0:
                if self.on_flush_callback:
                    self.on_flush_callback(self.active)
                self.active.clear()
    
    def close(self):
        """Shutdown the manager and wait for pending flushes."""
        print("[MemtableManager] Shutting down...")
        
        # Shutdown thread pool gracefully
        self.flush_executor.shutdown(wait=True)
        
        print("[MemtableManager] All flush workers stopped")
    
    def stats(self) -> dict:
        """
        Get statistics about the memtable manager.
        
        Returns:
            Dictionary with stats
        """
        with self.lock:
            total_memory = sum(im.size_bytes for im in self.immutable_queue)
            
            return {
                "active_memtable_size": len(self.active),
                "active_memtable_full": self.active.is_full(),
                "immutable_count": len(self.immutable_queue),
                "immutable_queue_full": len(self.immutable_queue) >= self.max_immutable,
                "total_memory_bytes": total_memory,
                "memory_limit_bytes": self.max_memory_bytes,
                "total_rotations": self.total_rotations,
                "total_async_flushes": self.total_flushes,
                "max_queue_size": self.max_immutable,
            }
    
    def get_all_immutable_memtables(self) -> List[ImmutableMemtable]:
        """
        Get all immutable memtables (for inspection/testing).
        
        Returns:
            List of immutable memtables
        """
        with self.lock:
            return list(self.immutable_queue)
