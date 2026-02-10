"""
SSTable (Sorted String Table) implementation for persistent storage.
Enhanced with Bloom filter, sparse index, and mmap I/O.

Features:
- Lazy loading: Only metadata is loaded on startup, actual SSTable loaded on demand
- Bloom filter for fast negative lookups
- Sparse index for efficient range scans using floor/ceil bounds
- mmap for I/O performance with targeted reads
"""
import os
import json
import mmap
import threading
from typing import List, Optional, TYPE_CHECKING
from lsmkv.core.dto import Entry
from lsmkv.storage.bloom_filter import BloomFilter
from lsmkv.storage.sparse_index import SparseIndex


class SSTableMetadata:
    """Metadata for an SSTable."""
    
    def __init__(self, sstable_id: int, dirname: str, num_entries: int, min_key: str, max_key: str):
        """
        Initialize SSTable metadata.
        
        Args:
            sstable_id: Unique ID for the SSTable
            dirname: Directory name for this SSTable
            num_entries: Number of entries in the SSTable
            min_key: Smallest key in the SSTable
            max_key: Largest key in the SSTable
        """
        self.sstable_id = sstable_id
        self.dirname = dirname
        self.num_entries = num_entries
        self.min_key = min_key
        self.max_key = max_key
    
    def to_dict(self) -> dict:
        """Convert metadata to dictionary."""
        return {
            "sstable_id": self.sstable_id,
            "dirname": self.dirname,
            "num_entries": self.num_entries,
            "min_key": self.min_key,
            "max_key": self.max_key
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'SSTableMetadata':
        """Create metadata from dictionary."""
        return SSTableMetadata(
            sstable_id=data["sstable_id"],
            dirname=data["dirname"],
            num_entries=data["num_entries"],
            min_key=data["min_key"],
            max_key=data["max_key"]
        )


class SSTable:
    """
    Sorted String Table for persistent storage of key-value pairs.
    
    Directory structure:
    sstable_000001/
        ├── data.db          # Main data file
        ├── bloom_filter.bf  # Bloom filter
        └── sparse_index.idx # Sparse index
    
    Features:
    - Bloom filter for fast negative lookups
    - Sparse index for efficient range scans
    - mmap for I/O performance
    """
    
    # File names within SSTable directory
    DATA_FILE = "data.db"
    BLOOM_FILTER_FILE = "bloom_filter.bf"
    SPARSE_INDEX_FILE = "sparse_index.idx"
    
    def __init__(self, sstable_dir: str, sstable_id: int):
        """
        Initialize an SSTable.
        
        Args:
            sstable_dir: Base directory for SSTables (e.g., ./data/sstables)
            sstable_id: Unique ID for this SSTable
        """
        self.sstable_id = sstable_id
        self.dirname = f"sstable_{sstable_id:06d}"
        self.base_dir = os.path.join(sstable_dir, self.dirname)
        
        # File paths
        self.data_filepath = os.path.join(self.base_dir, self.DATA_FILE)
        self.bloom_filter_filepath = os.path.join(self.base_dir, self.BLOOM_FILTER_FILE)
        self.sparse_index_filepath = os.path.join(self.base_dir, self.SPARSE_INDEX_FILE)
        
        # Components (loaded on demand)
        self.metadata = None
        self._bloom_filter: Optional[BloomFilter] = None
        self._sparse_index: Optional[SparseIndex] = None
        self._mmap = None
        self._file = None
    
    def write(self, entries: List[Entry], block_size: int = 4) -> SSTableMetadata:
        """
        Write entries to the SSTable with Bloom filter and sparse index.
        
        Args:
            entries: List of entries to write (must be sorted by key)
            block_size: Index every Nth entry (default: 4)
            
        Returns:
            Metadata about the written SSTable
        """
        if not entries:
            raise ValueError("Cannot write empty SSTable")
        
        # Create SSTable directory
        os.makedirs(self.base_dir, exist_ok=True)
        
        # Create Bloom filter with file path (uses mmap automatically)
        # and sparse index
        bloom_filter = BloomFilter(
            expected_elements=len(entries), 
            false_positive_rate=0.01,
            filepath=self.bloom_filter_filepath
        )
        sparse_index = SparseIndex(block_size=block_size)
        
        # Write entries to data file and build index/filter
        with open(self.data_filepath, 'w') as f:
            for i, entry in enumerate(entries):
                # Track current byte offset
                offset = f.tell()
                
                # Add to Bloom filter
                bloom_filter.add(entry.key)
                
                # Add to sparse index (every Nth entry)
                if i % block_size == 0:
                    sparse_index.add_entry(entry.key, offset)
                
                # Serialize entry as JSON
                entry_dict = {
                    "key": entry.key,
                    "value": entry.value,
                    "timestamp": entry.timestamp,
                    "is_deleted": entry.is_deleted
                }
                f.write(json.dumps(entry_dict) + '\n')
        
        # Bloom filter is already saved (mmap-backed by pybloomfiltermmap3)
        # Just sync to ensure it's written to disk
        bloom_filter.close()
        
        # Save sparse index
        sparse_index.save_to_file(self.sparse_index_filepath)
        
        # Create metadata
        self.metadata = SSTableMetadata(
            sstable_id=self.sstable_id,
            dirname=self.dirname,
            num_entries=len(entries),
            min_key=entries[0].key,
            max_key=entries[-1].key
        )
        
        # Cache the components
        self._bloom_filter = bloom_filter
        self._sparse_index = sparse_index
        
        return self.metadata
    
    def _ensure_bloom_filter_loaded(self):
        """Lazy load Bloom filter."""
        if self._bloom_filter is None and os.path.exists(self.bloom_filter_filepath):
            self._bloom_filter = BloomFilter.load_from_file(self.bloom_filter_filepath)
    
    def _ensure_sparse_index_loaded(self):
        """Lazy load sparse index."""
        if self._sparse_index is None and os.path.exists(self.sparse_index_filepath):
            self._sparse_index = SparseIndex.load_from_file(self.sparse_index_filepath)
    
    def _ensure_mmap_ready(self):
        """Ensure mmap is ready for reading."""
        if self._mmap is None and os.path.exists(self.data_filepath):
            self._file = open(self.data_filepath, 'r+b')
            if os.path.getsize(self.data_filepath) > 0:
                self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
    
    def read_all(self) -> List[Entry]:
        """
        Read all entries from the SSTable using mmap.
        
        Returns:
            List of entries
        """
        entries = []
        
        if not os.path.exists(self.data_filepath):
            return entries
        
        self._ensure_mmap_ready()
        
        if self._mmap is None:
            return entries
        
        # Read using mmap
        self._mmap.seek(0)
        content = self._mmap.read().decode('utf-8')
        
        for line in content.split('\n'):
            line = line.strip()
            if line:
                entry_dict = json.loads(line)
                entry = Entry(
                    key=entry_dict["key"],
                    value=entry_dict["value"],
                    timestamp=entry_dict["timestamp"],
                    is_deleted=entry_dict["is_deleted"]
                )
                entries.append(entry)
        
        return entries
    
    def get(self, key: str) -> Optional[Entry]:
        """
        Get an entry by key from the SSTable.
        
        Uses optimized read strategy:
        1. Bloom filter for fast negative lookup (O(k) where k = hash functions)
        2. Sparse index to get floor/ceil byte offsets (O(log n))
        3. mmap to read ONLY the bytes between floor and ceil offsets
        4. Binary-like scan within the bounded region
        
        Args:
            key: The key to look up
            
        Returns:
            The entry if found, None otherwise
        """
        if not os.path.exists(self.data_filepath):
            return None
        
        # Check Bloom filter first (fast negative lookup)
        self._ensure_bloom_filter_loaded()
        if self._bloom_filter and not self._bloom_filter.might_contain(key):
            # Definitely not in this SSTable
            return None
        
        # Load sparse index and mmap
        self._ensure_sparse_index_loaded()
        self._ensure_mmap_ready()
        
        if self._mmap is None:
            return None
        
        # Get scan range from sparse index (floor and ceil bounds)
        start_offset = 0
        end_offset = None
        
        if self._sparse_index:
            start_offset, end_offset = self._sparse_index.get_scan_range(key)
        
        # OPTIMIZED: Read only the bounded region using mmap
        # This avoids reading the entire file
        return self._read_bounded_region(key, start_offset, end_offset)
    
    def _read_bounded_region(self, key: str, start_offset: int, 
                             end_offset: Optional[int]) -> Optional[Entry]:
        """
        Read only the bytes between floor and ceil offsets from sparse index.
        
        This is the key optimization: instead of reading the entire file,
        we only mmap-read the specific byte range where the key could exist.
        
        Args:
            key: The key to search for
            start_offset: Floor offset from sparse index (largest key <= target)
            end_offset: Ceil offset from sparse index (smallest key > target), or None for EOF
            
        Returns:
            Entry if found, None otherwise
        """
        if self._mmap is None:
            return None
        
        mmap_len = len(self._mmap)
        
        # Clamp end_offset to mmap length
        if end_offset is None or end_offset > mmap_len:
            end_offset = mmap_len
        
        # Validate offsets
        if start_offset >= mmap_len or start_offset >= end_offset:
            return None
        
        # Read ONLY the bounded region
        # This is efficient because mmap only loads pages on demand
        bounded_bytes = self._mmap[start_offset:end_offset]
        
        # Parse entries within the bounded region
        try:
            bounded_content = bounded_bytes.decode('utf-8')
        except UnicodeDecodeError:
            return None
        
        for line in bounded_content.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            try:
                entry_dict = json.loads(line)
                entry_key = entry_dict["key"]
                
                # Check if this is the key we're looking for
                if entry_key == key:
                    return Entry(
                        key=entry_dict["key"],
                        value=entry_dict["value"],
                        timestamp=entry_dict["timestamp"],
                        is_deleted=entry_dict["is_deleted"]
                    )
                
                # Since entries are sorted, if we've passed the key, it's not here
                if entry_key > key:
                    break
                    
            except (json.JSONDecodeError, KeyError):
                continue
        
        return None
    
    def exists(self) -> bool:
        """Check if the SSTable directory and data file exist."""
        return os.path.exists(self.base_dir) and os.path.exists(self.data_filepath)
    
    def size_bytes(self) -> int:
        """Get the total size of the SSTable (all files) in bytes."""
        total_size = 0
        if os.path.exists(self.base_dir):
            for filename in os.listdir(self.base_dir):
                filepath = os.path.join(self.base_dir, filename)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size
    
    def close(self):
        """Close mmap and file handles."""
        # Close Bloom filter (syncs to disk if file-backed)
        if self._bloom_filter is not None:
            self._bloom_filter.close()
        
        # Close data file mmap
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._file is not None:
            self._file.close()
            self._file = None
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        self.close()
    
    def delete(self):
        """Delete the entire SSTable directory and all its files."""
        self.close()
        
        if os.path.exists(self.base_dir):
            import shutil
            shutil.rmtree(self.base_dir)


class LazySSTable:
    """
    Lazy-loading wrapper for SSTable.
    
    Only stores metadata on initialization. The actual SSTable object
    (with mmap, bloom filter, sparse index) is loaded on demand when
    first accessed for read operations.
    
    Benefits:
    - Fast startup: Only reads manifest metadata, not all SSTable files
    - Memory efficient: SSTables loaded only when needed
    - Supports caching: Frequently accessed SSTables stay in memory
    
    Usage:
        lazy = LazySSTable(sstables_dir, metadata)
        # SSTable not loaded yet
        
        entry = lazy.get("key")  # SSTable loaded here on first access
        # SSTable now cached in memory
    """
    
    def __init__(self, sstables_dir: str, sstable_id: int,
                 metadata: Optional[SSTableMetadata] = None):
        """
        Initialize lazy SSTable wrapper.
        
        Args:
            sstables_dir: Base directory for SSTables
            sstable_id: Unique ID for this SSTable
            metadata: Pre-loaded metadata (optional, avoids disk read)
        """
        self.sstables_dir = sstables_dir
        self.sstable_id = sstable_id
        self.dirname = f"sstable_{sstable_id:06d}"
        
        # Store metadata (loaded from manifest, no disk I/O needed)
        self._metadata = metadata
        
        # Actual SSTable object (loaded on demand)
        self._sstable: Optional[SSTable] = None
        
        # Lock for thread-safe lazy loading
        self._load_lock = threading.Lock()
        
        # Track if we've been accessed
        self._access_count = 0
        self._loaded = False
    
    @property
    def metadata(self) -> Optional[SSTableMetadata]:
        """Get metadata (no disk I/O)."""
        return self._metadata
    
    @metadata.setter
    def metadata(self, value: SSTableMetadata):
        """Set metadata."""
        self._metadata = value
    
    def _ensure_loaded(self) -> Optional[SSTable]:
        """
        Lazy load the actual SSTable.
        
        Thread-safe: Uses double-checked locking pattern.
        
        Returns:
            The loaded SSTable, or None if it doesn't exist
        """
        if self._sstable is not None:
            return self._sstable
        
        with self._load_lock:
            # Double-check after acquiring lock
            if self._sstable is not None:
                return self._sstable
            
            # Create and load the actual SSTable
            sstable = SSTable(self.sstables_dir, self.sstable_id)
            
            if not sstable.exists():
                return None
            
            # Copy metadata if we have it
            if self._metadata:
                sstable.metadata = self._metadata
            
            self._sstable = sstable
            self._loaded = True
            
            return self._sstable
    
    def get(self, key: str) -> Optional[Entry]:
        """
        Get an entry by key (loads SSTable on demand).
        
        Args:
            key: The key to look up
            
        Returns:
            Entry if found, None otherwise
        """
        self._access_count += 1
        
        # Quick key range check using metadata (no disk I/O)
        if self._metadata:
            if key < self._metadata.min_key or key > self._metadata.max_key:
                return None
        
        sstable = self._ensure_loaded()
        if sstable is None:
            return None
        
        return sstable.get(key)
    
    def read_all(self) -> List[Entry]:
        """Read all entries (loads SSTable on demand)."""
        self._access_count += 1
        
        sstable = self._ensure_loaded()
        if sstable is None:
            return []
        
        return sstable.read_all()
    
    def exists(self) -> bool:
        """Check if SSTable exists on disk."""
        base_dir = os.path.join(self.sstables_dir, self.dirname)
        data_filepath = os.path.join(base_dir, SSTable.DATA_FILE)
        return os.path.exists(base_dir) and os.path.exists(data_filepath)
    
    def size_bytes(self) -> int:
        """Get total size in bytes."""
        base_dir = os.path.join(self.sstables_dir, self.dirname)
        total_size = 0
        if os.path.exists(base_dir):
            for filename in os.listdir(base_dir):
                filepath = os.path.join(base_dir, filename)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size
    
    def close(self):
        """Close the underlying SSTable if loaded."""
        with self._load_lock:
            if self._sstable is not None:
                self._sstable.close()
                self._sstable = None
                self._loaded = False
    
    def unload(self):
        """
        Unload the SSTable from memory (keeps metadata).
        
        Useful for memory management - can reload on next access.
        """
        self.close()
    
    def delete(self):
        """Delete the SSTable from disk."""
        self.close()
        
        base_dir = os.path.join(self.sstables_dir, self.dirname)
        if os.path.exists(base_dir):
            import shutil
            shutil.rmtree(base_dir)
    
    def is_loaded(self) -> bool:
        """Check if the SSTable is currently loaded in memory."""
        return self._loaded
    
    @property
    def access_count(self) -> int:
        """Get the number of times this SSTable has been accessed."""
        return self._access_count
    
    def __str__(self) -> str:
        """String representation."""
        loaded_str = "loaded" if self._loaded else "not loaded"
        return f"LazySSTable({self.dirname}, {loaded_str}, accesses={self._access_count})"
