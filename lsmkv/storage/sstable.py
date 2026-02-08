"""
SSTable (Sorted String Table) implementation for persistent storage.
Enhanced with Bloom filter, sparse index, and mmap I/O.
"""
import os
import json
import mmap
from typing import List, Optional
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
        Uses Bloom filter for fast negative lookups and sparse index for efficient scanning.
        
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
        
        # Get scan range from sparse index
        start_offset = 0
        end_offset = None
        
        if self._sparse_index:
            start_offset, end_offset = self._sparse_index.get_scan_range(key)
        
        # Scan the relevant section using mmap
        self._mmap.seek(start_offset)
        
        # Read line by line from start_offset
        current_pos = start_offset
        
        while True:
            # Check if we've reached the end offset
            if end_offset is not None and current_pos >= end_offset:
                break
            
            # Read until newline
            line_bytes = b''
            while current_pos < len(self._mmap):
                byte = self._mmap[current_pos:current_pos+1]
                current_pos += 1
                
                if byte == b'\n':
                    break
                line_bytes += byte
            
            if not line_bytes:
                break
            
            try:
                line = line_bytes.decode('utf-8').strip()
                if line:
                    entry_dict = json.loads(line)
                    
                    # Check if this is the key we're looking for
                    if entry_dict["key"] == key:
                        return Entry(
                            key=entry_dict["key"],
                            value=entry_dict["value"],
                            timestamp=entry_dict["timestamp"],
                            is_deleted=entry_dict["is_deleted"]
                        )
                    
                    # Since entries are sorted, if we've passed the key, it's not here
                    if entry_dict["key"] > key:
                        break
            except (json.JSONDecodeError, UnicodeDecodeError):
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
