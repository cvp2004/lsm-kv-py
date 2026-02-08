"""
Sparse index implementation for fast SSTable lookups.
Uses bisect for efficient binary search operations.
"""
import bisect
import struct
from typing import List, Optional, Tuple


class IndexEntry:
    """Entry in the sparse index."""
    
    def __init__(self, key: str, offset: int):
        """
        Initialize an index entry.
        
        Args:
            key: The key at this position
            offset: Byte offset in the SSTable file
        """
        self.key = key
        self.offset = offset
    
    def __lt__(self, other):
        """Compare entries by key for bisect operations."""
        if isinstance(other, IndexEntry):
            return self.key < other.key
        # Support comparing with string keys directly
        return self.key < other
    
    def __le__(self, other):
        """Less than or equal comparison."""
        if isinstance(other, IndexEntry):
            return self.key <= other.key
        return self.key <= other
    
    def __gt__(self, other):
        """Greater than comparison."""
        if isinstance(other, IndexEntry):
            return self.key > other.key
        return self.key > other
    
    def __ge__(self, other):
        """Greater than or equal comparison."""
        if isinstance(other, IndexEntry):
            return self.key >= other.key
        return self.key >= other
    
    def __eq__(self, other):
        """Equality comparison."""
        if isinstance(other, IndexEntry):
            return self.key == other.key
        return self.key == other
    
    def to_bytes(self) -> bytes:
        """
        Serialize index entry to bytes.
        
        Returns:
            Serialized index entry
        """
        # Format: [key_length(4)][key][offset(8)]
        key_bytes = self.key.encode('utf-8')
        key_length = len(key_bytes)
        return struct.pack('<I', key_length) + key_bytes + struct.pack('<Q', self.offset)
    
    @staticmethod
    def from_bytes(data: bytes, offset: int = 0) -> Tuple['IndexEntry', int]:
        """
        Deserialize index entry from bytes.
        
        Args:
            data: Byte data
            offset: Starting offset in data
            
        Returns:
            Tuple of (IndexEntry, bytes_consumed)
        """
        # Read key length
        key_length = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        
        # Read key
        key = data[offset:offset+key_length].decode('utf-8')
        offset += key_length
        
        # Read file offset
        file_offset = struct.unpack('<Q', data[offset:offset+8])[0]
        offset += 8
        
        return IndexEntry(key, file_offset), offset


class SparseIndex:
    """
    Sparse index for fast SSTable lookups.
    Stores every Nth key with its byte offset in the SSTable.
    
    Uses Python's bisect module for efficient O(log n) binary search:
    - bisect_left: Find position where key would be inserted (ceil operation)
    - bisect_right: Find position after all equal keys (floor operation)
    
    The entries list must remain sorted by key for bisect to work correctly.
    """
    
    def __init__(self, block_size: int = 4):
        """
        Initialize sparse index.
        
        Args:
            block_size: Store index entry every N keys (default: 4)
        """
        self.block_size = block_size
        self.entries: List[IndexEntry] = []
    
    def add_entry(self, key: str, offset: int):
        """
        Add an entry to the sparse index.
        
        Args:
            key: The key
            offset: Byte offset in SSTable file
        """
        self.entries.append(IndexEntry(key, offset))
    
    def find_block_offset(self, key: str) -> int:
        """
        Find the byte offset to start scanning for a key (floor operation).
        Returns the offset of the largest indexed key <= target key.
        
        Uses bisect_right to find the insertion point, then goes one position back
        to get the floor entry.
        
        Args:
            key: The key to search for
            
        Returns:
            Byte offset to start scanning, or 0 if key < first indexed key
        """
        if not self.entries:
            return 0
        
        # Use bisect_right to find where key would be inserted
        # This gives us the position after all entries <= key
        pos = bisect.bisect_right(self.entries, key)
        
        # Go back one position to get the floor entry (largest key <= target)
        if pos > 0:
            return self.entries[pos - 1].offset
        else:
            # Key is smaller than first indexed key, start from beginning
            return 0
    
    def find_ceil_offset(self, key: str) -> Optional[int]:
        """
        Find the byte offset of the smallest indexed key >= target key (ceil operation).
        
        Uses bisect_left to find the insertion point, which gives us the ceil entry.
        
        Args:
            key: The key to search for
            
        Returns:
            Byte offset of ceil entry, or None if no entry >= key
        """
        if not self.entries:
            return None
        
        # Use bisect_left to find where key would be inserted
        # This gives us the position of the first entry >= key
        pos = bisect.bisect_left(self.entries, key)
        
        if pos < len(self.entries):
            return self.entries[pos].offset
        else:
            # No entry >= key
            return None
    
    def get_scan_range(self, key: str) -> Tuple[int, Optional[int]]:
        """
        Get the byte range to scan for a key.
        
        Uses bisect operations to efficiently find:
        - start_offset: floor(key) - largest indexed key <= target
        - end_offset: ceil(key + 1) - smallest indexed key > target
        
        Args:
            key: The key to search for
            
        Returns:
            Tuple of (start_offset, end_offset)
            end_offset is None if scanning to end of file
        """
        if not self.entries:
            return 0, None
        
        # Find the starting offset (floor - largest key <= target)
        start_offset = self.find_block_offset(key)
        
        # Find the ending offset (next indexed key after target)
        # Use bisect_right to find position after all entries <= key
        pos = bisect.bisect_right(self.entries, key)
        
        if pos < len(self.entries):
            # There's an entry after the target key
            end_offset = self.entries[pos].offset
        else:
            # No entry after target key, scan to end
            end_offset = None
        
        return start_offset, end_offset
    
    def to_bytes(self) -> bytes:
        """
        Serialize the sparse index to bytes.
        
        Returns:
            Serialized sparse index
        """
        # Format: [block_size(4)][num_entries(4)][entries...]
        data = struct.pack('<II', self.block_size, len(self.entries))
        
        for entry in self.entries:
            data += entry.to_bytes()
        
        return data
    
    @staticmethod
    def from_bytes(data: bytes) -> 'SparseIndex':
        """
        Deserialize sparse index from bytes.
        
        Args:
            data: Serialized sparse index
            
        Returns:
            Deserialized sparse index
        """
        # Read header
        block_size, num_entries = struct.unpack('<II', data[:8])
        offset = 8
        
        # Create sparse index
        index = SparseIndex(block_size)
        
        # Read entries
        for _ in range(num_entries):
            entry, offset = IndexEntry.from_bytes(data, offset)
            index.entries.append(entry)
        
        return index
    
    def save_to_file(self, filepath: str):
        """
        Save the sparse index to a file.
        
        Args:
            filepath: Path to save the index
        """
        with open(filepath, 'wb') as f:
            f.write(self.to_bytes())
    
    @staticmethod
    def load_from_file(filepath: str) -> 'SparseIndex':
        """
        Load a sparse index from a file.
        
        Args:
            filepath: Path to the index file
            
        Returns:
            Loaded sparse index
        """
        with open(filepath, 'rb') as f:
            data = f.read()
        return SparseIndex.from_bytes(data)
    
    def __str__(self) -> str:
        """String representation of the sparse index."""
        return f"SparseIndex(block_size={self.block_size}, entries={len(self.entries)})"
    
    def __len__(self) -> int:
        """Number of index entries."""
        return len(self.entries)
