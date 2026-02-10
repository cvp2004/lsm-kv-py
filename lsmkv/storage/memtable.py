"""
Memtable implementation using skiplistcollections with dictionary.
"""
from typing import Optional, List, Dict
from skiplistcollections import SkipListDict
from lsmkv.core.dto import Entry


class Memtable:
    """In-memory table using skiplist and dictionary for fast lookups."""
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize the memtable.
        
        Args:
            max_size: Maximum number of entries before flush is needed
        """
        # Use SkipListDict for sorted storage (O(log n) operations)
        self.skiplist = SkipListDict(capacity=max(max_size * 2, 16))
        # Use dict for O(1) lookups
        self.key_map: Dict[str, Entry] = {}
        self.max_size = max_size
    
    def put(self, entry: Entry):
        """
        Insert or update an entry in the memtable.
        
        Args:
            entry: The entry to insert
        """
        # Update both skiplist and dict
        self.skiplist[entry.key] = entry
        self.key_map[entry.key] = entry
    
    def get(self, key: str, include_tombstones: bool = False) -> Optional[Entry]:
        """
        Get an entry from the memtable.
        
        Args:
            key: The key to look up
            include_tombstones: If True, return tombstone entries (for stopping search)
            
        Returns:
            The entry if found (including tombstones if include_tombstones=True),
            None otherwise
        """
        # O(1) lookup using dictionary
        entry = self.key_map.get(key)
        if entry:
            if include_tombstones or not entry.is_deleted:
                return entry
        return None
    
    def delete(self, entry: Entry):
        """
        Mark an entry as deleted (tombstone).
        
        Args:
            entry: The entry to delete (with is_deleted=True)
        """
        # Store tombstone in both skiplist and dict
        self.skiplist[entry.key] = entry
        self.key_map[entry.key] = entry
    
    def is_full(self) -> bool:
        """
        Check if the memtable has reached its size limit.
        
        Returns:
            True if full, False otherwise
        """
        return len(self.key_map) >= self.max_size
    
    def clear(self):
        """Clear all entries from the memtable."""
        self.skiplist = SkipListDict(capacity=max(self.max_size * 2, 16))
        self.key_map = {}
    
    def __len__(self) -> int:
        """Return the number of entries in the memtable."""
        return len(self.key_map)
    
    def get_all_entries(self) -> List[Entry]:
        """
        Get all entries from the memtable in sorted order.
        
        Returns:
            List of entries sorted by key
        """
        # SkipListDict maintains sorted order
        return list(self.skiplist.values())
