"""
SSTableManager - Manages all SSTable operations with leveled compaction.

This class encapsulates all SSTable-related functionality including:
- Level-based SSTable organization
- Manifest operations
- Leveled compaction logic
- Thread-safe operations
"""
import os
import threading
from typing import List, Optional, Dict
from lsmkv.core.dto import Entry
from lsmkv.storage.sstable import SSTable, SSTableMetadata
from lsmkv.storage.manifest import Manifest


class SSTableManager:
    """
    Manages SSTables with leveled compaction strategy.
    
    Level organization:
    - Level 0: Multiple SSTables allowed, duplicates OK
    - Level 1+: Single SSTable per level, no duplicates
    
    Compaction triggers:
    - Entry count exceeds level limit
    - Total size exceeds level limit
    
    Level size grows exponentially:
    - Level N max size = base_size × (level_ratio ^ N)
    - Level N max entries = base_entries × (level_ratio ^ N)
    """
    
    def __init__(self, sstables_dir: str, manifest_path: str,
                 level_ratio: int = 10,
                 base_level_size_mb: float = 1.0,
                 base_level_entries: int = 1000,
                 max_l0_sstables: int = 4):
        """
        Initialize SSTable manager with leveled compaction.
        
        Args:
            sstables_dir: Directory where SSTable subdirectories are stored
            manifest_path: Path to manifest file
            level_ratio: Size multiplier between levels (default: 10)
            base_level_size_mb: L0 max size in MB (default: 1.0)
            base_level_entries: L0 max entries (default: 1000)
            max_l0_sstables: Max SSTables in L0 before compaction (default: 4)
        """
        self.sstables_dir = sstables_dir
        self.manifest = Manifest(manifest_path)
        
        # Level-based organization
        self.levels: Dict[int, List[SSTable]] = {}
        
        # Configuration
        self.level_ratio = level_ratio
        self.base_level_size_mb = base_level_size_mb
        self.base_level_entries = base_level_entries
        self.max_l0_sstables = max_l0_sstables
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Create SSTables directory if it doesn't exist
        os.makedirs(self.sstables_dir, exist_ok=True)
        
        print(f"[SSTableManager] Initialized with leveled compaction:")
        print(f"  - Level ratio: {level_ratio}")
        print(f"  - L0 max: {max_l0_sstables} SSTables, {base_level_entries} entries, {base_level_size_mb}MB")
        print(f"  - L1 max: {base_level_entries * level_ratio} entries, {base_level_size_mb * level_ratio}MB")
    
    def _get_level_max_size_bytes(self, level: int) -> int:
        """
        Calculate maximum size in bytes for a level.
        
        Args:
            level: Level number (0, 1, 2, ...)
            
        Returns:
            Maximum size in bytes for this level
        """
        base_size_bytes = int(self.base_level_size_mb * 1024 * 1024)
        return base_size_bytes * (self.level_ratio ** level)
    
    def _get_level_max_entries(self, level: int) -> int:
        """
        Calculate maximum entries for a level.
        
        Args:
            level: Level number (0, 1, 2, ...)
            
        Returns:
            Maximum entries for this level
        """
        return self.base_level_entries * (self.level_ratio ** level)
    
    def _get_level_stats(self, level: int) -> dict:
        """
        Get statistics for a specific level.
        
        Args:
            level: Level number
            
        Returns:
            Dictionary with level statistics
        """
        if level not in self.levels or not self.levels[level]:
            return {
                "num_sstables": 0,
                "total_entries": 0,
                "total_size_bytes": 0,
            }
        
        sstables = self.levels[level]
        total_size = sum(s.size_bytes() for s in sstables)
        
        # Estimate entries (read from manifest if available)
        total_entries = 0
        for sstable in sstables:
            # Find in manifest
            for entry in self.manifest.get_all_entries():
                if entry.sstable_id == sstable.sstable_id:
                    total_entries += entry.num_entries
                    break
        
        return {
            "num_sstables": len(sstables),
            "total_entries": total_entries,
            "total_size_bytes": total_size,
        }
    
    def load_from_manifest(self):
        """
        Load existing SSTables from manifest file.
        Called during initialization to recover SSTables with level organization.
        """
        with self.lock:
            entries = self.manifest.get_all_entries()
            
            for entry in entries:
                sstable = SSTable(self.sstables_dir, entry.sstable_id)
                if sstable.exists():
                    # Add to appropriate level
                    level = entry.level
                    if level not in self.levels:
                        self.levels[level] = []
                    self.levels[level].append(sstable)
            
            total_sstables = sum(len(sstables) for sstables in self.levels.values())
            if total_sstables > 0:
                print(f"[SSTableManager] Loaded {total_sstables} existing SSTables from manifest")
                for level, sstables in sorted(self.levels.items()):
                    print(f"  - Level {level}: {len(sstables)} SSTable(s)")
    
    def add_sstable(self, entries: List[Entry], level: int = 0, 
                    auto_compact: bool = True) -> SSTableMetadata:
        """
        Create a new SSTable from entries and add to specified level.
        
        This method:
        1. Gets next SSTable ID from manifest
        2. Creates new SSTable with directory structure
        3. Writes entries (creates Bloom filter and sparse index)
        4. Updates manifest
        5. Adds to in-memory collection at specified level
        6. Triggers auto-compaction if enabled
        
        Args:
            entries: List of entries to write (must be sorted by key)
            level: Level to add SSTable to (default: 0)
            auto_compact: Whether to trigger auto-compaction (default: True)
            
        Returns:
            Metadata about the created SSTable
        """
        if not entries:
            raise ValueError("Cannot create SSTable from empty entries")
        
        with self.lock:
            # Get next SSTable ID
            sstable_id = self.manifest.get_next_id()
            
            # Create new SSTable
            sstable = SSTable(self.sstables_dir, sstable_id)
            
            # Write entries (creates data.db, bloom_filter.bf, sparse_index.idx)
            metadata = sstable.write(entries)
            
            # Add to manifest with level information
            self.manifest.add_sstable(
                dirname=metadata.dirname,
                num_entries=metadata.num_entries,
                min_key=metadata.min_key,
                max_key=metadata.max_key,
                level=level,
                sstable_id=sstable_id
            )
            
            # Add to in-memory collection at appropriate level
            if level not in self.levels:
                self.levels[level] = []
            self.levels[level].append(sstable)
            
            print(f"[SSTableManager] Created SSTable {metadata.dirname} at L{level} with {metadata.num_entries} entries")
            
            # Trigger auto-compaction if enabled
            if auto_compact:
                self._auto_compact()
            
            return metadata
    
    def get(self, key: str) -> Optional[Entry]:
        """
        Search SSTables for a key using level-based search.
        
        Search order: L0 (newest to oldest) → L1 → L2 → ...
        
        Uses Bloom filters and sparse indexes for efficient lookup.
        
        Args:
            key: The key to search for
            
        Returns:
            Entry if found, None otherwise
        """
        with self.lock:
            # Search level by level (L0, L1, L2, ...)
            for level in sorted(self.levels.keys()):
                sstables = self.levels[level]
                
                # Within a level, search newest to oldest (important for L0)
                for sstable in reversed(sstables):
                    entry = sstable.get(key)
                    if entry:
                        return entry
            
            return None
    
    def get_all_entries(self) -> List[Entry]:
        """
        Get all entries from all SSTables across all levels.
        
        Used during full compaction to collect all data.
        
        Returns:
            List of all entries from all SSTables
        """
        with self.lock:
            all_entries = []
            for level in sorted(self.levels.keys()):
                for sstable in self.levels[level]:
                    all_entries.extend(sstable.read_all())
            return all_entries
    
    def _should_compact_level(self, level: int) -> bool:
        """
        Check if a level should be compacted to next level.
        
        Compaction triggers:
        1. L0: Number of SSTables >= max_l0_sstables
        2. Any level: Total entries >= level max
        3. Any level: Total size >= level max
        
        Args:
            level: Level to check
            
        Returns:
            True if level should be compacted
        """
        if level not in self.levels or not self.levels[level]:
            return False
        
        stats = self._get_level_stats(level)
        
        # Special case for L0: check SSTable count
        if level == 0:
            if len(self.levels[level]) >= self.max_l0_sstables:
                return True
        
        # Check entry count limit
        max_entries = self._get_level_max_entries(level)
        if stats["total_entries"] >= max_entries:
            return True
        
        # Check size limit
        max_size = self._get_level_max_size_bytes(level)
        if stats["total_size_bytes"] >= max_size:
            return True
        
        return False
    
    def _compact_level_to_next(self, level: int) -> Optional[SSTableMetadata]:
        """
        Compact a specific level to the next level.
        
        Process:
        - L0 → L1: Merge all L0 SSTables, possibly merge with L1
        - L1+ → L(N+1): Merge single SSTable with next level
        
        Args:
            level: Level to compact (source level)
            
        Returns:
            Metadata of new SSTable at next level, or None if nothing to compact
        """
        if level not in self.levels or not self.levels[level]:
            return None
        
        next_level = level + 1
        
        print(f"[SSTableManager] Compacting L{level} → L{next_level}")
        
        # Collect entries from current level
        current_entries = []
        for sstable in self.levels[level]:
            current_entries.extend(sstable.read_all())
        
        print(f"[SSTableManager] L{level}: {len(self.levels[level])} SSTable(s), {len(current_entries)} entries")
        
        # If next level exists, merge with it
        next_entries = []
        if next_level in self.levels and self.levels[next_level]:
            for sstable in self.levels[next_level]:
                next_entries.extend(sstable.read_all())
            print(f"[SSTableManager] L{next_level}: {len(self.levels[next_level])} SSTable(s), {len(next_entries)} entries (will merge)")
        
        # Merge all entries
        all_entries = current_entries + next_entries
        
        # Deduplicate: keep entry with highest timestamp per key
        key_map = {}
        for entry in all_entries:
            if entry.key not in key_map:
                key_map[entry.key] = entry
            else:
                if entry.timestamp > key_map[entry.key].timestamp:
                    key_map[entry.key] = entry
        
        # Remove tombstones
        live_entries = [
            entry for entry in key_map.values()
            if not entry.is_deleted
        ]
        
        if not live_entries:
            print(f"[SSTableManager] No live entries after compaction (all deleted)")
            # Clean up current and next levels
            self._delete_level_sstables(level)
            if next_level in self.levels:
                self._delete_level_sstables(next_level)
            return None
        
        # Sort by key
        live_entries.sort(key=lambda e: e.key)
        
        print(f"[SSTableManager] After merge: {len(live_entries)} unique live entries")
        
        # Delete old SSTables from current and next levels
        self._delete_level_sstables(level)
        if next_level in self.levels:
            self._delete_level_sstables(next_level)
        
        # Create new SSTable at next level (disable auto-compact to avoid recursion)
        metadata = self.add_sstable(live_entries, level=next_level, auto_compact=False)
        
        print(f"[SSTableManager] Created {metadata.dirname} at L{next_level}")
        
        return metadata
    
    def _delete_level_sstables(self, level: int):
        """
        Delete all SSTables at a specific level.
        
        Args:
            level: Level to clear
        """
        if level not in self.levels:
            return
        
        # Get SSTable IDs
        sstable_ids = [s.sstable_id for s in self.levels[level]]
        
        # Delete SSTable directories
        for sstable in self.levels[level]:
            if sstable.exists():
                sstable.delete()
        
        # Remove from manifest
        if sstable_ids:
            self.manifest.remove_sstables(sstable_ids)
        
        # Clear level
        self.levels[level] = []
    
    def _auto_compact(self):
        """
        Automatically compact levels that exceed their limits.
        
        Checks each level from L0 upward and compacts if necessary.
        This can cascade (L0→L1 might trigger L1→L2, etc.)
        """
        # Start from L0 and work upward
        max_level = max(self.levels.keys()) if self.levels else 0
        
        for level in range(max_level + 1):
            if self._should_compact_level(level):
                stats = self._get_level_stats(level)
                print(f"[SSTableManager] L{level} needs compaction: {stats['num_sstables']} SSTables, "
                      f"{stats['total_entries']} entries, {stats['total_size_bytes']} bytes")
                
                self._compact_level_to_next(level)
                
                # After compacting, check next level (cascade)
                # This is handled automatically by checking all levels
    
    def compact(self, target_level: Optional[int] = None) -> SSTableMetadata:
        """
        Compact all SSTables into a single SSTable.
        
        If target_level is specified, compacts all data to that level.
        Otherwise, finds the highest level and compacts everything there.
        
        Process:
        1. Read all entries from all SSTables across all levels
        2. Deduplicate by keeping latest version (highest timestamp)
        3. Remove tombstones (deleted entries)
        4. Sort entries by key
        5. Delete all old SSTable directories
        6. Create new compacted SSTable at target level
        7. Update manifest
        
        Args:
            target_level: Level to compact to (default: highest existing level + 1)
        
        Returns:
            Metadata about the compacted SSTable
            
        Raises:
            ValueError: If no SSTables exist or all entries are deleted
        """
        with self.lock:
            total_sstables = sum(len(sstables) for sstables in self.levels.values())
            
            if total_sstables == 0:
                raise ValueError("No SSTables to compact")
            
            # Determine target level
            if target_level is None:
                # Use highest existing level + 1, or L1 if only L0 exists
                max_level = max(self.levels.keys()) if self.levels else 0
                target_level = max_level + 1 if max_level == 0 else max_level
            
            # Collect all entries from all SSTables
            all_entries = self.get_all_entries()
            
            if not all_entries:
                raise ValueError("No entries found in SSTables")
            
            print(f"[SSTableManager] Full compaction: {total_sstables} SSTables across {len(self.levels)} levels → L{target_level}")
            print(f"[SSTableManager] Total entries: {len(all_entries)}")
            
            # Build a map to keep only the latest entry for each key
            key_map = {}
            for entry in all_entries:
                if entry.key not in key_map:
                    key_map[entry.key] = entry
                else:
                    # Keep entry with higher timestamp
                    if entry.timestamp > key_map[entry.key].timestamp:
                        key_map[entry.key] = entry
            
            # Remove tombstones (deleted entries)
            compacted_entries = [
                entry for entry in key_map.values()
                if not entry.is_deleted
            ]
            
            if not compacted_entries:
                raise ValueError("No live entries after compaction (all deleted)")
            
            # Sort entries by key
            compacted_entries.sort(key=lambda e: e.key)
            
            print(f"[SSTableManager] After deduplication: {len(compacted_entries)} unique live entries")
            
            # Delete all old SSTables from all levels
            for level in list(self.levels.keys()):
                self._delete_level_sstables(level)
            
            # Create new compacted SSTable at target level (disable auto-compact)
            metadata = self.add_sstable(compacted_entries, level=target_level, auto_compact=False)
            
            print(f"[SSTableManager] Full compaction complete: {metadata.dirname} at L{target_level}")
            
            return metadata
    
    def remove_sstable(self, sstable_id: int):
        """
        Remove a specific SSTable from collection.
        
        Args:
            sstable_id: ID of SSTable to remove
        """
        with self.lock:
            # Find and remove from appropriate level
            for level in self.levels:
                self.levels[level] = [s for s in self.levels[level] if s.sstable_id != sstable_id]
            
            # Remove from manifest
            self.manifest.remove_sstables([sstable_id])
    
    def close(self):
        """
        Close all SSTables across all levels.
        
        This closes mmap file handles and syncs Bloom filters to disk.
        """
        with self.lock:
            total_sstables = sum(len(sstables) for sstables in self.levels.values())
            print(f"[SSTableManager] Closing {total_sstables} SSTables across {len(self.levels)} levels...")
            
            for level in self.levels:
                for sstable in self.levels[level]:
                    sstable.close()
            
            print(f"[SSTableManager] All SSTables closed")
    
    def stats(self) -> dict:
        """
        Calculate statistics about SSTables across all levels.
        
        Returns:
            Dictionary with SSTable statistics including per-level breakdown
        """
        with self.lock:
            total_sstables = sum(len(sstables) for sstables in self.levels.values())
            total_size = sum(
                s.size_bytes() 
                for sstables in self.levels.values() 
                for s in sstables
            )
            
            # Per-level stats
            level_stats = {}
            for level in sorted(self.levels.keys()):
                level_stats[f"l{level}_sstables"] = len(self.levels[level])
                level_stats[f"l{level}_size_bytes"] = sum(s.size_bytes() for s in self.levels[level])
            
            return {
                "num_sstables": total_sstables,
                "total_sstable_size_bytes": total_size,
                "num_levels": len(self.levels),
                **level_stats
            }
    
    def count(self) -> int:
        """
        Get the total number of SSTables across all levels.
        
        Returns:
            Total number of SSTables
        """
        with self.lock:
            return sum(len(sstables) for sstables in self.levels.values())
    
    def is_empty(self) -> bool:
        """
        Check if there are any SSTables in any level.
        
        Returns:
            True if no SSTables exist, False otherwise
        """
        with self.lock:
            return self.count() == 0
    
    def __len__(self) -> int:
        """Support len() operator."""
        return self.count()
    
    def get_level_info(self) -> dict:
        """
        Get detailed information about each level.
        
        Returns:
            Dictionary mapping level to stats
        """
        with self.lock:
            info = {}
            for level in sorted(self.levels.keys()):
                stats = self._get_level_stats(level)
                info[level] = {
                    "sstables": len(self.levels[level]),
                    "entries": stats["total_entries"],
                    "size_bytes": stats["total_size_bytes"],
                    "max_entries": self._get_level_max_entries(level),
                    "max_size_bytes": self._get_level_max_size_bytes(level),
                }
            return info
    
    def get_all_sstables(self) -> List[SSTable]:
        """
        Get all SSTables across all levels.
        
        Returns:
            List of all SSTable objects
        """
        with self.lock:
            all_sstables = []
            for level in sorted(self.levels.keys()):
                all_sstables.extend(self.levels[level])
            return all_sstables
    
    @property
    def sstables(self) -> List[SSTable]:
        """
        Property to access all SSTables (for backward compatibility).
        
        Returns:
            List of all SSTable objects across all levels
        """
        return self.get_all_sstables()
    
    def __len__(self) -> int:
        """Support len() operator."""
        return self.count()
    
    def __str__(self) -> str:
        """String representation."""
        with self.lock:
            total = self.count()
            num_levels = len(self.levels)
            return f"SSTableManager(sstables={total}, levels={num_levels}, dir='{self.sstables_dir}')"
