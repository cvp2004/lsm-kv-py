"""
SSTableManager - Manages all SSTable operations with leveled compaction.

This class encapsulates all SSTable-related functionality including:
- Level-based SSTable organization
- Per-level manifest operations (separate manifest per level)
- Leveled compaction logic with background processing
- Snapshot-based compaction (non-blocking for reads/writes)
- Lazy SSTable loading (only metadata on startup, actual SSTable on demand)
- Background manifest reload with atomic swap
- Thread-safe operations
"""
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Set, Tuple, Union
from lsmkv.core.dto import Entry
from lsmkv.storage.sstable import SSTable, SSTableMetadata, LazySSTable
from lsmkv.storage.manifest import Manifest, ManifestEntry
from lsmkv.storage.level_manifest import LevelManifestManager


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
    
    Lazy Loading:
    - Only SSTable metadata loaded on startup (from manifest)
    - Actual SSTable (bloom filter, sparse index, mmap) loaded on first read
    - Memory efficient for large numbers of SSTables
    
    Background Manifest Reload:
    - Manifest updates trigger background reload
    - Old manifest preserved until new one is ready
    - Atomic swap of manifest data
    """
    
    # Type alias for SSTable or LazySSTable
    SSTableType = Union[SSTable, LazySSTable]
    
    def __init__(self, sstables_dir: str, manifest_path: str,
                 level_ratio: int = 10,
                 base_level_size_mb: float = 1.0,
                 base_level_entries: int = 1000,
                 max_l0_sstables: int = 4,
                 soft_limit_ratio: float = 0.85):
        """
        Initialize SSTable manager with leveled compaction.
        
        Args:
            sstables_dir: Directory where SSTable subdirectories are stored
            manifest_path: Path to OLD manifest file (for migration)
            level_ratio: Size multiplier between levels (default: 10)
            base_level_size_mb: L0 max size in MB (default: 1.0)
            base_level_entries: L0 max entries (default: 1000)
            max_l0_sstables: Max SSTables in L0 before compaction (default: 4)
            soft_limit_ratio: Trigger compaction at this % of hard limit (default: 0.85 = 85%)
        """
        self.sstables_dir = sstables_dir
        
        # Determine data directory from sstables_dir
        self.data_dir = os.path.dirname(sstables_dir)
        
        # Use level-based manifest manager
        # Pass old manifest path for migration from v1 format
        self.level_manifest_manager = LevelManifestManager(
            data_dir=self.data_dir,
            old_manifest_path=manifest_path
        )
        
        # Backward compatibility: keep old manifest reference (read-only after migration)
        self._legacy_manifest_path = manifest_path
        
        # Level-based organization - uses LazySSTable for memory efficiency
        self.levels: Dict[int, List[LazySSTable]] = {}
        
        # Configuration
        self.level_ratio = level_ratio
        self.base_level_size_mb = base_level_size_mb
        self.base_level_entries = base_level_entries
        self.max_l0_sstables = max_l0_sstables
        self.soft_limit_ratio = soft_limit_ratio  # 85% threshold
        
        # Thread safety - use RLock for reentrant operations
        self.lock = threading.RLock()
        
        # Background compaction thread pool (non-blocking compaction)
        self.compaction_executor = ThreadPoolExecutor(
            max_workers=1,  # Single compaction thread to avoid conflicts
            thread_name_prefix="compact-worker"
        )
        
        # Background manifest reload thread pool
        self._manifest_reload_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="manifest-reload"
        )
        
        # Track SSTables being compacted (snapshot isolation)
        self._compacting_sstable_ids: Set[int] = set()
        self._compaction_lock = threading.Lock()
        
        # Pending manifest reload flag and lock (prevents TOCTOU double submission)
        self._manifest_reload_pending = threading.Event()
        self._manifest_reload_lock = threading.Lock()
        
        # Stats
        self.total_compactions = 0
        self.background_compactions = 0
        self.lazy_loads = 0  # Track how many SSTables were loaded on demand
        
        # Create SSTables directory if it doesn't exist
        os.makedirs(self.sstables_dir, exist_ok=True)
        
        print(f"[SSTableManager] Initialized with leveled compaction:")
        print(f"  - Level ratio: {level_ratio}")
        print(f"  - Soft limit: {int(soft_limit_ratio * 100)}% of hard limit")
        print(f"  - Lazy loading: enabled (SSTables loaded on demand)")
        print(f"  - Background compaction: enabled (non-blocking)")
        print(f"  - Per-level manifests: {self.data_dir}/manifests/")
        print(f"  - L0 max: {max_l0_sstables} SSTables, {base_level_entries} entries, {base_level_size_mb}MB")
        print(f"  - L0 soft: {int(max_l0_sstables * soft_limit_ratio)} SSTables, "
              f"{int(base_level_entries * soft_limit_ratio)} entries")
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
        total_size = 0
        for s in sstables:
            try:
                if s.exists():
                    total_size += s.size_bytes()
            except (OSError, FileNotFoundError):
                pass  # SSTable may have been deleted by concurrent compaction
        
        # Get entries count from level manifest (more efficient than searching all entries)
        level_manifest = self.level_manifest_manager.get_level_manifest(level)
        total_entries = level_manifest.total_entries()
        
        return {
            "num_sstables": len(sstables),
            "total_entries": total_entries,
            "total_size_bytes": total_size,
        }
    
    def load_from_manifest(self):
        """
        Load existing SSTables from level manifests using LAZY LOADING.
        Called during initialization to recover SSTables with level organization.
        
        LAZY LOADING: Only SSTable metadata is loaded from manifest.
        Actual SSTable objects (bloom filter, sparse index, mmap) are loaded
        on demand when first accessed for read operations.
        
        Benefits:
        - Fast startup: No file I/O for SSTable data files
        - Memory efficient: SSTables only loaded when needed
        - Scales well: Handles thousands of SSTables without memory pressure
        """
        with self.lock:
            # Discover all existing level manifests
            self.level_manifest_manager.discover_levels()
            
            # Load SSTable METADATA from all level manifests (no actual file I/O)
            for level in self.level_manifest_manager.get_levels():
                level_entries = self.level_manifest_manager.get_level_entries(level)
                
                for entry in level_entries:
                    # Create LazySSTable with metadata (no disk I/O)
                    metadata = SSTableMetadata(
                        sstable_id=entry.sstable_id,
                        dirname=entry.dirname,
                        num_entries=entry.num_entries,
                        min_key=entry.min_key,
                        max_key=entry.max_key
                    )
                    
                    lazy_sstable = LazySSTable(
                        sstables_dir=self.sstables_dir,
                        sstable_id=entry.sstable_id,
                        metadata=metadata
                    )
                    
                    # Only add if the SSTable directory exists
                    if lazy_sstable.exists():
                        if level not in self.levels:
                            self.levels[level] = []
                        self.levels[level].append(lazy_sstable)
            
            total_sstables = sum(len(sstables) for sstables in self.levels.values())
            if total_sstables > 0:
                print(f"[SSTableManager] Loaded {total_sstables} existing SSTables from level manifests")
                for level, sstables in sorted(self.levels.items()):
                    print(f"  - Level {level}: {len(sstables)} SSTable(s)")
    
    def add_sstable(self, entries: List[Entry], level: int = 0, 
                    auto_compact: bool = True) -> SSTableMetadata:
        """
        Create a new SSTable from entries and add to specified level.
        
        This method:
        1. Gets next SSTable ID from level manifest manager
        2. Creates new SSTable with directory structure
        3. Writes entries (creates Bloom filter and sparse index)
        4. Updates level-specific manifest
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
            # Get next SSTable ID from level manifest manager
            sstable_id = self.level_manifest_manager.get_next_id()
            
            # Create new SSTable
            sstable = SSTable(self.sstables_dir, sstable_id)
            
            # Write entries (creates data.db, bloom_filter.bf, sparse_index.idx)
            metadata = sstable.write(entries)
            
            # Add to level-specific manifest
            self.level_manifest_manager.add_sstable(
                dirname=metadata.dirname,
                num_entries=metadata.num_entries,
                min_key=metadata.min_key,
                max_key=metadata.max_key,
                level=level,
                sstable_id=sstable_id
            )
            
            # Wrap in LazySSTable for consistent handling
            # The SSTable is already loaded since we just wrote it
            lazy_sstable = LazySSTable(
                sstables_dir=self.sstables_dir,
                sstable_id=sstable_id,
                metadata=metadata
            )
            # Pre-set the loaded SSTable to avoid re-loading
            lazy_sstable._sstable = sstable
            lazy_sstable._loaded = True
            
            # Add to in-memory collection at appropriate level
            if level not in self.levels:
                self.levels[level] = []
            self.levels[level].append(lazy_sstable)
            
            # Trigger background manifest reload for other readers
            self._trigger_manifest_reload()
            
            print(f"[SSTableManager] Created SSTable {metadata.dirname} at L{level} with {metadata.num_entries} entries")
            
            need_auto_compact = auto_compact
        
        # Trigger auto-compaction outside lock — snapshot reads are disk I/O
        if need_auto_compact:
            self._auto_compact()
        
        return metadata
    
    def get(self, key: str) -> Optional[Entry]:
        """
        Search SSTables for a key using level-based search.

        Search order: L0 (newest to oldest) → L1 → L2 → ...
        Snapshot taken under lock; I/O performed without lock to avoid serializing reads.

        Args:
            key: The key to search for

        Returns:
            Entry if found, None otherwise
        """
        with self.lock:
            snapshot = {
                level: list(sstables)
                for level, sstables in sorted(self.levels.items())
            }
        for level in sorted(snapshot.keys()):
            for sstable in reversed(snapshot[level]):
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
        
        Uses SOFT LIMITS (85% of hard limits) to trigger compaction early.
        This prevents hitting actual limits and maintains performance headroom.
        
        Compaction triggers (at 85% of hard limits):
        1. L0: Number of SSTables >= (max_l0_sstables × 0.85)
        2. Any level: Total entries >= (level_max_entries × 0.85)
        3. Any level: Total size >= (level_max_size × 0.85)
        
        Args:
            level: Level to check
            
        Returns:
            True if level should be compacted
        """
        if level not in self.levels or not self.levels[level]:
            return False
        
        stats = self._get_level_stats(level)
        
        # Calculate soft limits (85% of hard limits)
        soft_limit = self.soft_limit_ratio
        
        # Special case for L0: check SSTable count with soft limit
        if level == 0:
            soft_max_sstables = int(self.max_l0_sstables * soft_limit)
            if len(self.levels[level]) >= soft_max_sstables:
                return True
        
        # Check entry count limit (soft)
        max_entries = self._get_level_max_entries(level)
        soft_max_entries = int(max_entries * soft_limit)
        if stats["total_entries"] >= soft_max_entries:
            return True
        
        # Check size limit (soft)
        max_size = self._get_level_max_size_bytes(level)
        soft_max_size = int(max_size * soft_limit)
        if stats["total_size_bytes"] >= soft_max_size:
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

        # Only drop tombstones at the bottommost level to prevent resurrection
        is_bottommost = not any(
            lvl > next_level and self.levels.get(lvl)
            for lvl in self.levels
        )

        if is_bottommost:
            merged_entries = [e for e in key_map.values() if not e.is_deleted]
        else:
            merged_entries = list(key_map.values())

        if not merged_entries:
            print(f"[SSTableManager] No entries after compaction (all deleted at bottommost)")
            self._delete_level_sstables(level)
            if next_level in self.levels:
                self._delete_level_sstables(next_level)
            return None

        # Sort by key
        merged_entries.sort(key=lambda e: e.key)

        print(f"[SSTableManager] After merge: {len(merged_entries)} entries (bottommost={is_bottommost})")
        
        # Record old SSTables before creating new (crash-safe: create before delete)
        old_source = list(self.levels[level])
        old_next = list(self.levels.get(next_level, []))
        
        # Create new SSTable first — only after it's persisted do we delete old
        metadata = self.add_sstable(merged_entries, level=next_level, auto_compact=False)
        new_sstable_id = metadata.sstable_id
        
        # Delete old SSTables (exclude new one at next_level)
        for sstable in old_source:
            self.levels[level] = [s for s in self.levels[level] if s.sstable_id != sstable.sstable_id]
            self.level_manifest_manager.remove_sstables([sstable.sstable_id], level=level)
            if sstable.exists():
                sstable.delete()
        for sstable in old_next:
            self.levels[next_level] = [s for s in self.levels[next_level] if s.sstable_id != sstable.sstable_id]
            self.level_manifest_manager.remove_sstables([sstable.sstable_id], level=next_level)
            if sstable.exists():
                sstable.delete()
        
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
        
        # Clear level manifest (more efficient than removing individual entries)
        self.level_manifest_manager.clear_level(level)
        
        # Clear in-memory level
        self.levels[level] = []
    
    def _auto_compact(self):
        """
        Automatically trigger background compaction for levels exceeding limits.
        
        NON-BLOCKING: Submits compaction to background thread pool.
        The main thread returns immediately after scheduling.
        
        Uses snapshot-based compaction:
        1. Take snapshot of SSTables to compact
        2. Submit to background thread
        3. Background thread performs merge
        4. Only after new SSTable is persisted, old ones are deleted
        """
        # Check each level from L0 upward
        max_level = max(self.levels.keys()) if self.levels else 0
        
        for level in range(max_level + 1):
            if self._should_compact_level(level):
                # Check if already compacting this level
                with self._compaction_lock:
                    level_sstable_ids = {s.sstable_id for s in self.levels.get(level, [])}
                    if level_sstable_ids & self._compacting_sstable_ids:
                        # Already compacting some of these SSTables
                        continue
                
                # Take snapshot and submit to background
                self._submit_background_compaction(level)
    
    def _submit_background_compaction(self, level: int):
        """
        Submit a compaction job to the background thread.
        
        Takes a snapshot of SSTables to compact before releasing lock.
        """
        # Take snapshot while holding lock
        snapshot = self._take_compaction_snapshot(level)
        if not snapshot:
            return
        
        source_level, next_level, source_ids, next_ids, source_entries, next_entries = snapshot
        
        # Mark SSTables as being compacted
        with self._compaction_lock:
            self._compacting_sstable_ids.update(source_ids)
            self._compacting_sstable_ids.update(next_ids)
        
        stats = self._get_level_stats(level)
        print(f"[SSTableManager] L{level} needs compaction: {stats['num_sstables']} SSTables, "
              f"{stats['total_entries']} entries (background)")
        
        # Submit to background thread (non-blocking)
        self.compaction_executor.submit(
            self._background_compact,
            source_level, next_level, source_ids, next_ids, source_entries, next_entries
        )
        self.background_compactions += 1
    
    def _take_compaction_snapshot(self, level: int) -> Optional[Tuple]:
        """
        Take a snapshot of SSTables for compaction.
        
        Copies sstable references under lock; reads entries without lock to avoid
        blocking add_sstable/get during disk I/O.
        Returns tuple: (source_level, next_level, source_ids, next_ids, source_entries, next_entries)
        """
        with self.lock:
            if level not in self.levels or not self.levels[level]:
                return None
            
            next_level = level + 1
            source_sstables = list(self.levels[level])
            source_ids = {s.sstable_id for s in source_sstables}
            next_sstables = list(self.levels[next_level]) if next_level in self.levels else []
            next_ids = {s.sstable_id for s in next_sstables}
        
        # Read entries outside lock — disk I/O should not block other operations
        source_entries = []
        for sstable in source_sstables:
            source_entries.extend(sstable.read_all())
        next_entries = []
        for sstable in next_sstables:
            next_entries.extend(sstable.read_all())
        
        return (level, next_level, source_ids, next_ids, source_entries, next_entries)
    
    def _background_compact(self, source_level: int, next_level: int,
                            source_ids: Set[int], next_ids: Set[int],
                            source_entries: List[Entry], next_entries: List[Entry]):
        """
        Background compaction worker.
        
        Process:
        1. Merge entries from snapshot (already read)
        2. Create new SSTable (persisted to disk)
        3. Only after success: atomically update levels and manifests
        4. Delete old SSTables
        """
        try:
            start_time = time.time()
            
            print(f"[Compact-Worker] Starting L{source_level} → L{next_level} compaction")
            print(f"[Compact-Worker] Source: {len(source_ids)} SSTables, {len(source_entries)} entries")
            if next_entries:
                print(f"[Compact-Worker] Merging with L{next_level}: {len(next_ids)} SSTables, {len(next_entries)} entries")
            
            # Merge all entries
            all_entries = source_entries + next_entries
            
            # Deduplicate: keep entry with highest timestamp per key
            key_map = {}
            for entry in all_entries:
                if entry.key not in key_map:
                    key_map[entry.key] = entry
                else:
                    if entry.timestamp > key_map[entry.key].timestamp:
                        key_map[entry.key] = entry
            
            # Only drop tombstones at the bottommost level to prevent resurrection
            with self.lock:
                is_bottommost = not any(
                    lvl > next_level and self.levels.get(lvl)
                    for lvl in self.levels
                )

            if is_bottommost:
                merged_entries = [e for e in key_map.values() if not e.is_deleted]
            else:
                merged_entries = list(key_map.values())

            if not merged_entries:
                print(f"[Compact-Worker] No entries after merge (all deleted at bottommost)")
                self._finalize_compaction(source_level, next_level, source_ids, next_ids, None)
                return

            # Sort by key
            merged_entries.sort(key=lambda e: e.key)

            print(f"[Compact-Worker] After merge: {len(merged_entries)} entries (bottommost={is_bottommost})")

            # Create new SSTable (this persists to disk)
            new_sstable, new_metadata = self._create_sstable_for_compaction(merged_entries, next_level)
            
            # Atomically finalize: update levels, manifests, delete old
            self._finalize_compaction(source_level, next_level, source_ids, next_ids, new_sstable)
            
            elapsed = time.time() - start_time
            print(f"[Compact-Worker] Completed L{source_level} → L{next_level} in {elapsed:.3f}s")
            
            self.total_compactions += 1
            
            # Check if next level now needs compaction (cascade)
            with self.lock:
                if self._should_compact_level(next_level):
                    self._submit_background_compaction(next_level)
            
        except Exception as e:
            print(f"[Compact-Worker] Error during compaction: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Remove from compacting set
            with self._compaction_lock:
                self._compacting_sstable_ids -= source_ids
                self._compacting_sstable_ids -= next_ids
    
    def _create_sstable_for_compaction(self, entries: List[Entry], level: int) -> Tuple[SSTable, SSTableMetadata]:
        """
        Create a new SSTable for compaction result.
        
        This creates the SSTable on disk but does NOT update in-memory state.
        """
        # Get next SSTable ID (thread-safe)
        with self.lock:
            sstable_id = self.level_manifest_manager.global_manifest.get_next_id()
        
        # Create SSTable (file I/O, no lock needed)
        sstable = SSTable(self.sstables_dir, sstable_id)
        metadata = sstable.write(entries)
        
        return sstable, metadata
    
    def _finalize_compaction(self, source_level: int, next_level: int,
                             source_ids: Set[int], next_ids: Set[int],
                             new_sstable: Optional[SSTable]):
        """
        Atomically finalize compaction.
        
        Only called after new SSTable is persisted to disk.
        Updates in-memory state, manifests, and deletes old SSTables.
        """
        # Collect old LazySSTable objects to close/delete after releasing lock
        # Must collect the actual objects to properly close their mmap handles
        old_sstables_to_delete: List[LazySSTable] = []
        
        with self.lock:
            # Collect and remove old SSTables from source level
            if source_level in self.levels:
                old_sstables_to_delete.extend([
                    s for s in self.levels[source_level]
                    if s.sstable_id in source_ids
                ])
                self.levels[source_level] = [
                    s for s in self.levels[source_level]
                    if s.sstable_id not in source_ids
                ]
            
            # Collect and remove old SSTables from next level
            if next_level in self.levels:
                old_sstables_to_delete.extend([
                    s for s in self.levels[next_level]
                    if s.sstable_id in next_ids
                ])
                self.levels[next_level] = [
                    s for s in self.levels[next_level]
                    if s.sstable_id not in next_ids
                ]
            
            # Add new SSTable to next level (wrapped in LazySSTable)
            if new_sstable:
                if next_level not in self.levels:
                    self.levels[next_level] = []
                
                # Wrap in LazySSTable for consistent handling
                lazy_sstable = LazySSTable(
                    sstables_dir=self.sstables_dir,
                    sstable_id=new_sstable.sstable_id,
                    metadata=new_sstable.metadata
                )
                # Pre-set the loaded SSTable
                lazy_sstable._sstable = new_sstable
                lazy_sstable._loaded = True
                
                self.levels[next_level].append(lazy_sstable)
                
                # Update manifest for new SSTable
                self.level_manifest_manager.add_sstable(
                    dirname=new_sstable.dirname,
                    num_entries=new_sstable.metadata.num_entries if new_sstable.metadata else 0,
                    min_key=new_sstable.metadata.min_key if new_sstable.metadata else "",
                    max_key=new_sstable.metadata.max_key if new_sstable.metadata else "",
                    level=next_level,
                    sstable_id=new_sstable.sstable_id
                )
                
                print(f"[Compact-Worker] Created {new_sstable.dirname} at L{next_level}")
            
            # Trigger background manifest reload
            self._trigger_manifest_reload()
            
            # Clear manifests for old SSTables
            self.level_manifest_manager.remove_sstables(list(source_ids), level=source_level)
            if next_ids:
                self.level_manifest_manager.remove_sstables(list(next_ids), level=next_level)
        
        # Delete old SSTable files (outside lock, just file I/O)
        # Use the actual LazySSTable objects which have the open mmap handles
        # LazySSTable.delete() calls close() internally before removing files
        for old_sstable in old_sstables_to_delete:
            try:
                old_sstable.delete()
            except Exception as e:
                print(f"[Compact-Worker] Warning: Failed to delete {old_sstable.dirname}: {e}")
    
    def _trigger_manifest_reload(self):
        """
        Trigger a background reload of manifests.
        
        This ensures that after any manifest update, the manifest data
        is reloaded in the background. Old manifest data is preserved
        until the new data is ready (atomic swap).
        Uses lock to prevent TOCTOU race on Event flag (double submission).
        """
        with self._manifest_reload_lock:
            if not self._manifest_reload_pending.is_set():
                self._manifest_reload_pending.set()
                self._manifest_reload_executor.submit(self._background_manifest_reload)
    
    def _background_manifest_reload(self):
        """
        Background worker to reload manifest data.
        
        Process:
        1. Read new manifest data from disk
        2. Build new level-to-SSTable mapping
        3. Atomically swap the old mapping with new one
        4. Old SSTables preserved until new mapping is ready
        
        This ensures readers always see a consistent view.
        """
        try:
            # Clear the pending flag first
            self._manifest_reload_pending.clear()
            
            # Re-discover all levels (reads manifest files)
            self.level_manifest_manager.discover_levels()
            
            # Reload each level manifest
            for level in self.level_manifest_manager.get_levels():
                level_manifest = self.level_manifest_manager.get_level_manifest(level)
                # Force reload from disk
                level_manifest._load()
            
            # The in-memory levels dict is already up-to-date because
            # we update it atomically in add_sstable and _finalize_compaction
            # This reload just ensures the manifest files are in sync
            
        except Exception as e:
            print(f"[Manifest-Reload] Error during reload: {e}")
    
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
            
            # Record old SSTables before creating new one (crash-safe: create before delete)
            old_sstables_by_level = {
                level: list(sstables)
                for level, sstables in self.levels.items()
            }
            
            # Create new compacted SSTable first — only after it's persisted do we delete old
            metadata = self.add_sstable(compacted_entries, level=target_level, auto_compact=False)
            new_sstable_id = metadata.sstable_id
            
            # Delete old SSTables (keep the new one at target_level)
            for level, sstables in old_sstables_by_level.items():
                old_ids = [s.sstable_id for s in sstables]
                self.levels[level] = [s for s in self.levels[level] if s.sstable_id not in old_ids]
                if old_ids:
                    self.level_manifest_manager.remove_sstables(old_ids, level=level)
                for sstable in sstables:
                    if sstable.exists():
                        sstable.delete()
            
            print(f"[SSTableManager] Full compaction complete: {metadata.dirname} at L{target_level}")
            
            return metadata
    
    def remove_sstable(self, sstable_id: int):
        """
        Remove a specific SSTable from collection.
        
        Args:
            sstable_id: ID of SSTable to remove
        """
        with self.lock:
            # Find the level containing this SSTable
            target_level = None
            for level in self.levels:
                for sstable in self.levels[level]:
                    if sstable.sstable_id == sstable_id:
                        target_level = level
                        break
                if target_level is not None:
                    break
            
            # Remove from in-memory collection
            for level in self.levels:
                self.levels[level] = [s for s in self.levels[level] if s.sstable_id != sstable_id]
            
            # Remove from level manifest
            self.level_manifest_manager.remove_sstables([sstable_id], level=target_level)
    
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
    
    def wait_for_compaction(self, timeout: float = 30.0) -> bool:
        """
        Wait for any pending background compactions to complete.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if all compactions completed, False if timeout
        """
        start = time.time()
        while time.time() - start < timeout:
            with self._compaction_lock:
                if not self._compacting_sstable_ids:
                    return True
            time.sleep(0.1)
        return False
    
    def is_compacting(self) -> bool:
        """Check if any compaction is in progress."""
        with self._compaction_lock:
            return len(self._compacting_sstable_ids) > 0
    
    def shutdown(self, wait: bool = True, timeout: float = 30.0):
        """
        Shutdown the background thread pools.
        
        Args:
            wait: If True, wait for pending operations to complete
            timeout: Maximum time to wait if wait=True
        """
        if wait:
            self.wait_for_compaction(timeout)
        self.compaction_executor.shutdown(wait=wait)
        self._manifest_reload_executor.shutdown(wait=wait)
    
    def get_lazy_load_stats(self) -> dict:
        """
        Get statistics about lazy loading.
        
        Returns:
            Dictionary with lazy loading statistics
        """
        with self.lock:
            total_sstables = 0
            loaded_sstables = 0
            total_accesses = 0
            
            for level, sstables in self.levels.items():
                for sstable in sstables:
                    total_sstables += 1
                    if isinstance(sstable, LazySSTable):
                        if sstable.is_loaded():
                            loaded_sstables += 1
                        total_accesses += sstable.access_count
            
            return {
                "total_sstables": total_sstables,
                "loaded_sstables": loaded_sstables,
                "unloaded_sstables": total_sstables - loaded_sstables,
                "total_accesses": total_accesses,
                "memory_saved_pct": 100 * (total_sstables - loaded_sstables) / max(1, total_sstables)
            }