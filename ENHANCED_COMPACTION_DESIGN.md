# Enhanced Compaction & Manifest Design

## Requirements Summary

### 1. Soft Limits (85% threshold)
- Trigger compaction at 85% of hard limit
- Prevents hitting actual limits
- Better performance headroom

### 2. Non-Blocking Background Operations
- Background threads for flush/compact/merge
- Never block main thread during reads/writes
- Use snapshots for operations

### 3. Separate Manifest Per Level
- manifest_l0.json, manifest_l1.json, manifest_l2.json, etc.
- Better isolation and concurrency
- Smaller files to update

### 4. Snapshot-Based Compaction
- Background thread gets snapshot of SSTables
- Works on snapshot (doesn't block new writes)
- Only after new SSTable persisted:
  - Delete old SSTables
  - Update manifest
  - Atomic switchover

### 5. Lazy SSTable Loading
- On startup: Only load metadata from manifests
- Don't load actual SSTable objects until needed
- Reduces startup time and memory

### 6. Manifest Reloading
- After manifest update: Reload in background
- Keep old manifest data until new is ready
- Atomic switchover to new metadata
- No read disruption

## Design

### Manifest Structure

**Before (single manifest):**
```
data/
└── manifest.json  (all SSTables)
```

**After (per-level manifests):**
```
data/
└── manifests/
    ├── manifest_l0.json
    ├── manifest_l1.json
    ├── manifest_l2.json
    └── ...
```

### SSTableMetadata vs SSTable Object

**SSTableMetadata (lightweight):**
- sstable_id, dirname, num_entries, min_key, max_key, level
- ~100 bytes per SSTable
- Loaded on startup
- Kept in memory

**SSTable Object (heavyweight):**
- File handles, mmap, Bloom filter, sparse index
- ~1KB+ per SSTable (with loaded components)
- Loaded on-demand
- Cached with LRU eviction

### Compaction Flow with Snapshots

```
Main Thread:
  1. PUT operations continue
  2. Memtable flushes continue
  3. No blocking

Background Thread:
  1. Take snapshot of level SSTables
  2. Read entries from snapshot
  3. Deduplicate and merge
  4. Write new SSTable to disk
  5. Fsync to ensure persistence
  6. Atomically:
     a. Update manifest
     b. Delete old SSTables
     c. Update in-memory metadata
  7. Reload manifest (in background)
```

### Soft Limit Implementation

```python
# Hard limits
max_entries = base_entries × (ratio ^ level)
max_size = base_size × (ratio ^ level)

# Soft limits (85%)
soft_max_entries = max_entries × 0.85
soft_max_size = max_size × 0.85

# Trigger compaction at soft limit
if entries >= soft_max_entries or size >= soft_max_size:
    trigger_background_compaction()
```

## Implementation Plan

### Phase 1: Soft Limits
- Add `soft_limit_ratio` parameter (default: 0.85)
- Update `_should_compact_level()` to use soft limits
- Test with existing code

### Phase 2: Separate Manifests
- Create ManifestManager class
- One manifest file per level
- Update SSTableManager to use per-level manifests
- Migration from single manifest

### Phase 3: Lazy Loading
- Separate SSTableMetadata from SSTable
- Load only metadata on startup
- Cache SSTable objects (LRU)
- Load on first access

### Phase 4: Snapshot-Based Compaction
- Snapshot mechanism for compaction
- Background compaction thread
- Atomic updates after persistence
- Test non-blocking behavior

### Phase 5: Manifest Reloading
- Background manifest reload
- Atomic switchover
- Keep old until new ready
- Test concurrency

## Key Classes

### ManifestManager (NEW)
```python
class ManifestManager:
    def __init__(self, manifests_dir):
        self.manifests_dir = manifests_dir
        self.level_manifests = {}  # level → Manifest object
    
    def get_manifest(self, level) -> Manifest
    def save_metadata(self, level, metadata)
    def remove_metadata(self, level, sstable_id)
    def reload_level(self, level)  # Background reload
```

### SSTableCache (NEW)
```python
class SSTableCache:
    def __init__(self, max_size=100):
        self.cache = {}  # sstable_id → SSTable
        self.lru_queue = deque()
    
    def get(self, sstable_id, loader_fn) -> SSTable
    def evict_lru()
    def clear()
```

### Enhanced SSTableManager
```python
class SSTableManager:
    def __init__(..., soft_limit_ratio=0.85):
        self.metadata_by_level = {}  # level → List[SSTableMetadata]
        self.sstable_cache = SSTableCache()
        self.manifest_manager = ManifestManager()
        self.soft_limit_ratio = soft_limit_ratio
    
    def _get_sstable(self, sstable_id) -> SSTable:
        """Load SSTable on-demand with caching."""
        return self.sstable_cache.get(
            sstable_id,
            lambda: SSTable(self.sstables_dir, sstable_id)
        )
    
    def _compact_level_background(self, level):
        """Background compaction with snapshots."""
        # Snapshot metadata
        snapshot = self.metadata_by_level[level].copy()
        
        # Work on snapshot (doesn't block reads/writes)
        ...
        
        # After new SSTable persisted:
        with self.lock:
            # Atomic update
            self._update_level_after_compaction()
```

## Benefits

### 1. Non-Blocking Operations
- Main thread never waits for compaction
- Reads continue during compaction
- Writes continue during compaction

### 2. Better Performance
- Soft limits prevent hitting hard limits
- Lazy loading reduces memory usage
- LRU cache for hot SSTables

### 3. Scalability
- Separate manifests scale better
- Smaller files to update
- Better concurrency

### 4. Reliability
- Atomic updates after persistence
- Old data preserved until new ready
- No partial states visible

### 5. Memory Efficiency
- Don't load all SSTables on startup
- Only load what's needed
- LRU eviction for memory management

Ready to implement! 🚀
