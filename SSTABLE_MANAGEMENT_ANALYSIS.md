# Current SSTable Management - Analysis Before Refactoring

## Current State: How SSTables Are Handled

Currently, SSTable operations are **scattered across multiple methods in `LSMKVStore`** class. There is no dedicated SSTable manager.

### Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    LSMKVStore                               │
│                                                             │
│  ┌──────────────────┐      ┌─────────────────────────┐     │
│  │ MemtableManager  │      │  SSTable Operations     │     │
│  │                  │      │  (scattered in KVStore) │     │
│  │ - Manages        │      │                         │     │
│  │   memtables      │      │ - self.sstables: List   │     │
│  │ - Background     │      │ - self.sstable_lock     │     │
│  │   flushing       │      │ - self.manifest         │     │
│  │ - Thread pool    │      │                         │     │
│  └──────────────────┘      └─────────────────────────┘     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### SSTable-Related State in LSMKVStore

Currently in `__init__()`:
```python
class LSMKVStore:
    def __init__(...):
        # SSTable-related state (scattered)
        self.sstables_dir = os.path.join(data_dir, "sstables")
        self.manifest = Manifest(f"{data_dir}/manifest.json")
        self.sstables: List[SSTable] = []
        self.sstable_lock = threading.RLock()
        
        # Create directory
        os.makedirs(self.sstables_dir, exist_ok=True)
        
        # Load SSTables
        self._load_existing_sstables()
```

**Problems:**
- ❌ SSTable state mixed with memtable state
- ❌ No clear separation of concerns
- ❌ Hard to test SSTable operations independently
- ❌ No encapsulation of SSTable logic

---

## Current SSTable Operations

### 1. **Loading SSTables** (in `_load_existing_sstables()`)

**Location:** Lines 65-76

```python
def _load_existing_sstables(self):
    """Load existing SSTables from manifest."""
    entries = self.manifest.get_all_entries()
    
    for entry in entries:
        sstable = SSTable(self.sstables_dir, entry.sstable_id)
        if sstable.exists():
            self.sstables.append(sstable)
    
    if self.sstables:
        print(f"Loaded {len(self.sstables)} existing SSTables from manifest")
```

**What it does:**
- Reads manifest file
- Creates SSTable objects for each entry
- Adds to `self.sstables` list
- Prints confirmation

**Issues:**
- Direct manipulation of `self.sstables`
- Direct interaction with `self.manifest`
- Loading logic in KVStore instead of dedicated manager

---

### 2. **Reading from SSTables** (in `get()`)

**Location:** Lines 152-161

```python
def get(self, key: str) -> GetResult:
    # ... check memtable manager first ...
    
    # 2. Check SSTables (newest to oldest)
    with self.sstable_lock:
        for sstable in reversed(self.sstables):
            entry = sstable.get(key)
            if entry:
                if entry.is_deleted:
                    return GetResult(key=key, value=None, found=False)
                return GetResult(key=key, value=entry.value, found=True)
    
    return GetResult(key=key, value=None, found=False)
```

**What it does:**
- Iterates through SSTables in reverse order (newest first)
- Calls `sstable.get()` on each
- Handles tombstones
- Returns first match found

**Issues:**
- Locking logic in KVStore
- Iteration logic exposed
- Tombstone handling in KVStore instead of SSTable layer
- No abstraction

---

### 3. **Creating SSTables** (in `_flush_memtable_to_sstable()`)

**Location:** Lines 196-235

```python
def _flush_memtable_to_sstable(self, memtable: Memtable):
    entries = memtable.get_all_entries()
    
    if not entries:
        return
    
    # Get next ID from manifest
    sstable_id = self.manifest.get_next_id()
    
    # Create SSTable
    sstable = SSTable(self.sstables_dir, sstable_id)
    
    # Write entries
    metadata = sstable.write(entries)
    
    # Add to manifest
    self.manifest.add_sstable(
        dirname=metadata.dirname,
        num_entries=metadata.num_entries,
        min_key=metadata.min_key,
        max_key=metadata.max_key,
        level=0,
        sstable_id=sstable_id
    )
    
    # Add to list (thread-safe)
    with self.sstable_lock:
        self.sstables.append(sstable)
    
    # Clear WAL
    self._clear_wal_for_flushed_data(entries)
```

**What it does:**
- Gets next SSTable ID from manifest
- Creates new SSTable
- Writes entries
- Updates manifest
- Adds to in-memory list
- Clears WAL

**Issues:**
- SSTable creation logic in KVStore
- Direct manifest manipulation
- WAL clearing mixed with SSTable operations
- No abstraction layer

---

### 4. **Manual Flush** (in `flush()`)

**Location:** Lines 263-306

```python
def flush(self) -> SSTableMetadata:
    # Check if memtable is empty
    active_size = len(self.memtable_manager.active)
    if active_size == 0:
        raise ValueError("Cannot flush empty memtable")
    
    # Get entries
    entries = self.memtable_manager.active.get_all_entries()
    
    # Create SSTable (same logic as background flush)
    sstable_id = self.manifest.get_next_id()
    sstable = SSTable(self.sstables_dir, sstable_id)
    metadata = sstable.write(entries)
    
    # Add to manifest
    self.manifest.add_sstable(...)
    
    # Add to list
    with self.sstable_lock:
        self.sstables.append(sstable)
    
    # Clear memtable and WAL
    self.memtable_manager.active.clear()
    self.wal.clear()
    
    return metadata
```

**What it does:**
- Similar to `_flush_memtable_to_sstable()` but for active memtable
- Duplicated SSTable creation logic
- Clears memtable and WAL

**Issues:**
- **Code duplication** with `_flush_memtable_to_sstable()`
- SSTable operations in KVStore
- No reusable abstraction

---

### 5. **Compaction** (in `compact()`)

**Location:** Lines 308-383

```python
def compact(self) -> SSTableMetadata:
    with self.sstable_lock:
        if len(self.sstables) == 0:
            raise ValueError("No SSTables to compact")
        
        # Collect all entries from all SSTables
        all_entries = []
        for sstable in self.sstables:
            all_entries.extend(sstable.read_all())
        
        # Deduplicate (keep latest version per key)
        key_map = {}
        for entry in all_entries:
            if entry.key not in key_map:
                key_map[entry.key] = entry
            else:
                if entry.timestamp > key_map[entry.key].timestamp:
                    key_map[entry.key] = entry
        
        # Remove tombstones
        compacted_entries = [
            entry for entry in key_map.values()
            if not entry.is_deleted
        ]
        
        # Sort entries
        compacted_entries.sort(key=lambda e: e.key)
        
        # Get old SSTable IDs
        old_entries = self.manifest.get_all_entries()
        old_ids = [entry.sstable_id for entry in old_entries]
        
        # Delete old SSTables
        for sstable in self.sstables:
            if sstable.exists():
                sstable.delete()
        
        # Remove from manifest
        self.manifest.remove_sstables(old_ids)
        
        # Clear list
        self.sstables.clear()
        
        # Create new compacted SSTable
        sstable_id = self.manifest.get_next_id()
        sstable = SSTable(self.sstables_dir, sstable_id)
        metadata = sstable.write(compacted_entries)
        
        # Add to manifest
        self.manifest.add_sstable(...)
        
        # Add to list
        self.sstables.append(sstable)
        
        return metadata
```

**What it does:**
- Reads all SSTables
- Deduplicates by timestamp
- Removes tombstones
- Deletes old SSTables
- Creates new compacted SSTable
- Updates manifest

**Issues:**
- **Complex logic** directly in KVStore
- Direct list manipulation (`self.sstables`)
- Direct manifest manipulation
- Hard to test compaction independently
- No abstraction

---

### 6. **Closing SSTables** (in `close()`)

**Location:** Lines 389-402

```python
def close(self):
    print("Closing KV store...")
    
    # Shutdown memtable manager
    self.memtable_manager.close()
    
    # Close all SSTables (cleanup mmap)
    with self.sstable_lock:
        for sstable in self.sstables:
            sstable.close()
    
    # Wait a bit for any pending flushes
    time.sleep(0.2)
```

**What it does:**
- Closes memtable manager
- Iterates through all SSTables and closes them
- Waits for pending operations

**Issues:**
- Direct iteration over `self.sstables`
- Mixed with memtable closing logic

---

### 7. **Statistics** (in `stats()`)

**Location:** Lines 404-435

```python
def stats(self) -> dict:
    # Get memtable manager stats
    manager_stats = self.memtable_manager.stats()
    
    # Calculate SSTable stats
    with self.sstable_lock:
        num_sstables = len(self.sstables)
        total_sstable_size = sum(s.size_bytes() for s in self.sstables)
    
    return {
        # Memtable stats...
        # SSTable stats
        "num_sstables": num_sstables,
        "total_sstable_size_bytes": total_sstable_size,
        # Performance stats...
    }
```

**What it does:**
- Counts SSTables
- Calculates total size
- Returns stats dict

**Issues:**
- Direct access to `self.sstables`
- SSTable stats calculation in KVStore

---

## Problems with Current Approach

### 1. **Lack of Separation of Concerns**
```
LSMKVStore currently handles:
├─ WAL operations
├─ Memtable management (via MemtableManager) ✓ Good
├─ SSTable operations (scattered) ✗ Bad
└─ Manifest operations
```

**Issue:** MemtableManager exists, but no SSTableManager equivalent

### 2. **Code Duplication**
- SSTable creation logic appears in:
  - `_flush_memtable_to_sstable()` (background flush)
  - `flush()` (manual flush)
- Both do essentially the same thing:
  ```python
  sstable_id = self.manifest.get_next_id()
  sstable = SSTable(self.sstables_dir, sstable_id)
  metadata = sstable.write(entries)
  self.manifest.add_sstable(...)
  self.sstables.append(sstable)
  ```

### 3. **Direct State Manipulation**
```python
# Throughout the code:
with self.sstable_lock:
    self.sstables.append(sstable)      # Direct manipulation
    self.sstables.clear()              # Direct manipulation
    for sstable in self.sstables:      # Direct iteration
```

**Issue:** No encapsulation, hard to maintain

### 4. **Complex Methods**
- `compact()` is 75 lines with complex logic
- `get()` has SSTable iteration logic embedded
- Hard to test individual pieces

### 5. **Tight Coupling**
```python
# LSMKVStore directly depends on:
- SSTable (storage layer)
- Manifest (storage layer)
- threading.RLock (synchronization)
- List management
```

**Issue:** High coupling, low cohesion

### 6. **No Abstraction for Common Operations**

Common operations not abstracted:
- ❌ Add SSTable to collection
- ❌ Remove SSTable from collection
- ❌ Search SSTables for key
- ❌ Get all entries from all SSTables
- ❌ Calculate statistics

---

## Proposed Solution: SSTableManager Class

### Design Goals

1. **Encapsulation**: Hide SSTable collection and operations
2. **Single Responsibility**: Manage SSTables only
3. **Reusability**: Common operations in one place
4. **Testability**: Easy to test independently
5. **Consistency**: Mirror MemtableManager design

### Proposed Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    LSMKVStore                               │
│                                                             │
│  ┌──────────────────┐      ┌─────────────────────────┐     │
│  │ MemtableManager  │      │   SSTableManager        │     │
│  │                  │      │                         │     │
│  │ - Active         │      │ - SSTables list         │     │
│  │ - Immutable      │      │ - Manifest              │     │
│  │ - Thread pool    │      │ - Lock                  │     │
│  │ - Flush callback │      │ - Operations            │     │
│  └──────────────────┘      └─────────────────────────┘     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### SSTableManager Responsibilities

**State Management:**
- Maintain list of SSTables
- Handle manifest operations
- Manage synchronization (lock)

**Operations:**
1. **add_sstable(entries)** → Create and add new SSTable
2. **get(key)** → Search SSTables for key
3. **get_all_entries()** → Collect all entries from all SSTables
4. **compact()** → Compact all SSTables into one
5. **close()** → Close all SSTables
6. **stats()** → Calculate SSTable statistics
7. **load_from_manifest()** → Load SSTables on startup

**Benefits:**
- ✅ Clean API
- ✅ Single responsibility
- ✅ Reusable operations
- ✅ Easy to test
- ✅ Reduced coupling

---

## Detailed Comparison

### Current: SSTable Operations Scattered

```python
class LSMKVStore:
    def __init__(...):
        self.sstables: List[SSTable] = []
        self.sstable_lock = threading.RLock()
        self.manifest = Manifest(...)
        self._load_existing_sstables()
    
    def _load_existing_sstables(self):
        # Load logic here
        ...
    
    def get(self, key: str):
        # ... check memtables ...
        with self.sstable_lock:
            for sstable in reversed(self.sstables):
                # Search logic here
                ...
    
    def _flush_memtable_to_sstable(self, memtable):
        # Create SSTable
        sstable_id = self.manifest.get_next_id()
        sstable = SSTable(...)
        metadata = sstable.write(entries)
        self.manifest.add_sstable(...)
        with self.sstable_lock:
            self.sstables.append(sstable)
        ...
    
    def flush(self):
        # DUPLICATE SSTable creation logic
        sstable_id = self.manifest.get_next_id()
        sstable = SSTable(...)
        # ... same as above ...
    
    def compact(self):
        with self.sstable_lock:
            # 75 lines of compaction logic
            ...
    
    def close(self):
        with self.sstable_lock:
            for sstable in self.sstables:
                sstable.close()
    
    def stats(self):
        with self.sstable_lock:
            num_sstables = len(self.sstables)
            total_size = sum(s.size_bytes() for s in self.sstables)
        ...
```

**Lines of SSTable-related code in LSMKVStore:** ~200 lines

---

### Proposed: SSTableManager

```python
class SSTableManager:
    def __init__(self, sstables_dir: str, manifest_path: str):
        self.sstables_dir = sstables_dir
        self.manifest = Manifest(manifest_path)
        self.sstables: List[SSTable] = []
        self.lock = threading.RLock()
        self.load_from_manifest()
    
    def load_from_manifest(self):
        """Load existing SSTables."""
        entries = self.manifest.get_all_entries()
        for entry in entries:
            sstable = SSTable(self.sstables_dir, entry.sstable_id)
            if sstable.exists():
                self.sstables.append(sstable)
    
    def add_sstable(self, entries: List[Entry]) -> SSTableMetadata:
        """Create and add new SSTable."""
        with self.lock:
            sstable_id = self.manifest.get_next_id()
            sstable = SSTable(self.sstables_dir, sstable_id)
            metadata = sstable.write(entries)
            
            self.manifest.add_sstable(
                dirname=metadata.dirname,
                num_entries=metadata.num_entries,
                min_key=metadata.min_key,
                max_key=metadata.max_key,
                level=0,
                sstable_id=sstable_id
            )
            
            self.sstables.append(sstable)
            return metadata
    
    def get(self, key: str) -> Optional[Entry]:
        """Search SSTables for key (newest to oldest)."""
        with self.lock:
            for sstable in reversed(self.sstables):
                entry = sstable.get(key)
                if entry:
                    return entry
        return None
    
    def get_all_entries(self) -> List[Entry]:
        """Get all entries from all SSTables."""
        with self.lock:
            all_entries = []
            for sstable in self.sstables:
                all_entries.extend(sstable.read_all())
            return all_entries
    
    def compact(self) -> SSTableMetadata:
        """Compact all SSTables into one."""
        with self.lock:
            if len(self.sstables) == 0:
                raise ValueError("No SSTables to compact")
            
            # Get all entries
            all_entries = self.get_all_entries()
            
            # Deduplicate
            key_map = {}
            for entry in all_entries:
                if entry.key not in key_map or entry.timestamp > key_map[entry.key].timestamp:
                    key_map[entry.key] = entry
            
            # Remove tombstones and sort
            compacted_entries = sorted(
                [e for e in key_map.values() if not e.is_deleted],
                key=lambda e: e.key
            )
            
            if not compacted_entries:
                raise ValueError("No live entries after compaction")
            
            # Delete old SSTables
            old_ids = [e.sstable_id for e in self.manifest.get_all_entries()]
            for sstable in self.sstables:
                sstable.delete()
            self.manifest.remove_sstables(old_ids)
            self.sstables.clear()
            
            # Create new compacted SSTable
            return self.add_sstable(compacted_entries)
    
    def close(self):
        """Close all SSTables."""
        with self.lock:
            for sstable in self.sstables:
                sstable.close()
    
    def stats(self) -> dict:
        """Calculate SSTable statistics."""
        with self.lock:
            return {
                "num_sstables": len(self.sstables),
                "total_size_bytes": sum(s.size_bytes() for s in self.sstables),
            }


class LSMKVStore:
    def __init__(...):
        # Much cleaner!
        self.wal = WAL(...)
        self.memtable_manager = MemtableManager(...)
        self.sstable_manager = SSTableManager(
            sstables_dir=self.sstables_dir,
            manifest_path=f"{data_dir}/manifest.json"
        )
    
    def get(self, key: str):
        # Check memtables
        entry = self.memtable_manager.get(key)
        if entry:
            return GetResult(...)
        
        # Check SSTables (clean delegation!)
        entry = self.sstable_manager.get(key)
        if entry:
            if entry.is_deleted:
                return GetResult(key=key, value=None, found=False)
            return GetResult(key=key, value=entry.value, found=True)
        
        return GetResult(key=key, value=None, found=False)
    
    def _flush_memtable_to_sstable(self, memtable):
        entries = memtable.get_all_entries()
        if entries:
            # Delegate to SSTableManager!
            self.sstable_manager.add_sstable(entries)
            self._clear_wal_for_flushed_data(entries)
    
    def flush(self):
        entries = self.memtable_manager.active.get_all_entries()
        # Delegate to SSTableManager!
        metadata = self.sstable_manager.add_sstable(entries)
        self.memtable_manager.active.clear()
        self.wal.clear()
        return metadata
    
    def compact(self):
        # Delegate to SSTableManager!
        return self.sstable_manager.compact()
    
    def close(self):
        self.memtable_manager.close()
        self.sstable_manager.close()  # Clean!
```

**Lines of SSTable-related code in LSMKVStore:** ~30 lines (85% reduction!)

---

## Benefits of SSTableManager

### 1. **Separation of Concerns**
| Component | Responsibility |
|-----------|----------------|
| MemtableManager | Manage memtables, rotations, background flushing |
| **SSTableManager** | Manage SSTables, manifest, compaction |
| LSMKVStore | Coordinate components, handle WAL |

### 2. **Code Reduction**
- LSMKVStore: 200+ lines → ~30 lines of SSTable code
- Eliminated duplication between `flush()` and `_flush_memtable_to_sstable()`

### 3. **Better Encapsulation**
```python
# Before:
with self.sstable_lock:
    self.sstables.append(sstable)
    self.manifest.add_sstable(...)

# After:
self.sstable_manager.add_sstable(entries)  # Encapsulated!
```

### 4. **Easier Testing**
```python
# Test SSTable operations independently
manager = SSTableManager("/tmp/sstables", "/tmp/manifest.json")
manager.add_sstable([entry1, entry2, entry3])
result = manager.get("key1")
manager.compact()
```

### 5. **Consistency**
- Mirrors MemtableManager design
- Similar API patterns
- Consistent error handling

### 6. **Thread Safety**
- All locking logic encapsulated in SSTableManager
- No need for `self.sstable_lock` in LSMKVStore

---

## Summary of Current Issues

| Issue | Impact | Solution |
|-------|--------|----------|
| Scattered SSTable logic | Hard to maintain | SSTableManager encapsulation |
| Code duplication | Error-prone | Single `add_sstable()` method |
| Direct state manipulation | Breaks encapsulation | Private state in manager |
| Complex methods | Hard to test | Smaller focused methods |
| Tight coupling | Hard to change | Clean interfaces |
| No abstraction | Code repetition | Reusable manager class |

---

## Implementation Plan

### Step 1: Create SSTableManager Class
- File: `lsmkv/core/sstable_manager.py`
- ~200 lines (extracted from LSMKVStore)

### Step 2: Refactor LSMKVStore
- Replace direct SSTable operations with manager calls
- Remove `self.sstables`, `self.sstable_lock`
- Simplify `get()`, `flush()`, `compact()`

### Step 3: Update Tests
- Test SSTableManager independently
- Verify LSMKVStore still works

### Step 4: Update Documentation
- Document SSTableManager API
- Update architecture diagrams

---

## Expected Outcome

**Before:**
- LSMKVStore: 441 lines
- SSTable logic: Scattered across 7+ methods
- Code duplication: 2 places creating SSTables
- Direct state access: Throughout

**After:**
- LSMKVStore: ~250 lines (43% reduction!)
- SSTableManager: ~200 lines (new, focused)
- SSTable logic: Centralized in manager
- Code duplication: Eliminated
- Clean delegation: `self.sstable_manager.add_sstable()`

**Result:**
- ✅ Better separation of concerns
- ✅ Easier to maintain
- ✅ Easier to test
- ✅ More consistent with MemtableManager
- ✅ Cleaner LSMKVStore code

---

## Should We Proceed?

**Recommendation:** **YES**, implement SSTableManager

**Reasons:**
1. Mirrors successful MemtableManager pattern
2. Reduces complexity in LSMKVStore
3. Eliminates code duplication
4. Improves testability
5. Better encapsulation
6. More maintainable long-term

**Next Steps:**
1. Review this analysis
2. Approve design
3. Implement SSTableManager
4. Refactor LSMKVStore
5. Update tests and documentation
