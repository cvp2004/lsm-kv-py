# SSTable Refactoring Plan - Visual Summary

## Current State vs. Proposed State

### CURRENT: No SSTableManager ❌

```
┌───────────────────────────────────────────────────────────────┐
│                      LSMKVStore (441 lines)                   │
│                                                               │
│  ┌─────────────────────┐                                      │
│  │ MemtableManager     │  ✅ Well organized                   │
│  │ - Active memtable   │                                      │
│  │ - Immutable queue   │                                      │
│  │ - Thread pool       │                                      │
│  └─────────────────────┘                                      │
│                                                               │
│  SSTable Operations (scattered):  ❌ Needs organization       │
│  ├─ self.sstables: List[SSTable]                             │
│  ├─ self.sstable_lock: RLock                                 │
│  ├─ self.manifest: Manifest                                  │
│  │                                                            │
│  ├─ _load_existing_sstables()     (12 lines)                 │
│  ├─ get() - SSTable iteration     (10 lines)                 │
│  ├─ _flush_memtable_to_sstable()  (40 lines)  ← DUPLICATE    │
│  ├─ flush()                        (44 lines)  ← DUPLICATE    │
│  ├─ compact()                      (75 lines)  ← DUPLICATE    │
│  ├─ close() - SSTable cleanup      (3 lines)                 │
│  └─ stats() - SSTable stats        (4 lines)                 │
│                                                               │
│  Total SSTable code: ~200 lines, scattered across 8 methods  │
└───────────────────────────────────────────────────────────────┘
```

### PROPOSED: With SSTableManager ✅

```
┌───────────────────────────────────────────────────────────────┐
│                  LSMKVStore (~250 lines)                      │
│                                                               │
│  ┌─────────────────────┐    ┌──────────────────────┐         │
│  │ MemtableManager     │    │  SSTableManager      │  ← NEW! │
│  │                     │    │                      │         │
│  │ - Active memtable   │    │ - SSTables list      │         │
│  │ - Immutable queue   │    │ - Manifest           │         │
│  │ - Thread pool       │    │ - Lock               │         │
│  │                     │    │                      │         │
│  │ Methods:            │    │ Methods:             │         │
│  │ • put()             │    │ • add_sstable()      │         │
│  │ • get()             │    │ • get()              │         │
│  │ • delete()          │    │ • compact()          │         │
│  │ • stats()           │    │ • close()            │         │
│  │ • close()           │    │ • stats()            │         │
│  └─────────────────────┘    │ • get_all_entries()  │         │
│                             │ • load_from_manifest()│        │
│  Clean delegation:          └──────────────────────┘         │
│  • self.memtable_manager.get(key)                            │
│  • self.sstable_manager.get(key)   ← Simple!                 │
│  • self.sstable_manager.add_sstable(entries)                 │
│  • self.sstable_manager.compact()                            │
└───────────────────────────────────────────────────────────────┘
```

---

## Code Duplication Visualization

### Current: Same Logic in 3 Places

```
_flush_memtable_to_sstable():          flush():                    compact():
┌─────────────────────────┐    ┌─────────────────────┐    ┌──────────────────────┐
│ sstable_id = manifest   │    │ sstable_id = manifest│    │ ... dedup logic ...  │
│   .get_next_id()        │    │   .get_next_id()     │    │                      │
│                         │    │                      │    │ sstable_id = manifest│
│ sstable = SSTable(...)  │    │ sstable = SSTable(...)│   │   .get_next_id()     │
│                         │    │                      │    │                      │
│ metadata = sstable      │    │ metadata = sstable   │    │ sstable = SSTable(...) │
│   .write(entries)       │    │   .write(entries)    │    │                      │
│                         │    │                      │    │ metadata = sstable   │
│ manifest.add_sstable()  │    │ manifest.add_sstable()│   │   .write(entries)    │
│                         │    │                      │    │                      │
│ with lock:              │    │ with lock:           │    │ manifest.add_sstable()│
│   sstables.append()     │    │   sstables.append()  │    │                      │
└─────────────────────────┘    └─────────────────────┘    │ with lock:           │
                                                          │   sstables.append()  │
                                                          └──────────────────────┘
```

**15-line pattern repeated 3 times = 45 lines of duplicated code!**

### Proposed: One Method

```
SSTableManager.add_sstable(entries):
┌────────────────────────────────────┐
│ with self.lock:                    │
│   sstable_id = self.manifest       │
│     .get_next_id()                 │
│                                    │
│   sstable = SSTable(...)           │
│   metadata = sstable.write(entries)│
│                                    │
│   self.manifest.add_sstable(...)   │
│   self.sstables.append(sstable)    │
│                                    │
│   return metadata                  │
└────────────────────────────────────┘

Used by:
- _flush_memtable_to_sstable() → manager.add_sstable()
- flush()                       → manager.add_sstable()
- compact()                     → manager.add_sstable()
```

**15 lines once, called 3 times = DRY principle!**

---

## Method Call Comparison

### Current: Direct Access

```python
# In LSMKVStore.get()
with self.sstable_lock:                    # Expose locking
    for sstable in reversed(self.sstables): # Expose iteration
        entry = sstable.get(key)            # Expose search logic
        if entry:
            if entry.is_deleted:            # Expose tombstone logic
                return ...
            return ...
```

### Proposed: Clean Delegation

```python
# In LSMKVStore.get()
entry = self.sstable_manager.get(key)  # Encapsulated!
if entry:
    if entry.is_deleted:
        return GetResult(key=key, value=None, found=False)
    return GetResult(key=key, value=entry.value, found=True)
```

**Benefit:** Locking, iteration, search logic hidden in manager

---

## Responsibilities Matrix

### Current

| Responsibility | MemtableManager | LSMKVStore | Manifest | SSTable |
|----------------|-----------------|------------|----------|---------|
| Manage memtables | ✅ Yes | ❌ No | - | - |
| Manage SSTables | - | ❌ **Yes** | - | - |
| Coordinate | - | ✅ Yes | - | - |
| Persist metadata | - | ❌ **Yes** | ✅ Yes | - |
| Thread safety | ✅ Yes | ❌ **Yes** | ✅ Yes | - |

**Problem:** LSMKVStore has too many responsibilities!

### Proposed

| Responsibility | MemtableManager | **SSTableManager** | LSMKVStore | Manifest |
|----------------|-----------------|-------------------|------------|----------|
| Manage memtables | ✅ Yes | - | ❌ No | - |
| Manage SSTables | - | ✅ **Yes** | ❌ No | - |
| Coordinate | - | - | ✅ Yes | - |
| Persist metadata | - | - | ❌ No | ✅ Yes |
| Thread safety | ✅ Yes | ✅ **Yes** | ❌ No | ✅ Yes |

**Benefit:** Clear separation of concerns!

---

## Line Count Projection

### Current
```
LSMKVStore:             441 lines
  - Memtable logic:     ~150 lines
  - SSTable logic:      ~200 lines  ← To be extracted
  - WAL logic:          ~50 lines
  - Coordination:       ~41 lines
```

### Proposed
```
LSMKVStore:             ~250 lines (43% reduction!)
  - Memtable logic:     ~80 lines (delegated)
  - SSTable logic:      ~20 lines (delegated)  ✅
  - WAL logic:          ~50 lines
  - Coordination:       ~100 lines

SSTableManager:         ~200 lines (NEW)
  - SSTable operations
  - Manifest operations
  - Compaction logic
  - Thread safety
```

**Net result:** Better organized, easier to maintain!

---

## Summary

### How SSTables Are Currently Handled

**Location:** Directly in `LSMKVStore` class (lsmkv/core/kvstore.py)

**State:**
- `self.sstables: List[SSTable]` - List of all SSTables
- `self.sstable_lock: RLock` - Thread synchronization
- `self.manifest: Manifest` - Metadata persistence

**Operations (8 methods, ~200 lines):**
1. `_load_existing_sstables()` - Load from manifest
2. `get()` - Search SSTables (reversed iteration)
3. `_flush_memtable_to_sstable()` - Create SSTable (background)
4. `flush()` - Create SSTable (manual) **← DUPLICATE**
5. `compact()` - Merge all SSTables **← DUPLICATE**
6. `_clear_wal_for_flushed_data()` - WAL management
7. `close()` - Close all SSTables
8. `stats()` - Calculate statistics

**Key Issues:**
- ❌ 45 lines of duplicated code (SSTable creation appears 3x)
- ❌ No encapsulation (direct list/lock access)
- ❌ Mixed responsibilities (KVStore does everything)
- ❌ Hard to test independently
- ❌ Inconsistent (has MemtableManager but not SSTableManager)

**Recommendation:** Create SSTableManager to mirror MemtableManager pattern and solve all these issues.

See `SSTABLE_MANAGEMENT_ANALYSIS.md` for detailed analysis and proposed implementation.
