# How SSTables Are Currently Handled - Quick Reference

## Current State (No SSTableManager)

### Where SSTable Code Lives

All SSTable operations are **embedded directly in `LSMKVStore`** class:

```
lsmkv/core/kvstore.py (441 lines)
│
├─ State (lines 37-59)
│  ├─ self.sstables_dir
│  ├─ self.manifest
│  ├─ self.sstables (List[SSTable])
│  └─ self.sstable_lock
│
├─ Loading (lines 65-76)
│  └─ _load_existing_sstables()
│
├─ Reading (lines 152-161)
│  └─ get() - iterates SSTables
│
├─ Creating - Background (lines 196-235)
│  └─ _flush_memtable_to_sstable()
│
├─ Creating - Manual (lines 263-306)
│  └─ flush() - DUPLICATES above logic
│
├─ Compacting (lines 308-383)
│  └─ compact() - 75 lines of complex logic
│
├─ Closing (lines 396-399)
│  └─ close() - iterates and closes
│
└─ Stats (lines 411-414)
   └─ stats() - calculates metrics
```

**Total SSTable code in LSMKVStore:** ~200 lines across 8 methods

---

## Current Operations Flow

### 1. Initialization
```
LSMKVStore.__init__()
  ↓
Create self.sstables_dir
Create self.manifest
Create self.sstables = []
Create self.sstable_lock
  ↓
_load_existing_sstables()
  ↓
Read manifest → Create SSTable objects → Add to self.sstables
```

### 2. Reading (GET operation)
```
LSMKVStore.get(key)
  ↓
Check memtable_manager.get(key)
  ↓ (if not found)
with self.sstable_lock:
    for sstable in reversed(self.sstables):
        entry = sstable.get(key)
        if entry: return it
  ↓
Return not found
```

### 3. Background Flush (MemtableManager callback)
```
MemtableManager triggers flush
  ↓
LSMKVStore._flush_memtable_to_sstable(memtable)
  ↓
Get entries from memtable
Get next ID from self.manifest.get_next_id()
Create SSTable(self.sstables_dir, sstable_id)
Write entries → creates Bloom filter + sparse index
Add to self.manifest
  ↓
with self.sstable_lock:
    self.sstables.append(sstable)
  ↓
Clear WAL entries
```

### 4. Manual Flush
```
LSMKVStore.flush()
  ↓
Get entries from active memtable
Get next ID from self.manifest.get_next_id()  ← DUPLICATE
Create SSTable(self.sstables_dir, sstable_id)  ← DUPLICATE
Write entries                                   ← DUPLICATE
Add to self.manifest                            ← DUPLICATE
  ↓
with self.sstable_lock:
    self.sstables.append(sstable)                ← DUPLICATE
  ↓
Clear active memtable and WAL
```

**Problem:** flush() duplicates _flush_memtable_to_sstable() logic!

### 5. Compaction
```
LSMKVStore.compact()
  ↓
with self.sstable_lock:
    Check if SSTables exist
    
    for sstable in self.sstables:
        all_entries.extend(sstable.read_all())
    
    Deduplicate by timestamp
    Remove tombstones
    Sort entries
    
    Get old IDs from manifest
    
    for sstable in self.sstables:
        sstable.delete()
    
    self.manifest.remove_sstables(old_ids)
    self.sstables.clear()
    
    Create new SSTable                   ← DUPLICATE creation logic
    Add to manifest                      ← DUPLICATE
    self.sstables.append(sstable)       ← DUPLICATE
```

**Problem:** Compaction also duplicates SSTable creation logic!

### 6. Closing
```
LSMKVStore.close()
  ↓
Close memtable_manager
  ↓
with self.sstable_lock:
    for sstable in self.sstables:
        sstable.close()
```

### 7. Statistics
```
LSMKVStore.stats()
  ↓
Get memtable_manager.stats()
  ↓
with self.sstable_lock:
    num = len(self.sstables)
    size = sum(s.size_bytes() for s in self.sstables)
  ↓
Return combined stats
```

---

## Code Duplication Analysis

### Duplicated Pattern (appears 3 times!)

```python
# Pattern appears in:
# 1. _flush_memtable_to_sstable()
# 2. flush()
# 3. compact()

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

with self.sstable_lock:
    self.sstables.append(sstable)
```

**This same 15-line block appears 3 times!**

---

## Direct State Access (Anti-Pattern)

Throughout LSMKVStore, code directly accesses:

```python
self.sstables         # 12 occurrences
self.sstable_lock     # 8 occurrences
self.manifest         # 10 occurrences
```

**Issues:**
- No encapsulation
- Can't change implementation easily
- Hard to add features (like SSTable caching, lazy loading, etc.)
- Testing requires mocking internals

---

## Why This Matters

### Current Pain Points

**1. Adding a new feature (e.g., SSTable caching):**
```python
# Would need to modify:
- __init__() - add cache
- _load_existing_sstables() - populate cache
- get() - check cache
- _flush_memtable_to_sstable() - update cache
- flush() - update cache
- compact() - invalidate cache
- close() - clear cache
- stats() - include cache stats
```
**Result:** Changes scattered across 8+ methods!

**2. Testing compaction:**
```python
# Currently need to:
- Create full LSMKVStore
- Set up WAL
- Set up MemtableManager
- Create test SSTables
- Run compact()
```
**Result:** Complex test setup!

**3. Code review:**
- Hard to find all SSTable-related code
- Mixed with WAL and memtable logic
- Have to read entire LSMKVStore class

---

## Comparison with MemtableManager

### MemtableManager (Good Example)

```python
class MemtableManager:
    # Encapsulates:
    - active memtable
    - immutable queue
    - thread pool
    - all memtable operations
    
    # Clean API:
    - put(entry)
    - get(key)
    - delete(entry)
    - stats()
    - close()
```

**Result:** LSMKVStore just calls `self.memtable_manager.method()`

### Current SSTable Handling (Needs Improvement)

```python
# No SSTableManager, instead:
class LSMKVStore:
    # Directly manages:
    - self.sstables list
    - self.sstable_lock
    - self.manifest
    - all SSTable operations (scattered)
    
    # No clean API
    # No encapsulation
```

**Result:** LSMKVStore is bloated with SSTable logic

---

## The Need for SSTableManager

### Current Problems

❌ **Code duplication** (3 copies of SSTable creation)
❌ **No encapsulation** (direct access to self.sstables)
❌ **Mixed responsibilities** (KVStore does too much)
❌ **Hard to test** (need full store for SSTable tests)
❌ **Hard to extend** (changes scattered across many methods)
❌ **Inconsistent** (has MemtableManager but not SSTableManager)

### With SSTableManager

✅ **No duplication** (one add_sstable() method)
✅ **Clean encapsulation** (private state)
✅ **Single responsibility** (manages SSTables only)
✅ **Easy to test** (test manager independently)
✅ **Easy to extend** (changes in one place)
✅ **Consistent** (mirrors MemtableManager pattern)

---

## Conclusion

**Current handling:** SSTable operations are scattered across LSMKVStore with:
- 200+ lines of SSTable code
- Code duplication (3x)
- No encapsulation
- Mixed responsibilities
- Hard to maintain and test

**Solution:** Create SSTableManager to:
- Centralize SSTable operations
- Eliminate duplication
- Encapsulate state
- Mirror MemtableManager pattern
- Reduce LSMKVStore complexity

**Recommendation:** Proceed with SSTableManager implementation.

The analysis is complete. Ready to implement when you approve! 🚀
