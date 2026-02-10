# Feature Improvements — Production Readiness

These are enhancements needed to make lsm-kv a usable library for real systems.
Ordered by impact. Each can be done incrementally after all P0 bugs are fixed.

---

## FI-1: Replace `print()` With Python `logging` Module

### Priority: High (blocks all other work — noisy output confuses users)

### Files to Change
Every file that uses `print()`:
- `lsmkv/core/kvstore.py` — lines 81, 97, 289, 300 (4 print calls)
- `lsmkv/core/memtable_manager.py` — lines 182, 206, 227-228, 231, 255, 260 (6 print calls)
- `lsmkv/core/sstable_manager.py` — lines 127-136, 239-241, 306, 422, 429, 436, 457, 467,
  477, 552-553, 608-611, 632, 640, 647, 649, 659, 747, 764, 808, 853-854, 878, 887, 924, 930
  (~25 print calls)
- `lsmkv/storage/wal.py` — line 55
- `lsmkv/storage/level_manifest.py` — lines 70, 202, 299, 326, 329

### Current Problem
```python
# Example: sstable_manager.py:127-136 — prints 7 lines on EVERY initialization
print(f"[SSTableManager] Initialized with leveled compaction:")
print(f"  - Level ratio: {level_ratio}")
print(f"  - Soft limit: {int(soft_limit_ratio * 100)}% of hard limit")
...
```
Users importing the library see a wall of internal debug text. There's no way to silence,
redirect, or control verbosity.

### What to Do
1. Add a module-level logger in each file:
   ```python
   import logging
   logger = logging.getLogger(__name__)
   ```

2. Replace all `print()` calls with appropriate log levels:
   - Initialization info → `logger.info()`
   - Operational details (rotation, flush, compaction start/end) → `logger.debug()`
   - Warnings (corrupted records, failed deletes) → `logger.warning()`
   - Errors (compaction failures) → `logger.error()`

3. Remove `import traceback` / `traceback.print_exc()` calls (memtable_manager.py:232-233,
   sstable_manager.py:660-661) — replace with `logger.exception()` which logs the traceback
   automatically.

4. Do NOT configure any handler or formatter in the library code. Users configure logging in
   their application. The library should only create loggers and emit records.

### Behavior After Fix
```python
# Silent by default
store = LSMKVStore()  # no output

# User opts in to debug output
import logging
logging.basicConfig(level=logging.DEBUG)
store = LSMKVStore()  # shows debug messages
```

---

## FI-2: Add Context Manager Support (`with` Statement)

### Priority: High

### Files to Change
- `lsmkv/core/kvstore.py` — add `__enter__` and `__exit__` methods

### Current Problem
Users must remember to call `close()`. Forgetting leaves mmap handles and thread pools
open, leaking resources.

### What to Do
Add to `LSMKVStore`:
```python
def __enter__(self):
    return self

def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()
    return False  # don't suppress exceptions
```

Usage becomes:
```python
with LSMKVStore(data_dir="./mydb") as store:
    store.put("key", "value")
    result = store.get("key")
# auto-closed here, all data flushed
```

### Tests to Add
- `with` block normal exit → verify close() was called, data persisted
- `with` block exception exit → verify close() still called, resources cleaned up
- Nested `with` blocks with different data_dirs

---

## FI-3: Add Range Query / Scan API

### Priority: High (fundamental KV store operation)

### Files to Change
- `lsmkv/core/kvstore.py` — add `scan()` method
- `lsmkv/core/sstable_manager.py` — add `scan()` method
- `lsmkv/storage/sstable.py` — add `scan()` method to SSTable and LazySSTable
- `lsmkv/core/memtable_manager.py` — add `scan()` method
- `lsmkv/storage/memtable.py` — add `scan()` method

### Current State
The data structures already support range queries:
- Memtable uses `SkipListDict` — inherently sorted, supports iteration
- SSTables are sorted by key on disk
- Sparse index has `get_scan_range()` (sparse_index.py:179) for bounded reads
- Bloom filters can be skipped for scans (they only help point lookups)

But **no scan API is exposed** anywhere in the class hierarchy.

### What to Do

**Public API** (kvstore.py):
```python
def scan(self, start_key: str = None, end_key: str = None,
         limit: int = None) -> List[GetResult]:
    """
    Scan keys in sorted order within [start_key, end_key).

    Args:
        start_key: Inclusive lower bound (None = from beginning)
        end_key: Exclusive upper bound (None = to end)
        limit: Maximum number of results to return

    Returns:
        List of GetResult for live (non-deleted) keys in sorted order
    """
```

**Implementation strategy:**
1. Collect entries from active memtable (filter by key range)
2. Collect entries from each immutable memtable (filter by key range)
3. Collect entries from each SSTable level (use sparse index for bounded reads)
4. Merge all entries using a priority-based merge (newest timestamp wins per key)
5. Filter out tombstones
6. Apply limit

**Memtable scan** (memtable.py):
The SkipListDict supports iteration in sorted order. Iterate and filter:
```python
def scan(self, start_key=None, end_key=None):
    results = []
    for key, entry in self.skiplist.items():
        if start_key and key < start_key:
            continue
        if end_key and key >= end_key:
            break
        results.append(entry)
    return results
```

**SSTable scan** (sstable.py):
Use sparse index to find start offset, then mmap-read forward until end_key:
```python
def scan(self, start_key=None, end_key=None):
    start_offset = 0
    if start_key and self._sparse_index:
        start_offset = self._sparse_index.find_block_offset(start_key)
    # Read from start_offset, collect entries until key >= end_key
    ...
```

### Tests to Add
- `scan()` with no bounds → returns all entries sorted
- `scan(start_key="b", end_key="d")` → returns only keys in [b, d)
- `scan()` after deletes → tombstoned keys excluded
- `scan()` across memtable + SSTables → correctly merged
- `scan(limit=10)` → returns at most 10 entries
- `scan()` with concurrent writes → consistent snapshot

---

## FI-4: Add Batch Write / WriteBatch API

### Priority: High (10-100x write throughput improvement)

### Files to Change
- `lsmkv/core/kvstore.py` — add `WriteBatch` class and `write_batch()` method
- `lsmkv/storage/wal.py` — add `append_batch()` method
- `lsmkv/core/dto.py` — (optional) add batch WAL record type

### Current Problem
Every `put()` and `delete()` does a separate `os.fsync()` on the WAL (wal.py:38).
`fsync` is ~1-2ms on SSD, capping throughput at ~500-1000 ops/sec. Batching N writes
into a single fsync gives N× throughput.

### What to Do

**WriteBatch class:**
```python
class WriteBatch:
    """Collects multiple writes to apply atomically."""

    def __init__(self):
        self.operations = []  # List of (operation_type, key, value)

    def put(self, key: str, value: str):
        self.operations.append((OperationType.PUT, key, value))

    def delete(self, key: str):
        self.operations.append((OperationType.DELETE, key, None))

    def __len__(self):
        return len(self.operations)
```

**Apply method in LSMKVStore:**
```python
def write_batch(self, batch: WriteBatch) -> bool:
    """
    Apply a batch of writes atomically.
    Single WAL fsync for all operations.
    """
    timestamp = self._get_timestamp()

    # Write all records to WAL with single fsync
    self.wal.append_batch([
        WALRecord(op, key, value, timestamp)
        for op, key, value in batch.operations
    ])

    # Apply to memtable
    for op, key, value in batch.operations:
        entry = Entry(key=key, value=value, timestamp=timestamp,
                      is_deleted=(op == OperationType.DELETE))
        if op == OperationType.PUT:
            self.memtable_manager.put(entry)
        else:
            self.memtable_manager.delete(entry)

    return True
```

**WAL batch append** (wal.py):
```python
def append_batch(self, records: List[WALRecord]):
    """Write multiple records with a single fsync."""
    with self.lock:
        with open(self.filepath, 'a') as f:
            for record in records:
                f.write(record.serialize())
            f.flush()
            os.fsync(f.fileno())  # single fsync for all records
```

### Tests to Add
- Batch of 1000 puts → verify all retrievable
- Batch with mixed puts and deletes → verify correct state
- Batch atomicity: if process crashes mid-apply, either all or none should be recovered
- Performance: batch of 10K puts should be >> 10x faster than individual puts

---

## FI-5: Use Read-Write Lock for Concurrent Reads

### Priority: High (eliminates read serialization bottleneck)

### Files to Change
- `lsmkv/core/sstable_manager.py` — lines 98 (replace RLock with RWLock)

### Current Problem
`SSTableManager.get()` (line 328) acquires `self.lock` (an RLock) for the entire duration
of the read — which may include lazy SSTable loading, bloom filter checks, sparse index
lookups, and mmap reads. Since all reads and writes share the same RLock, reads are fully
serialized. Under read-heavy workloads, this is the primary bottleneck.

### What to Do
Replace the RLock with a read-write lock that allows concurrent readers:

```python
import threading

class RWLock:
    """Simple read-write lock. Multiple readers, exclusive writer."""
    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def read_acquire(self):
        with self._read_ready:
            self._readers += 1

    def read_release(self):
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def write_acquire(self):
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def write_release(self):
        self._read_ready.release()
```

Then in SSTableManager:
- `get()`, `get_all_entries()`, `stats()`, `get_level_info()` → use read lock
- `add_sstable()`, `_finalize_compaction()`, `_delete_level_sstables()` → use write lock

### Tests to Add
- 10 concurrent readers → verify no blocking (measure elapsed time)
- Reader + writer concurrent → writer waits for readers to finish
- Stress test: 50 readers + 5 writers → verify no data races

---

## FI-6: Add Data Integrity Checksums (CRC32)

### Priority: Medium

### Files to Change
- `lsmkv/core/dto.py` — add CRC to WAL record serialization
- `lsmkv/storage/wal.py` — verify CRC on read
- `lsmkv/storage/sstable.py` — add CRC to SSTable data file

### Current Problem
No checksums anywhere. Corrupted data (bit flips, partial writes, disk errors) is silently
read as valid or silently skipped. The WAL deserializer (dto.py:51-53) catches parse errors
but can't detect valid-looking but wrong data.

### What to Do

**WAL records:**
Append CRC32 to each serialized record:
```python
import zlib

def serialize(self) -> str:
    payload = json.dumps({"op": ..., "key": ..., "value": ..., "ts": ...})
    crc = zlib.crc32(payload.encode('utf-8')) & 0xFFFFFFFF
    return f"{crc:08x}:{payload}\n"

@staticmethod
def deserialize(line: str) -> 'WALRecord':
    crc_str, payload = line.strip().split(':', 1)
    expected_crc = int(crc_str, 16)
    actual_crc = zlib.crc32(payload.encode('utf-8')) & 0xFFFFFFFF
    if expected_crc != actual_crc:
        raise ValueError(f"CRC mismatch: expected {expected_crc}, got {actual_crc}")
    ...
```

**SSTable entries:**
Add a per-entry or per-block CRC in the data.db file. The simplest approach: append CRC
to each JSON line.

### Tests to Add
- Corrupt a single byte in WAL → verify detection on recovery
- Corrupt a single byte in SSTable → verify detection on read
- Valid records pass CRC check without errors

---

## FI-7: Add Iterator / Cursor API

### Priority: Medium

### Files to Change
- `lsmkv/core/kvstore.py` — add `__iter__`, `keys()`, `values()`, `items()` methods

### Current State
No way to iterate over all keys without loading everything into memory.

### What to Do
Add a merge-iterator that lazily produces entries in sorted order:

```python
def __iter__(self):
    """Iterate over all live key-value pairs in sorted order."""
    return self.items()

def keys(self):
    """Iterate over all keys in sorted order."""
    for key, value in self.items():
        yield key

def values(self):
    """Iterate over all values in sorted order."""
    for key, value in self.items():
        yield value

def items(self):
    """Iterate over all (key, value) pairs in sorted order."""
    # Collect all entries from all sources, merge, deduplicate, filter tombstones
    # Use a heap-based merge for memory efficiency
    ...
    for entry in merged:
        if not entry.is_deleted:
            yield entry.key, entry.value
```

For large datasets, implement a streaming merge using `heapq.merge()` instead of loading
all entries into memory.

### Tests to Add
- `list(store.keys())` matches expected sorted key set
- Iterator after deletes — deleted keys not yielded
- Iterator during concurrent writes — consistent snapshot
- `len(list(store))` matches expected count

---

## FI-8: Support `bytes` Keys and Values

### Priority: Medium

### Files to Change
- `lsmkv/core/dto.py` — `Entry`, `WALRecord`, `GetResult` types
- `lsmkv/core/kvstore.py` — `put()`, `get()`, `delete()` signatures
- `lsmkv/storage/sstable.py` — serialization format
- `lsmkv/storage/memtable.py` — comparisons need to work with bytes

### Current Problem
Only `str` keys/values supported. JSON serialization in SSTables and WAL prevents binary
data. Users storing binary data (images, protobuf, msgpack) must base64-encode, wasting
~33% space.

### What to Do
Accept `Union[str, bytes]` for keys and values. Internally, normalize to bytes.
Use a binary serialization format instead of JSON for SSTable entries.

This is a larger refactor — consider doing it alongside FI-10 (binary SSTable format).

---

## FI-9: Add `__contains__` / Key Existence Check

### Priority: Low (easy win)

### Files to Change
- `lsmkv/core/kvstore.py` — add `__contains__` and `exists()` methods

### What to Do
```python
def exists(self, key: str) -> bool:
    """Check if a key exists in the store."""
    return self.get(key).found

def __contains__(self, key: str) -> bool:
    """Support 'key in store' syntax."""
    return self.exists(key)

def __len__(self) -> int:
    """Return approximate number of live keys."""
    # This is expensive — requires scanning all SSTables
    # Consider maintaining a counter instead
    ...
```

Usage:
```python
if "user:123" in store:
    ...
```

### Tests to Add
- `"key" in store` after put → True
- `"key" in store` after delete → False
- `"nonexistent" in store` → False

---

## FI-10: Binary SSTable Format (Replace JSON Lines)

### Priority: Medium (significant performance + space improvement)

### Files to Change
- `lsmkv/storage/sstable.py` — `write()` and `read_all()` and `_read_bounded_region()`

### Current Problem
SSTable data.db uses JSON lines (sstable.py:148-154):
```python
entry_dict = {"key": entry.key, "value": entry.value, ...}
f.write(json.dumps(entry_dict) + '\n')
```
This is ~5-10x larger than binary and requires JSON parsing on every read. For a database
component that's read millions of times, this is a significant overhead.

### What to Do
Replace with a binary format:
```
[entry_count: 4 bytes]
[entry_1: key_len(4) + key + value_len(4) + value + timestamp(8) + is_deleted(1)]
[entry_2: ...]
...
```

Use `struct.pack` / `struct.unpack` for serialization. This is consistent with how the
sparse index already works (sparse_index.py:55-65).

### Impact
- ~5-10x smaller SSTable files
- ~5-10x faster reads (no JSON parsing)
- Enables binary keys/values (FI-8)
- Better mmap performance (less data to page in)

---

## FI-11: Group Commit for WAL

### Priority: Medium (major write throughput improvement)

### Files to Change
- `lsmkv/storage/wal.py` — add batching/group commit logic

### Current Problem
Every `put()` calls `os.fsync()` (wal.py:38), which takes ~1-2ms on SSD. This caps
single-thread write throughput at ~500-1000 ops/sec.

### What to Do
Buffer WAL writes and fsync periodically or after a batch threshold:
```python
class WAL:
    def __init__(self, filepath, sync_interval_ms=10, batch_size=100):
        self._buffer = []
        self._buffer_lock = threading.Lock()
        self._sync_interval = sync_interval_ms / 1000
        self._batch_size = batch_size
        self._file = open(filepath, 'a')
        # Background sync thread
        ...

    def append(self, record):
        with self._buffer_lock:
            self._file.write(record.serialize())
            self._buffer.append(record)
            if len(self._buffer) >= self._batch_size:
                self._sync()

    def _sync(self):
        self._file.flush()
        os.fsync(self._file.fileno())
        self._buffer.clear()
```

Trade-off: slightly increased data loss window (up to `sync_interval_ms` of writes) in
exchange for 10-100x throughput. Make this configurable — users who need strict durability
can set `sync_interval_ms=0`.

---

## FI-12: Orphaned SSTable Cleanup on Startup

### Priority: Low

### Files to Change
- `lsmkv/core/sstable_manager.py` — add cleanup in `load_from_manifest()`

### Current Problem
If a crash happens after writing SSTable files but before updating the manifest, orphaned
SSTable directories remain on disk forever. No mechanism to detect or clean them up.

### What to Do
During `load_from_manifest()`, scan the sstables directory for directories not referenced
by any manifest. Log them and optionally delete them:
```python
def _cleanup_orphaned_sstables(self):
    """Remove SSTable directories not referenced by any manifest."""
    manifest_dirnames = set()
    for level_entries in self.level_manifest_manager.get_all_entries():
        manifest_dirnames.add(level_entries.dirname)

    for dirname in os.listdir(self.sstables_dir):
        if dirname.startswith("sstable_") and dirname not in manifest_dirnames:
            logger.warning(f"Found orphaned SSTable: {dirname}, removing")
            shutil.rmtree(os.path.join(self.sstables_dir, dirname))
```

---

## FI-13: Metrics and Observability

### Priority: Low

### Files to Change
- New file: `lsmkv/metrics.py`
- `lsmkv/core/kvstore.py` — instrument put/get/delete
- `lsmkv/core/sstable_manager.py` — instrument compaction, bloom filter hits/misses

### What to Do
Track counters and histograms:
- `puts_total`, `gets_total`, `deletes_total`
- `get_latency_ms` (histogram)
- `put_latency_ms` (histogram)
- `bloom_filter_true_negatives` (how many reads were skipped)
- `bloom_filter_false_positives`
- `compaction_duration_ms`
- `sstable_reads_total` (per level)
- `memtable_hits` vs `sstable_hits`

Expose via a `metrics()` method that returns a dict. Optionally support Prometheus-style
export.

---

## FI-14: Migrate to `pyproject.toml`

### Priority: Low

### Files to Change
- Create `pyproject.toml`
- Remove `setup.py` (or keep as shim)
- Remove `MANIFEST.in` (handled by pyproject.toml)

### What to Do
```toml
[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "lsm-kv-store"
version = "1.2.0"
description = "LSM tree-based key-value store"
requires-python = ">=3.9"
dependencies = [
    "skiplistcollections>=0.0.6",
    "pybloomfiltermmap3>=0.5.0",
]

[project.scripts]
lsmkv = "scripts.cli:main"
```

---

## Implementation Order

| Phase | Items | Theme |
|-------|-------|-------|
| 1 | FI-1, FI-2 | Basics: logging + context manager |
| 2 | FI-4, FI-11 | Write performance: batch writes + group commit |
| 3 | FI-3, FI-7, FI-9 | Read API: scan, iterator, contains |
| 4 | FI-5 | Read performance: RW lock |
| 5 | FI-6 | Data safety: checksums |
| 6 | FI-10, FI-8 | Storage: binary format + bytes support |
| 7 | FI-12, FI-13, FI-14 | Polish: cleanup, metrics, packaging |
