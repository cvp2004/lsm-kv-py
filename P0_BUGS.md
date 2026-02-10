# P0 Bugs — Critical Fixes Before Any User Usage

Every issue in this document is a **data loss, data corruption, or install-breaking** bug.
Fix these before exposing the library to any user.

---

## P0-1: WAL Serialization Breaks on `|` or `\n` in Keys/Values

### Files
- `lsmkv/core/dto.py` — lines 44-60

### Current Code
```python
# dto.py:46 — serialize
def serialize(self) -> str:
    return f"{self.operation.value}|{self.key}|{self.value or ''}|{self.timestamp}\n"

# dto.py:51-53 — deserialize
parts = line.strip().split('|')
if len(parts) != 4:
    raise ValueError(f"Invalid WAL record format: {line}")
```

### The Bug
The serializer uses `|` as field delimiter and `\n` as record delimiter. Neither keys nor
values are escaped. If a user does:
```python
store.put("user|123", "some value")
```
The WAL line becomes: `PUT|user|123|some value|1700000000\n`
On deserialize, `split('|')` produces 5 parts, not 4 → `ValueError` → record is **silently
skipped** (wal.py:54-55 catches ValueError and prints a warning).

Similarly, a value containing `\n` splits into two lines, both of which fail to parse.

### Impact
- **Scope:** Any single key or value containing `|` or `\n` — common in real-world data
  (URLs, JSON strings, file paths, multi-line text, CSV data, etc.)
- **What happens:** The WAL record for that key is written incorrectly. On next startup,
  `_recover_from_wal()` calls `WALRecord.deserialize()`, which raises `ValueError`. The
  `wal.py:54-55` catch block prints a warning and **skips the record**. The entry is
  permanently lost — it's not in any SSTable and is now gone from the WAL too.
- **User observes:** After a restart, `get("user|123")` returns `found=False` even though
  `put("user|123", ...)` succeeded in the previous session. No exception, no error — just
  missing data.
- **Likelihood:** HIGH — any user storing structured data (URLs, JSON, paths) will hit this
  almost immediately. The `|` character is extremely common.
- **Blast radius:** Per-key. Each affected key is independently lost. If a user stores 1000
  keys and 50 contain `|`, those 50 are lost silently on every restart cycle.

### Reproduction
```python
store = LSMKVStore(data_dir="/tmp/test_wal_bug")
store.put("user|123", "value with|pipe")  # writes corrupt WAL
store.close()

store2 = LSMKVStore(data_dir="/tmp/test_wal_bug")  # recovery skips the record
result = store2.get("user|123")
assert result.found  # FAILS — data is gone
```

### Fix
Replace the `|`-delimited format with JSON serialization:
```python
def serialize(self) -> str:
    record = {
        "op": self.operation.value,
        "key": self.key,
        "value": self.value,
        "ts": self.timestamp
    }
    return json.dumps(record, separators=(',', ':')) + '\n'

@staticmethod
def deserialize(line: str) -> 'WALRecord':
    record = json.loads(line.strip())
    return WALRecord(
        operation=OperationType(record["op"]),
        key=record["key"],
        value=record["value"],
        timestamp=record["ts"]
    )
```
JSON handles all special characters safely. The `separators` kwarg keeps it compact.

### Tests to Add
- Put/get keys containing `|`, `\n`, `\t`, `\\`, `"`, unicode, empty string
- Restart recovery round-trip with special characters
- Deserialize a corrupted line and verify graceful error handling

---

## P0-2: WAL Has Zero Thread Safety

### Files
- `lsmkv/storage/wal.py` — entire class (lines 9-66)
- `lsmkv/core/kvstore.py` — lines 119, 186 (main thread writes) and lines 220-244 (background flush worker reads/clears/rewrites)

### Current Code
```python
# wal.py — no lock anywhere
class WAL:
    def append(self, record: WALRecord):    # called from main thread
        with open(self.filepath, 'a') as f:
            f.write(record.serialize())
            f.flush()
            os.fsync(f.fileno())

    def read_all(self) -> List[WALRecord]:  # called from flush worker
        ...

    def clear(self):                         # called from flush worker
        open(self.filepath, 'w').close()
```

### The Bug
Two threads access the WAL file concurrently:

**Thread A** (main thread): `put()` → `wal.append()` — opens file in append mode, writes, fsyncs.

**Thread B** (flush-worker): `_flush_memtable_to_sstable()` → `_clear_wal_for_flushed_data()` →
`wal.read_all()` then `wal.clear()` then `wal.append()` for each kept record.

Race scenario:
1. Thread B calls `wal.clear()` (kvstore.py:242) — truncates the file
2. Thread A calls `wal.append()` — writes record R to the now-empty file
3. Thread B starts rewriting kept records (kvstore.py:243-244) — overwrites R

Result: record R is lost.

Worse scenario — crash between step 1 and step 3: ALL WAL data is gone (file was cleared,
kept records not yet rewritten).

### Impact
- **Scope:** All writes happening concurrently with any background flush — this is the
  **normal operating mode** of the store once the memtable fills up and rotation begins.
- **What happens (race):** A `put()` writes to the WAL, then the flush worker truncates and
  rewrites the WAL. The put's WAL record is silently overwritten. If the process restarts
  before that entry reaches an SSTable, it is permanently lost.
- **What happens (crash):** If the process crashes between `wal.clear()` (kvstore.py:242)
  and completion of the rewrite loop (kvstore.py:243-244), the WAL file is empty or partial.
  On recovery, ALL records that should have been retained are lost — not just the current
  operation, but everything that was pending in the WAL.
- **User observes:** Intermittent, non-reproducible missing keys after restarts under load.
  Under light load with no concurrency, the bug is hidden. It surfaces under sustained
  write throughput when flushes happen frequently.
- **Likelihood:** HIGH under any real workload. The store is designed to flush in background
  threads, so this race is exercised during normal operation.
- **Blast radius:** Single keys (race scenario) or **entire WAL** (crash scenario — all
  unflushed data lost).

### Fix
Add a `threading.Lock` to the WAL class. All operations (`append`, `read_all`, `clear`)
must acquire it:
```python
class WAL:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock = threading.Lock()
        self._ensure_file_exists()

    def append(self, record: WALRecord):
        with self.lock:
            with open(self.filepath, 'a') as f:
                f.write(record.serialize())
                f.flush()
                os.fsync(f.fileno())

    def read_all(self) -> List[WALRecord]:
        with self.lock:
            ...

    def clear(self):
        with self.lock:
            open(self.filepath, 'w').close()
```

Additionally, `_clear_wal_for_flushed_data` (kvstore.py:220-244) should perform the
read-filter-clear-rewrite as a single atomic operation under the WAL lock, not as
separate calls. Ideally, write the filtered records to a temp file and rename (atomic on
POSIX):
```python
def _clear_wal_for_flushed_data(self, flushed_entries):
    with self.wal.lock:
        current_records = self.wal.read_all_unlocked()
        flushed_ts = {e.key: e.timestamp for e in flushed_entries}
        records_to_keep = [
            r for r in current_records
            if r.key not in flushed_ts or r.timestamp > flushed_ts[r.key]
        ]
        # Atomic rewrite
        temp_path = self.wal.filepath + ".tmp"
        with open(temp_path, 'w') as f:
            for record in records_to_keep:
                f.write(record.serialize())
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, self.wal.filepath)
```

### Tests to Add
- Concurrent put() from 10 threads + periodic flush — verify no records lost
- Kill process during flush cycle — verify WAL recovery is complete
- Stress test: rapid put/delete/flush cycles with thread contention

---

## P0-3: Tombstone Resurrection During Level Compaction

### Files
- `lsmkv/core/sstable_manager.py` — lines 450-454 (`_compact_level_to_next`)
- `lsmkv/core/sstable_manager.py` — lines 625-629 (`_background_compact`)

### Current Code
Both `_compact_level_to_next` and `_background_compact` do this:
```python
# sstable_manager.py:450-454
live_entries = [
    entry for entry in key_map.values()
    if not entry.is_deleted
]
```

### The Bug
Level-to-level compaction merges only **two adjacent levels** (e.g., L0 → L1) and strips
tombstones. But there may be older live entries for the same key at **deeper levels**
(L2, L3, etc.) that the tombstone was supposed to shadow.

Scenario:
1. User does `put("X", "old")` → flushed to SSTable, eventually compacted to L2
2. User does `delete("X")` → tombstone flushed to L0
3. L0 → L1 compaction runs. Merges L0 tombstone with L1 (empty). Tombstone is stripped.
4. L1 is now empty. L2 still has `("X", "old")`.
5. User does `get("X")` → memtable miss → L0 miss → L1 miss → L2 hit → **returns "old"**

The deleted key has come back to life.

### Impact
- **Scope:** Any key that (a) exists in a deeper SSTable level (L2+) and (b) is deleted
  while the tombstone sits at a shallower level (L0/L1). Becomes more likely as the store
  grows and data spans multiple levels.
- **What happens:** Level compaction strips the tombstone. The read path falls through to
  the deeper level and finds the old, supposedly-deleted value. The delete is effectively
  **undone** — the key reappears with its old value.
- **User observes:** `delete("X")` succeeds. Later (after a compaction cycle), `get("X")`
  returns the old value as if the delete never happened. This is a **correctness violation**
  — the most fundamental guarantee of a KV store (read-your-writes) is broken.
- **Likelihood:** MEDIUM — requires data at multiple levels, which happens naturally as the
  store grows. Any long-running store with deletes will hit this.
- **Blast radius:** Per-key. Every deleted key whose tombstone is compacted away before
  reaching the bottommost level will resurrect. In a workload with heavy deletes (e.g.,
  TTL-based expiration), this could affect a large fraction of keys.

### Fix
Tombstones must be **preserved** during level-to-level compaction. They can only be
removed when compacting the **bottommost level** (no deeper levels contain the key)
or during full compaction that merges all levels.

```python
# In _compact_level_to_next and _background_compact:
# Find the max level that currently has data
max_data_level = max(
    (lvl for lvl, sstables in self.levels.items() if sstables),
    default=0
)
is_bottommost = (next_level >= max_data_level)

if is_bottommost:
    # Safe to remove tombstones — no deeper levels
    merged_entries = [e for e in key_map.values() if not e.is_deleted]
else:
    # Keep tombstones — deeper levels may have shadowed data
    merged_entries = list(key_map.values())

merged_entries.sort(key=lambda e: e.key)
```

### Tests to Add
- Put key at L2, delete at L0, compact L0→L1, verify key is still deleted
- Chain: put → flush → compact to L2 → delete → flush → compact L0→L1 → get returns not found
- Full compaction after tombstones reach bottom level → verify tombstones are cleaned up

---

## P0-4: Manual `flush()` Bypasses MemtableManager Lock — Race Condition

### Files
- `lsmkv/core/kvstore.py` — lines 246-270 (the `flush()` method)

### Current Code
```python
# kvstore.py:246-270
def flush(self) -> SSTableMetadata:
    active_size = len(self.memtable_manager.active)         # line 254: no lock
    if active_size == 0:
        raise ValueError("Cannot flush empty memtable")

    entries = self.memtable_manager.active.get_all_entries() # line 259: no lock
    metadata = self.sstable_manager.add_sstable(entries)     # line 262
    self.memtable_manager.active.clear()                     # line 265: no lock
    self.wal.clear()                                         # line 268
    return metadata
```

### The Bug
This method directly accesses `self.memtable_manager.active` without acquiring
`self.memtable_manager.lock`. Meanwhile, `put()` and `delete()` hold that lock. Race:

1. `flush()` calls `active.get_all_entries()` at line 259 — reads entries [A, B, C]
2. Concurrently, `put("D", "val")` acquires lock, adds D to active memtable
3. `flush()` calls `active.clear()` at line 265 — clears the active memtable including D
4. Entry D was written to WAL (from `put()`) but the WAL is cleared at line 268
5. Entry D is **lost** — not in any SSTable, not in WAL, not in memtable

### Impact
- **Scope:** Any write (`put` or `delete`) that lands between the `get_all_entries()` read
  (line 259) and the `active.clear()` (line 265) during a manual `flush()` call.
- **What happens:** The concurrent write is added to the active memtable by `put()` (which
  holds the MemtableManager lock). Then `flush()` (which does NOT hold the lock) clears the
  entire active memtable — including the just-added entry. The WAL is then also cleared
  (line 268), removing the only remaining copy of the data.
- **User observes:** A `put()` call returns `True` (success), but the value is gone. Not in
  any SSTable, not in the WAL, not in the memtable. Unrecoverable data loss even on restart.
- **Likelihood:** MEDIUM — only triggered when a user calls `flush()` concurrently with
  `put()`/`delete()`. Less likely in single-threaded usage, but any multi-threaded application
  or CLI with a flush command can trigger it.
- **Blast radius:** Per-key. Only entries written during the narrow race window are lost, but
  those entries are **permanently** unrecoverable since the WAL is also cleared.

### Fix
Route manual flush through the MemtableManager which handles locking properly. The
simplest approach:
```python
def flush(self) -> SSTableMetadata:
    with self.memtable_manager.lock:
        if len(self.memtable_manager.active) == 0:
            raise ValueError("Cannot flush empty memtable")

        entries = self.memtable_manager.active.get_all_entries()
        metadata = self.sstable_manager.add_sstable(entries)
        self.memtable_manager.active.clear()
        self.wal.clear()
        return metadata
```

Or better — add a `flush_active()` method to MemtableManager that atomically rotates
the active memtable and flushes it synchronously, reusing the existing rotation logic:
```python
# In MemtableManager:
def flush_active_sync(self) -> Memtable:
    """Atomically rotate active memtable and return the old one for flushing."""
    with self.lock:
        if len(self.active) == 0:
            return None
        old_active = self.active
        self.active = Memtable(max_size=self.memtable_size)
        return old_active

# In LSMKVStore.flush():
def flush(self):
    old_memtable = self.memtable_manager.flush_active_sync()
    if old_memtable is None:
        raise ValueError("Cannot flush empty memtable")
    entries = old_memtable.get_all_entries()
    metadata = self.sstable_manager.add_sstable(entries)
    self._clear_wal_for_flushed_data(entries)
    return metadata
```

### Tests to Add
- Concurrent put() in one thread + flush() in another — verify no entries lost
- 1000 rapid put + flush cycles — verify all data present after recovery
- Flush with concurrent deletes — verify tombstones not lost

---

## P0-5: WAL Clear/Rewrite is Non-Atomic and O(n*m)

### Files
- `lsmkv/core/kvstore.py` — lines 220-244

### Current Code
```python
# kvstore.py:220-244
def _clear_wal_for_flushed_data(self, flushed_entries: List[Entry]):
    current_records = self.wal.read_all()                  # step 1: read
    flushed_keys = {entry.key for entry in flushed_entries}
    records_to_keep = []
    for record in current_records:
        if record.key not in flushed_keys or \
           record.timestamp > max(                         # O(n) scan per record!
               e.timestamp for e in flushed_entries if e.key == record.key
           ):
            records_to_keep.append(record)
    self.wal.clear()                                       # step 2: truncate
    for record in records_to_keep:                         # step 3: rewrite
        self.wal.append(record)
```

### Bug 1: Non-Atomic (Data Loss on Crash)
Between `self.wal.clear()` (line 242) and the completion of the rewrite loop (lines 243-244),
the WAL is in an incomplete state. A crash here loses all records that should have been kept.

### Bug 2: O(n*m) Performance
Line 238: `max(e.timestamp for e in flushed_entries if e.key == record.key)` scans the
entire `flushed_entries` list for every WAL record. With 10K WAL records and 1K flushed
entries, that's ~10M iterations per flush.

### Impact
- **Scope (crash):** All WAL records that were supposed to survive the flush — i.e., records
  for keys written AFTER the flushed memtable was rotated. These are the newest, most recent
  writes.
- **What happens (crash):** `wal.clear()` truncates the file to zero bytes. The rewrite loop
  then appends kept records one by one with `wal.append()` (which opens file, writes, fsyncs
  for EACH record). If the process crashes after `clear()` but before the loop completes,
  the WAL is either empty or partial. On recovery, the kept records are gone.
- **What happens (perf):** With a WAL of 50K records and 1K flushed entries, the inner
  `max(...)` generator runs ~50M iterations. At ~100ns per iteration, that's ~5 seconds of
  CPU per flush. During this time the flush worker is blocked, stalling all background
  flushes and causing the immutable queue to back up.
- **User observes (crash):** After a restart, recently written keys (from the current WAL
  cycle) are missing. The data was durably fsynced to the WAL, but the non-atomic rewrite
  destroyed it.
- **User observes (perf):** Flush operations take seconds instead of milliseconds. Under
  sustained write load, the immutable queue fills up, writes stall waiting for queue space.
- **Likelihood:** HIGH for the performance issue (any store with >1K entries per flush).
  MEDIUM for the crash issue (requires crash during the brief clear-rewrite window, but
  that window grows with WAL size).
- **Blast radius:** Crash: all unflushed WAL records (potentially thousands of keys).
  Perf: entire store throughput degraded during flush.

### Fix
**Atomicity:** Write kept records to a temp file, then `os.replace()` (atomic on POSIX).

**Performance:** Build a dict `{key: max_timestamp}` once before the loop.

```python
def _clear_wal_for_flushed_data(self, flushed_entries: List[Entry]):
    # Build timestamp map once — O(f) where f = len(flushed_entries)
    flushed_ts = {}
    for entry in flushed_entries:
        if entry.key not in flushed_ts or entry.timestamp > flushed_ts[entry.key]:
            flushed_ts[entry.key] = entry.timestamp

    current_records = self.wal.read_all()
    records_to_keep = [
        r for r in current_records
        if r.key not in flushed_ts or r.timestamp > flushed_ts[r.key]
    ]

    # Atomic rewrite via temp file + rename
    temp_path = self.wal.filepath + ".tmp"
    with open(temp_path, 'w') as f:
        for record in records_to_keep:
            f.write(record.serialize())
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, self.wal.filepath)
```

Note: This method is called from the background flush worker (via `_flush_memtable_to_sstable`),
so it must also be coordinated with the WAL lock from P0-2.

### Tests to Add
- Flush 10K entries from WAL with 50K remaining — measure time, should be < 100ms
- Simulate crash (kill process) during WAL rewrite — verify no data loss on recovery

---

## P0-6: `deque(maxlen=...)` Can Silently Drop Unflushed Memtables

### Files
- `lsmkv/core/memtable_manager.py` — line 87

### Current Code
```python
# memtable_manager.py:87
self.immutable_queue = deque(maxlen=max_immutable)
```

### The Bug
`deque(maxlen=N)` silently discards the oldest element when a new element is appended
beyond capacity. The current code relies on `_check_and_flush()` (called at line 185 after
`append`) to drain before overflow. In the normal flow, this works because the check
triggers at `len >= max_immutable`.

However, this is a latent data-loss trap:
- If any future code change adds to the queue without calling `_check_and_flush`
- If `_check_and_flush` is modified to skip flushing under some condition
- If a subtle race allows two near-simultaneous rotations

The deque will silently drop an unflushed memtable with **no error, no log, no indication**.

### Impact
- **Scope:** The oldest unflushed immutable memtable in the queue — could contain up to
  `memtable_size` entries (default: 10, but typically set to thousands in production).
- **What happens:** If the deque ever exceeds `maxlen` without a prior `_check_and_flush`
  draining it, the oldest immutable memtable is silently discarded by Python's deque
  implementation. No error, no log, no callback. The data in that memtable is gone — it was
  already removed from the active memtable during rotation, and if its WAL records were
  already cleared by a prior flush, there's no recovery path.
- **User observes:** Nothing. Keys that were successfully written (put returned True, WAL
  fsynced) silently disappear. No exception, no warning. The user has no way to know data
  was lost.
- **Likelihood:** LOW in current code (the check fires at capacity, preventing overflow).
  But **one code change** to `_check_and_flush` (e.g., adding a condition that skips flush)
  or `_rotate_memtable` would instantly create undetectable data loss. This is a design
  hazard, not a current runtime bug.
- **Blast radius:** One full memtable worth of entries (could be thousands of keys).
  Completely silent and unrecoverable.

### Fix
Remove `maxlen` and add an explicit check:
```python
# memtable_manager.py:87 — remove maxlen
self.immutable_queue = deque()

# memtable_manager.py:_rotate_memtable — add safety check before append
def _rotate_memtable(self):
    immutable = ImmutableMemtable(
        memtable=self.active,
        sequence_number=self.sequence_number
    )
    self.sequence_number += 1
    self.total_rotations += 1

    # Flush BEFORE adding to prevent overflow
    self._check_and_flush()

    if len(self.immutable_queue) >= self.max_immutable:
        # Queue still full after flush attempt — force synchronous flush
        oldest = self.immutable_queue.popleft()
        if self.on_flush_callback:
            self.on_flush_callback(oldest.memtable)

    self.immutable_queue.append(immutable)
    self.active = Memtable(max_size=self.memtable_size)
```

### Tests to Add
- Rapid rotation exceeding max_immutable — verify no entries dropped
- Verify that removing maxlen doesn't break existing behavior
- Stress test: 100 rapid rotations with slow flush callback — verify all data persisted

---

## P0-7: `setup.py` is Broken — Missing Dependency + Missing README

### Files
- `setup.py` — lines 6-7, 31-33

### Bug 1: `pybloomfiltermmap3` Not in `install_requires`
```python
# setup.py:31-33
install_requires=[
    "skiplistcollections>=0.0.6",
    # pybloomfiltermmap3 is MISSING
],
```
`requirements.txt` has it, but `pip install lsm-kv-store` uses `setup.py`, not
`requirements.txt`. Users get `ImportError` at runtime when `bloom_filter.py` imports
`pybloomfilter`.

### Bug 2: `setup.py` Reads Non-Existent `README.md`
```python
# setup.py:6-7
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()
```
There is no `README.md` in the project root. `pip install .` fails with `FileNotFoundError`.

### Bug 3: Version Mismatch
- `setup.py:11` → `version="1.0.0"`
- `lsmkv/__init__.py:13` → `__version__ = "1.2.0"`

### Impact
- **Scope:** Every user attempting to install or depend on this package.
- **What happens (Bug 1):** `pip install lsm-kv-store` succeeds, but the first
  `from lsmkv import LSMKVStore` (or any operation that touches bloom_filter.py) raises
  `ImportError: pybloomfiltermmap3 is required but not installed`. The user must manually
  discover and install the missing dependency.
- **What happens (Bug 2):** `pip install .` (or `pip install lsm-kv-store` from source)
  fails immediately with `FileNotFoundError: [Errno 2] No such file or directory: 'README.md'`.
  The package cannot be installed at all.
- **What happens (Bug 3):** Dependency management tools (`pip freeze`, `pipdeptree`) report
  version `1.0.0` while the code reports `1.2.0`. Users pinning versions or checking
  compatibility get wrong information.
- **User observes:** Cannot install the package (Bug 2), or installs it but gets runtime
  ImportError on first use (Bug 1). Complete blocker for any user.
- **Likelihood:** 100% — every single user hits this.
- **Blast radius:** Entire package unusable until fixed.

### Fix
```python
import os
from setuptools import setup, find_packages

readme_path = os.path.join(os.path.dirname(__file__), "README.md")
long_description = ""
if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as fh:
        long_description = fh.read()

setup(
    name="lsm-kv-store",
    version="1.2.0",  # match __init__.py
    ...
    install_requires=[
        "skiplistcollections>=0.0.6",
        "pybloomfiltermmap3>=0.5.0",
    ],
    ...
)
```

### Tests to Add
- `pip install .` in a clean venv — verify no errors
- `python -c "from lsmkv import LSMKVStore"` — verify import works
- Verify `lsmkv.__version__` matches setup.py version

---

## P0-8: No Input Validation — Arbitrary Keys/Values Accepted

### Files
- `lsmkv/core/kvstore.py` — lines 99-130 (`put()`), 167-197 (`delete()`)

### Current Code
```python
# kvstore.py:99
def put(self, key: str, value: str) -> bool:
    timestamp = self._get_timestamp()
    wal_record = WALRecord(operation=OperationType.PUT, key=key, value=value, ...)
    self.wal.append(wal_record)
    ...
```

### The Bug
No validation on `key` or `value`. Problematic inputs:
- `key=""` → empty string key, causes issues with SSTable key range checks
- `key=None` → `TypeError` deep inside serialization (confusing error)
- `value` that is 1GB → single JSON line in SSTable, mmap read loads it all, OOM
- `key` with null bytes → can break file operations on some systems

### Impact
- **Scope:** Any user passing unexpected input types or extreme sizes to `put()` or `delete()`.
- **What happens (None key):** `put(None, "val")` → `TypeError` inside `WALRecord.serialize()`
  when it tries to format `None` into the f-string. The error message references internal
  serialization code, not the user's bad input. Confusing traceback, no clear guidance.
- **What happens (empty key):** `put("", "val")` succeeds but creates an entry with key `""`.
  LazySSTable's key range check (`key < min_key or key > max_key` at sstable.py:484) behaves
  unpredictably with empty strings. SSTable metadata shows `min_key=""`, breaking
  assumptions in sparse index lookups and compaction merge logic.
- **What happens (huge value):** `put("k", "x" * 1_000_000_000)` writes a 1GB JSON line to
  the SSTable data.db file. When that SSTable is later read (e.g., during compaction),
  `mmap.read().decode('utf-8')` (sstable.py:214) loads the entire file into memory as a
  Python string → OOM kill. The store becomes unopenable after this point.
- **What happens (integer key):** `put(123, "val")` → `TypeError` during `SkipListDict`
  comparison since it expects string keys. Error message is internal, not user-facing.
- **User observes:** Cryptic tracebacks from internal code, or (with huge values) the store
  becomes permanently corrupted — unable to open or compact the offending SSTable.
- **Likelihood:** MEDIUM — depends on user discipline. Any web application passing
  unvalidated user input to the store will hit this.
- **Blast radius:** Single key for type errors (confusing but harmless). But a single huge
  value can **permanently corrupt the entire store** (OOM during compaction blocks recovery).

### Fix
Add validation at the API boundary in `put()` and `delete()`:
```python
MAX_KEY_SIZE = 1024          # 1 KB
MAX_VALUE_SIZE = 10485760    # 10 MB

def _validate_key(self, key: str):
    if not isinstance(key, str):
        raise TypeError(f"Key must be a string, got {type(key).__name__}")
    if not key:
        raise ValueError("Key cannot be empty")
    if len(key.encode('utf-8')) > MAX_KEY_SIZE:
        raise ValueError(f"Key exceeds maximum size of {MAX_KEY_SIZE} bytes")

def _validate_value(self, value: str):
    if not isinstance(value, str):
        raise TypeError(f"Value must be a string, got {type(value).__name__}")
    if len(value.encode('utf-8')) > MAX_VALUE_SIZE:
        raise ValueError(f"Value exceeds maximum size of {MAX_VALUE_SIZE} bytes")

def put(self, key: str, value: str) -> bool:
    self._validate_key(key)
    self._validate_value(value)
    ...
```

### Tests to Add
- `put("", "val")` → ValueError
- `put(None, "val")` → TypeError
- `put(123, "val")` → TypeError
- `put("k", "x" * 20_000_000)` → ValueError
- `put("k", None)` → TypeError
- `delete("")` → ValueError

---

## P0-9: `close()` Does Not Flush Pending Data — Silent Data Loss on Shutdown

### Files
- `lsmkv/core/kvstore.py` — lines 287-300 (`close()`)
- `lsmkv/core/memtable_manager.py` — lines 253-260 (`close()`)

### Current Code
```python
# kvstore.py:287-300
def close(self):
    print("Closing KV store...")
    self.memtable_manager.close()              # just shuts down thread pool
    self.sstable_manager.shutdown(wait=True, timeout=30.0)
    self.sstable_manager.close()
    print("KV store closed.")

# memtable_manager.py:253-260
def close(self):
    print("[MemtableManager] Shutting down...")
    self.flush_executor.shutdown(wait=True)     # waits for in-flight flushes only
    print("[MemtableManager] All flush workers stopped")
```

### The Bug
On `close()`:
- The active memtable (with unflushed writes) is **abandoned** — never flushed to SSTable
- The immutable queue may have memtables waiting to be flushed — only **in-flight** flushes
  are waited for, not queued ones
- The WAL is not cleared after flush, but that's moot since the data never gets flushed

If a user does:
```python
store.put("important", "data")
store.close()
```
The "important" key is in the active memtable and WAL. On reopen, WAL recovery will restore
it. **But** — the WAL recovery replays into a memtable, not into an SSTable. If the user had
relied on `close()` to persist everything durably to SSTables, they'd be wrong.

More critically: if `flush()` was called on the same session (clearing the WAL at line 268),
then `close()` is called — any puts after the last `flush()` are in the memtable and WAL.
The WAL is intact, so recovery works. But this is fragile and not documented.

The real issue: **immutable memtables in the queue that haven't been scheduled for flush are
lost**. The `close()` only waits for already-submitted tasks, not for unsubmitted queue items.

### Impact
- **Scope:** All data in the active memtable AND any immutable memtables in the queue that
  haven't been submitted to the flush thread pool yet.
- **What happens (active memtable):** Data in the active memtable is never written to an
  SSTable. It only survives if the WAL is intact (which it may not be — see P0-4 where
  `flush()` clears it). If the WAL is intact, data is recovered on restart into a memtable
  (not SSTable), meaning it's still volatile — another crash before flush loses it again.
- **What happens (queued immutables):** `MemtableManager.close()` (line 253-258) calls
  `self.flush_executor.shutdown(wait=True)`. This only waits for **already-submitted** tasks
  in the thread pool. Immutable memtables sitting in `self.immutable_queue` (not yet popped
  by `_check_and_flush`) are never flushed. Their data exists only in the WAL (if it hasn't
  been cleared by a prior flush operation). On restart, WAL recovery replays them back to
  memtables — but the same cycle repeats.
- **User observes:** After a clean `close()` + reopen, the store appears to have all data
  (thanks to WAL recovery). But the data is still in memtables, not SSTables. The store
  hasn't achieved durable persistence. A second crash (e.g., power failure on reopen) before
  any flush completes could lose everything that was only in the WAL.
- **Likelihood:** HIGH — every `close()` call. The `force_flush_all()` method exists but
  is never called during shutdown.
- **Blast radius:** All unflushed data. Could be multiple memtables worth of entries
  (active + up to `max_immutable` queued memtables). In the default config, that's up to
  5 memtables × `memtable_size` entries.

### Fix
`close()` should flush all pending data before shutting down:
```python
def close(self):
    print("Closing KV store...")

    # Flush any remaining data to SSTables
    self.memtable_manager.force_flush_all()

    # Now shutdown thread pools
    self.memtable_manager.close()
    self.sstable_manager.shutdown(wait=True, timeout=30.0)
    self.sstable_manager.close()

    # Clear WAL since everything is persisted
    self.wal.clear()
    print("KV store closed.")
```

Note: `force_flush_all()` already exists at memtable_manager.py:235-251 and does
exactly this — flushes all immutable + active memtables synchronously.

### Tests to Add
- put() → close() → reopen → get() → verify data present
- put() 1000 entries (triggers rotations) → close() immediately → reopen → verify all 1000 present
- Verify WAL is clean after close()

---

## Summary — Fix Order & Tracking

| Status | ID   | Issue                                | Risk            | Likelihood | Blast Radius                      | Effort |
|--------|------|--------------------------------------|-----------------|------------|-----------------------------------|--------|
| [x]    | P0-7 | setup.py broken install              | Install fails   | 100%       | Entire package unusable           | Small  |
| [x]    | P0-1 | WAL `\|` delimiter corruption        | Data loss       | HIGH       | Per-key (any key with `\|` or `\n`) | Small  |
| [x]    | P0-2 | WAL no thread safety                 | Data loss       | HIGH       | Single keys (race) or entire WAL (crash) | Small  |
| [x]    | P0-5 | WAL rewrite non-atomic + O(n*m)      | Data loss + perf| HIGH (perf), MEDIUM (crash) | All unflushed WAL records  | Small  |
| [x]    | P0-4 | Manual flush() race condition        | Data loss       | MEDIUM     | Per-key, permanently unrecoverable | Small  |
| [x]    | P0-3 | Tombstone resurrection in compaction | Data corruption | MEDIUM     | Per-key, deleted data reappears   | Medium |
| [x]    | P0-6 | deque maxlen silent drop             | Latent risk     | LOW (currently) | Full memtable (thousands of keys) | Small  |
| [x]    | P0-8 | No input validation                  | Crash / OOM     | MEDIUM     | Single key (type) or entire store (huge value) | Small  |
| [x]    | P0-9 | close() doesn't flush pending data   | Data loss       | HIGH       | All unflushed data (multiple memtables) | Small  |

**Progress: 9/9 fixed** ✅

Recommended order: P0-7 → P0-1 → P0-2 → P0-5 → P0-4 → P0-3 → P0-6 → P0-8 → P0-9

Rationale: P0-7 first (unblocks install), then WAL fixes (P0-1, P0-2, P0-5) as a group
since they touch the same files, then flush race (P0-4), tombstone logic (P0-3), then
the remaining safety issues (P0-6, P0-8, P0-9).
