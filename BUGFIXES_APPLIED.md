# Bug Fixes Applied — Logical Loopholes & Concurrency Issues

This document tracks the bug fixes applied per the analysis. Each fix is marked with its status.

## CRITICAL — Data Loss Risks

### ✅ #1: _clear_wal_for_flushed_data NOT atomic across WAL operations
**Files:** `wal.py`, `kvstore.py`

**Fix:** Added `WAL.replace_with_filtered(filter_fn)` that performs read-filter-write **under a single lock**. The entire operation is now atomic — no concurrent `put()` can append between read and replace.

### ✅ #2: put()/delete() split-brain between WAL and memtable ordering
**Files:** `kvstore.py`

**Fix:** Added `_write_lock` in LSMKVStore. `put()` and `delete()` now hold this lock for the full sequence: timestamp → WAL append → memtable update. WAL and memtable ordering are guaranteed to match.

### ✅ #3: flush_active_sync creates visibility gap
**Files:** `memtable_manager.py`, `kvstore.py`

**Fix:** `flush_active_sync()` now adds the rotated memtable to the immutable queue before returning, so it remains visible during reads. After flush completes, `remove_flushed_immutable()` removes it. No key is invisible between rotate and SSTable write.

### ✅ #4: close() clears WAL before async workers finish
**Files:** `kvstore.py`

**Fix:** Reordered shutdown: (1) set `_closed`, (2) `force_flush_all`, (3) `memtable_manager.close()` — wait for all async flush workers, (4) **then** `WAL.clear()`. WAL is only cleared after all flush workers have finished.

---

## HIGH — Concurrency Design Issues

### ✅ #5: force_flush_all holds lock during all I/O
**Files:** `memtable_manager.py`

**Fix:** Refactored to pop one memtable under lock, release lock, call flush callback, repeat. Lock is not held during SSTable write or WAL I/O.

### ✅ #6: SSTableManager.get() holds lock during full read I/O path
**Files:** `sstable_manager.py`

**Fix:** Snapshot `levels` under lock as a dict copy, then perform all bloom filter / mmap reads **without** holding the lock. Reads are no longer serialized.

### ✅ #7: Synchronous backpressure flush blocks writer thread
**Files:** `memtable_manager.py`

**Fix:** When queue is at limit, `_check_and_flush` returns the oldest for sync flush. The caller (`put`/`delete`) releases the lock **before** calling `_async_flush`. Sync flush no longer blocks other operations.

### ✅ #8: _auto_compact reads all SSTable data under lock
**Files:** `sstable_manager.py`

**Fix:** Moved `_auto_compact` call in `add_sstable` outside the `with self.lock` block. `_take_compaction_snapshot` copies sstable references under lock, then reads entries **without** the lock.

---

## MEDIUM — Logic & Correctness Gaps

### ✅ #9: _get_timestamp() uses non-monotonic wall clock
**Files:** `kvstore.py`

**Fix:** Switched to `time.monotonic_ns() // 1000` for monotonically increasing timestamps. Avoids NTP/clock-backward issues.

### ✅ #10: Unbounded immutable queue growth
**Files:** `memtable_manager.py`

**Fix:** Sync flush now triggers when queue `>= max_immutable` (not 2×). Queue is bounded at max_immutable.

### ✅ #11: Synchronous compact() deletes before creating — crash-unsafe
**Files:** `sstable_manager.py`

**Fix:** Full `compact()` and `_compact_level_to_next()` now create the new SSTable first, then delete old SSTables. Same create-then-delete pattern as background compaction.

### ✅ #12: _trigger_manifest_reload TOCTOU race
**Files:** `sstable_manager.py`

**Fix:** Added `_manifest_reload_lock`. The check-and-set for `_manifest_reload_pending` and submission are now under a single lock.

### ❌ #13: Duplicated compaction logic (sync vs async)
**Status:** Cancelled — deferred for maintainability. Both paths now use create-then-delete for crash safety.

---

## LOW — Minor Issues

### ✅ #14: LazySSTable._access_count not atomic
**Files:** `sstable.py`

**Fix:** Wrapped access count increment in `_access_lock` for atomicity.

### ✅ #15: SSTable.read_all() mmap seek/read not thread-safe
**Files:** `sstable.py`

**Fix:** Added `_read_lock` around the mmap seek/read block in `read_all()`.

### ✅ #16: No shutdown flag — writes accepted during/after close()
**Files:** `kvstore.py`

**Fix:** Added `_closed` flag set at start of `close()`. `put()`, `delete()`, and `get()` now raise `RuntimeError` if store is closed.

### ✅ #17: _get_level_stats race with compaction
**Files:** `sstable_manager.py`

**Fix:** Wrapped `size_bytes()` calls in try/except and `exists()` check. Gracefully handles SSTables deleted by concurrent compaction.

---

## Test Fixes

- **test_level_info:** Uses an isolated subdirectory so it does not inherit residual SSTables from earlier tests.
- **compact() race:** Added `wait_for_compaction()` before full compact to avoid race with in-flight background compaction (fixes test_compact flakiness).

---

## Summary

| Severity | Fixed | Total |
|----------|-------|-------|
| CRITICAL | 4     | 4     |
| HIGH     | 4     | 4     |
| MEDIUM   | 4     | 5 (1 cancelled) |
| LOW      | 4     | 4     |
| **Total**| **16**| **17** |
