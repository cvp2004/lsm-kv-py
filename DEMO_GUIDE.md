# Interactive Demo Script Guide

## Overview

`demo_all_features.sh` is an **interactive** shell script that demonstrates every feature of the LSM KV Store with detailed explanations and user prompts.

## Running the Demo

```bash
# First, install dependencies (REQUIRED)
pip install -r requirements.txt

# Then run the demo
./demo_all_features.sh
```

## What Makes This Demo Special

### 🎯 Interactive Experience
- **Pauses after each phase** - Press ENTER to continue
- **Explains what's coming** - Shows what to watch for before each step
- **Color-coded output** - Easy to distinguish different types of information
- **Thread activity visible** - See main thread vs worker threads

### 🎨 Color Coding

- 🔵 **Blue [OBSERVE]** - Things to observe in output
- 🟢 **Green [ACTION]** - Actions being performed  
- 🟣 **Purple [THREAD]** - Thread-specific activity
- 🟡 **Yellow sections** - Phase headers and "what's next" explanations
- 🔷 **Cyan sections** - Major section headers

### 📋 12 Interactive Phases

#### **PHASE 1: Initial Setup and Basic Operations** ⏱️ ~30 seconds
**What happens:**
- Creates LSM KV Store with memtable_size=5
- Inserts 25 key-value pairs
- Triggers multiple memtable rotations
- Background workers flush to SSTables

**What to watch for:**
- `[MAIN THREAD]` writes happening instantly
- `[MemtableManager] Rotated to immutable queue` - instant rotation
- `[Flush-Worker] Flushed memtable` - background activity
- Stats showing rotations and flushes

**Key observation:** Main thread never blocks!

---

#### **PHASE 2: Inspecting Directory Structure** ⏱️ ~10 seconds
**What happens:**
- Shows main data directory
- Lists sstables/ directory
- Shows individual SSTable directories
- Displays WAL and manifest files

**What to observe:**
- `sstable_000000/`, `sstable_000001/`, etc.
- Three files per SSTable directory
- WAL file (if not yet cleared)
- manifest.json with metadata

---

#### **PHASE 3: Inspecting SSTable Internals** ⏱️ ~15 seconds
**What happens:**
- Opens each SSTable directory
- Shows contents of data.db (JSON lines)
- Displays bloom_filter.bf size
- Displays sparse_index.idx size

**What to observe:**
- JSON format in data.db (human-readable)
- Small Bloom filter files (~30-40 bytes)
- Small sparse index files (~50-70 bytes)
- Total overhead is minimal

---

#### **PHASE 4: Testing Read Operations** ⏱️ ~10 seconds
**What happens:**
- Tests positive lookups (keys that exist)
- Tests negative lookups (keys that don't exist)
- Shows Bloom filter optimization

**What to observe:**
- Positive lookups: "Bloom filter check: PASS"
- Negative lookups: "Bloom filter check: REJECT" (no disk I/O!)
- Sparse index locating scan ranges
- mmap being used for data access

**Key observation:** Bloom filter saves disk I/O for non-existent keys

---

#### **PHASE 5: Updates and Deletions** ⏱️ ~15 seconds
**What happens:**
- Updates existing keys with new values
- Deletes some keys (creates tombstones)
- Verifies operations worked

**What to observe:**
- Newer timestamps override older values
- Tombstones created for deletions
- More rotations and background flushes
- Updated SSTable count

**Key observation:** Memtable data overrides SSTable data

---

#### **PHASE 6: Manual Flush Operation** ⏱️ ~10 seconds
**What happens:**
- Adds new data to active memtable
- Manually triggers flush command
- Creates new SSTable

**What to observe:**
- Manual flush in addition to automatic
- New SSTable directory created
- All three files created (data, bloom, index)
- Active memtable cleared after flush

**Key observation:** Both manual and automatic flush work together

---

#### **PHASE 7: Compaction** ⏱️ ~15 seconds
**What happens:**
- Counts existing SSTables
- Runs compaction to merge all SSTables
- Deletes old SSTable directories
- Creates single compacted SSTable

**What to observe:**
- Before: Multiple SSTable directories
- Process: Deduplication + tombstone removal
- After: Single SSTable directory
- Disk space reduction
- Old directories deleted

**Key observation:** Space savings from removing duplicates and tombstones

---

#### **PHASE 8: Detailed SSTable Structure** ⏱️ ~20 seconds
**What happens:**
- Deep dive into each SSTable directory
- Shows complete file breakdown
- Displays sample data entries
- Explains each file's purpose

**What to observe:**
- data.db: JSON lines format, mmap I/O
- bloom_filter.bf: pybloomfiltermmap3, mmap-backed
- sparse_index.idx: Binary format, bisect-based
- Exact file sizes

**Key observation:** Clean, organized structure with minimal overhead

---

#### **PHASE 9: Persistence and Recovery** ⏱️ ~15 seconds
**What happens:**
- Closes store (simulates app shutdown)
- Reopens store (simulates app restart)
- Recovers from WAL and SSTables
- Verifies all data still accessible

**What to observe:**
- WAL recovery log
- SSTable loading from manifest
- All data intact after "restart"
- Deletions persisted correctly

**Key observation:** Complete durability and crash recovery

---

#### **PHASE 10: Thread Activity Demonstration** ⏱️ ~20 seconds
**What happens:**
- Rapid insertion of 30 entries
- High-volume write workload
- Shows thread pool in action

**What to watch for:**
- `[MAIN THREAD]` writing continuously
- `[MemtableManager]` rotating instantly
- `[Flush-Worker]` flushing in background
- Throughput measurement (writes/sec)
- Main thread never waits!

**Key observation:** Zero write blocking - this is the magic of background flushing!

---

#### **PHASE 11: Final Statistics and Summary** ⏱️ ~10 seconds
**What happens:**
- Displays comprehensive statistics
- Shows all counters and metrics
- Lists all SSTables and entry counts

**What to observe:**
- Total rotations performed
- Total async flushes completed
- Entry distribution across SSTables
- Memory usage
- File sizes

---

#### **PHASE 12: Complete Directory Tree** ⏱️ ~5 seconds
**What happens:**
- Shows complete directory structure
- Visual tree representation (if available)
- All files listed

**What to observe:**
- Clean hierarchical structure
- All SSTables in sstables/ directory
- Each SSTable with three files
- Complete file organization

---

### **SUMMARY SECTIONS** ⏱️ ~10 seconds
**What happens:**
- Recaps all 9 features demonstrated
- Explains thread model
- Lists all files created
- Provides exploration commands

**Final output:**
- ✅ Checklist of all features
- 📊 Thread activity summary
- 📁 Files overview
- 💡 Commands to explore further

---

## Understanding the Output

### Thread Labels

```
[MAIN THREAD]        ← Your application's main thread
[MemtableManager]    ← Memtable management events
[Flush-Worker]       ← Background thread pool workers (2 workers)
```

### Timing Information

Watch for patterns like:
```
[MAIN THREAD] PUT user_004 = name_E
[MemtableManager] Rotated to immutable queue (size=1)  ← < 1ms
[MAIN THREAD] PUT user_005 = name_F                    ← Immediate!
...
[Flush-Worker] Flushed memtable seq=0 (5 entries) in 0.003s  ← Background
```

This shows:
1. **Rotation is instant** (< 1ms)
2. **Main thread continues** immediately
3. **Worker flushes in background** (3ms)

### Key Metrics to Observe

**Write Performance:**
- Rotation time: < 1ms
- Throughput: 30+ writes/sec (unaffected by flushes)

**Flush Performance:**
- Flush time: 2-10ms per memtable
- Parallel execution: 2 workers simultaneously

**Read Performance:**
- Bloom filter: < 0.1ms (for negative lookups)
- Sparse index: < 0.1ms (for binary search)

**Space Efficiency:**
- Bloom filter: ~30-40 bytes per SSTable
- Sparse index: ~50-70 bytes per SSTable
- Total overhead: ~100 bytes per SSTable

## Exploring After the Demo

The script provides commands to explore:

```bash
# View manifest with all SSTable metadata
cat ./demo_full_features/manifest.json | python3 -m json.tool

# View SSTable data (JSON format)
cat ./demo_full_features/sstables/sstable_*/data.db

# List all SSTables
ls -la ./demo_full_features/sstables/

# Check WAL contents
cat ./demo_full_features/wal.log

# See complete structure
tree ./demo_full_features/
```

## Total Demo Time

**Estimated duration:** 3-5 minutes (with reading and understanding)
- Actual execution: ~2 minutes
- Reading/understanding: 1-3 minutes

You control the pace by pressing ENTER after each phase!

## What You'll Learn

By the end of this demo, you'll understand:

1. ✅ How LSM trees work (memtable → SSTable flow)
2. ✅ How background flushing prevents write blocking
3. ✅ How Bloom filters optimize reads
4. ✅ How sparse indexes reduce scan ranges
5. ✅ How mmap improves I/O performance
6. ✅ How compaction optimizes space
7. ✅ How durability is ensured (WAL + SSTables)
8. ✅ How recovery works on restart
9. ✅ Thread model (main + worker pool)

## Tips for Maximum Learning

1. **Read the "NEXT:" sections carefully** - They explain what's about to happen
2. **Watch for thread labels** - See which thread is doing what
3. **Compare "before" and "after" stats** - Observe the changes
4. **Check the file sizes** - See the minimal overhead
5. **Note the timing information** - Understand the performance
6. **Explore the data directory** - Manual inspection after demo

## Cleanup

After the demo:
```bash
rm -rf ./demo_full_features
```

## Troubleshooting

### Script Fails on Import

**Error:**
```
ModuleNotFoundError: No module named 'pybloomfilter'
```

**Fix:**
```bash
pip install pybloomfiltermmap3
```

### Want to Run Again

Simply run the script again:
```bash
./demo_all_features.sh
```

It will automatically clean up and start fresh.

## Advanced: Running Phases Separately

If you want to run phases manually, you can extract the Python code from the script and run it directly:

```bash
# View the embedded Python code
grep -A 50 "cat > /tmp/demo_lsm" demo_all_features.sh
```

Then copy and modify as needed for your own experiments!
