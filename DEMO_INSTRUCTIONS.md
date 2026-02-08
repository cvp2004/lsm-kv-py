# How to Run the Complete Feature Demonstration

## Prerequisites

The demonstration script requires all dependencies to be installed:

```bash
# Install all dependencies
pip install -r requirements.txt

# Or install individually
pip install skiplistcollections
pip install pybloomfiltermmap3
```

## Running the Demo

Once dependencies are installed, run the comprehensive demonstration:

```bash
./demo_all_features.sh
```

This script will show you:

### 1. **Write-Ahead Log (WAL)**
- See operations being logged
- Observe fsync to disk
- Watch WAL clearing after flush

### 2. **MemtableManager Activity**
- Main thread: Instant writes and rotations
- Watch active memtable fill up
- See rotation to immutable queue
- Observe queue management (max 4 memtables)

### 3. **Background Thread Pool**
- Worker Thread 1 & 2 running in parallel
- Non-blocking async flushes
- FIFO queue processing (oldest first)
- Main thread continues without blocking

### 4. **SSTable Directory Structure**
- Each SSTable in its own directory
- Three files per SSTable:
  - `data.db` - Main data (mmap I/O)
  - `bloom_filter.bf` - Bloom filter (pybloomfiltermmap3)
  - `sparse_index.idx` - Sparse index (bisect-based)

### 5. **Bloom Filters in Action**
- Fast rejection of non-existent keys
- mmap-backed persistence
- See disk I/O savings

### 6. **Sparse Indexes with Bisect**
- O(log n) binary search
- Floor and ceil operations
- Reduced scan ranges

### 7. **mmap I/O Performance**
- Memory-mapped file access
- OS-managed caching
- Efficient random access

### 8. **Compaction**
- Merge multiple SSTables
- Keep latest versions
- Remove tombstones
- Reduce disk space

### 9. **Recovery**
- Automatic WAL recovery
- Load SSTables from manifest
- Data persistence

## Output Structure

The script is organized into phases:

```
PHASE 1: Initial Setup and Basic Operations
  - Create store
  - Insert 25 entries
  - Watch rotations and flushes
  - Thread activity visible

PHASE 2: Inspecting Directory Structure
  - Show data directory
  - Show SSTable directories
  - Show individual files

PHASE 3: Inspecting SSTable Internals
  - View data.db contents
  - Check bloom_filter.bf
  - Check sparse_index.idx

PHASE 4: Testing Read Operations
  - Positive lookups (with Bloom filter PASS)
  - Negative lookups (with Bloom filter REJECT)
  - Observe thread activity

PHASE 5: Updates and Deletions
  - Update existing keys
  - Delete keys
  - Verify tombstones

PHASE 6: Manual Flush Operation
  - Trigger manual flush
  - See new SSTable created

PHASE 7: Compaction
  - Merge multiple SSTables
  - Show space savings
  - Observe file cleanup

PHASE 8: Persistence and Recovery
  - Close and reopen store
  - Verify data persisted
  - Check WAL recovery

PHASE 9: Thread Activity Demonstration
  - High-volume writes
  - Show main thread never blocks
  - Watch worker threads flush in background
  - Measure throughput

PHASE 10: Final Statistics
  - Complete statistics
  - Entry counts
  - File sizes

PHASE 11: Complete Directory Tree
  - Visual tree structure
  - All files and sizes

SUMMARY: Features Demonstrated
  - Checklist of all features
  - Thread activity summary
  - Files overview
```

## Expected Output

You'll see detailed output including:

- `[MAIN THREAD]` - Main thread operations
- `[MemtableManager]` - Memtable management events
- `[Flush-Worker]` - Background flush activity
- `[ACTION]` - Actions being performed
- `[OBSERVE]` - Things to observe
- `[THREAD]` - Thread-specific activity

## Alternative: Simpler Demo

If you don't want to install dependencies, you can run a simpler demo:

```bash
python3 demo_enhanced_sstable.py
```

This requires pybloomfiltermmap3 to be installed but shows the key features.

## Manual Exploration

After running the demo, explore the data directory:

```bash
# View manifest
cat ./demo_full_features/manifest.json | python3 -m json.tool

# View SSTable data
cat ./demo_full_features/sstables/sstable_*/data.db | head -10

# List all SSTable directories
ls -la ./demo_full_features/sstables/

# Check file sizes
du -h ./demo_full_features/sstables/*/

# View WAL
cat ./demo_full_features/wal.log

# Tree view (if tree command available)
tree ./demo_full_features/
```

## Cleanup

After the demonstration:

```bash
rm -rf ./demo_full_features
```

## Troubleshooting

### Error: "No module named 'pybloomfilter'"

**Solution**: Install pybloomfiltermmap3:
```bash
pip install pybloomfiltermmap3
```

### Error: "No module named 'lsmkv'"

**Solution**: Run from the project root directory or install in development mode:
```bash
pip install -e .
```

### SSL Certificate Errors

**Solution**: Update certificates or use:
```bash
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org pybloomfiltermmap3
```

## Understanding Thread Activity

### Main Thread
- Handles all PUT/GET/DELETE operations
- Writes to WAL (with fsync)
- Updates active memtable  
- Rotates when full (instant, non-blocking)
- **NEVER** waits for flush to complete

### Worker Threads (Thread Pool: 2 workers)
- Pick up memtables from immutable queue
- Flush to SSTable (I/O happens here)
- Create Bloom filter file
- Create sparse index file
- Update manifest
- Clear WAL entries
- Run in parallel with main thread

### Observing Thread Activity

Look for these patterns in the output:

```
[MAIN THREAD] PUT user_004 = name_E
[MemtableManager] Rotated to immutable queue (size=1)  ← Instant!
[MAIN THREAD] PUT user_005 = name_F                    ← Continues immediately
...
[Flush-Worker] Flushed memtable seq=0 (5 entries) in 0.003s  ← Background
```

This shows:
1. Main thread rotates instantly
2. Main thread continues writing
3. Worker thread flushes in background

## Performance Observations

Watch for:
- **Rotation time**: < 1ms (instant)
- **Write throughput**: Unaffected by flushes
- **Flush time**: 2-10ms per memtable (background)
- **Bloom filter lookups**: < 0.1ms
- **Sparse index lookups**: < 0.1ms

## Complete Feature Verification

After running the script, you should see:
- ✅ WAL file created and managed
- ✅ Multiple SSTable directories created
- ✅ Bloom filter files in each SSTable
- ✅ Sparse index files in each SSTable
- ✅ Compaction merging SSTables
- ✅ Data persisting across restarts
- ✅ Thread pool activity (non-blocking)
- ✅ All operations working correctly
