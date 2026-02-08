# Leveled Compaction Design

## Requirements

### Level Organization
1. **Level 0 (L0)**: Multiple SSTables allowed, duplicates across SSTables OK
2. **Level 1+**: Single SSTable per level, no duplicates

### Compaction Triggers
Compact and move to next level when either:
1. **Entry count** exceeds limit (derived from level ratio)
2. **Total size** exceeds limit (derived from level ratio)

### Configuration
- Level ratio: Configurable parameter in LSMKVStore
- Default: 10 (each level is 10x the previous level)

## Design

### Level Size Calculation

```
Level 0: Base size (e.g., 100 entries or 10KB)
Level 1: Base × ratio (e.g., 1000 entries or 100KB)
Level 2: Level 1 × ratio (e.g., 10000 entries or 1MB)
Level 3: Level 2 × ratio (e.g., 100000 entries or 10MB)
...

Formula:
  max_entries[level] = base_entries × (ratio ^ level)
  max_size[level] = base_size × (ratio ^ level)
```

### Compaction Strategy

**Level 0 → Level 1:**
- Triggered when: L0 has > N SSTables OR total L0 size > limit
- Process: Merge all L0 SSTables → single L1 SSTable
- Result: One SSTable in L1 with all duplicates resolved

**Level 1 → Level 2:**
- Triggered when: L1 size > limit
- Process: Merge L1 SSTable with L2 SSTable (if exists)
- Result: One SSTable in L2

**Level N → Level N+1:**
- Same pattern as L1→L2

### Data Structure

```python
# SSTableManager will maintain:
{
    0: [sstable_000, sstable_001, sstable_002],  # Level 0: Multiple SSTables
    1: [sstable_003],                             # Level 1: Single SSTable
    2: [sstable_004],                             # Level 2: Single SSTable
    3: [],                                         # Level 3: Empty
}
```

## Implementation Plan

### 1. Update SSTableManager

**Add level tracking:**
```python
class SSTableManager:
    def __init__(..., level_ratio=10, base_level_size_mb=1):
        self.level_ratio = level_ratio
        self.base_level_size_mb = base_level_size_mb
        self.levels: Dict[int, List[SSTable]] = {}  # Level → SSTables
```

**New methods:**
- `get_level_max_size(level)` - Calculate max size for level
- `get_level_max_entries(level)` - Calculate max entries for level
- `should_compact_level(level)` - Check if level needs compaction
- `compact_level(level)` - Compact specific level to next level
- `auto_compact()` - Automatically compact levels that need it

### 2. Compaction Logic

**L0 → L1 (Special Case):**
```python
def compact_level_0_to_1():
    # Collect all L0 SSTables
    l0_sstables = self.levels[0]
    
    # Read all entries
    all_entries = []
    for sstable in l0_sstables:
        all_entries.extend(sstable.read_all())
    
    # Deduplicate + remove tombstones
    merged = deduplicate_and_sort(all_entries)
    
    # If L1 exists, merge with it
    if self.levels[1]:
        l1_entries = self.levels[1][0].read_all()
        merged = deduplicate_and_sort(merged + l1_entries)
    
    # Create new L1 SSTable
    delete_old_sstables(l0_sstables + self.levels[1])
    new_sstable = create_sstable(merged, level=1)
    self.levels[1] = [new_sstable]
    self.levels[0] = []
```

**L1+ → L(N+1) (General Case):**
```python
def compact_level_n_to_n_plus_1(level):
    # Get current level SSTable (only one)
    current_sstable = self.levels[level][0]
    current_entries = current_sstable.read_all()
    
    # If next level exists, merge with it
    if self.levels[level + 1]:
        next_entries = self.levels[level + 1][0].read_all()
        merged = deduplicate_and_sort(current_entries + next_entries)
    else:
        merged = current_entries
    
    # Delete old SSTables
    delete_old_sstables([current_sstable] + self.levels.get(level + 1, []))
    
    # Create new SSTable at next level
    new_sstable = create_sstable(merged, level=level + 1)
    self.levels[level + 1] = [new_sstable]
    self.levels[level] = []
```

### 3. Read Path

**Updated search order:**
```
1. Check MemtableManager (active + immutable)
2. Check Level 0 SSTables (newest to oldest)
3. Check Level 1 SSTable
4. Check Level 2 SSTable
5. Check Level 3 SSTable
...
```

Optimization: Higher levels have older data, can stop early

### 4. Automatic Compaction

**After each flush:**
```python
def add_sstable(entries):
    # Create new SSTable at L0
    sstable = create_sstable(entries, level=0)
    self.levels[0].append(sstable)
    
    # Check if auto-compaction needed
    self.auto_compact()

def auto_compact():
    # Check each level bottom-up
    for level in range(max_level, -1, -1):
        if self.should_compact_level(level):
            self.compact_level(level)
```

### 5. Configuration

**New parameters:**
```python
LSMKVStore(
    data_dir="./data",
    level_ratio=10,              # NEW: Size multiplier between levels
    base_level_size_mb=1,        # NEW: L0 size limit
    base_level_entries=1000,     # NEW: L0 entry limit
    max_l0_sstables=4,           # NEW: L0 SSTable count limit
)
```

## Example Scenario

### Configuration
```
level_ratio = 10
base_level_size_mb = 1
base_level_entries = 100
max_l0_sstables = 4
```

### Level Limits
```
L0: 4 SSTables max, 100 entries total, 1MB total
L1: 1 SSTable, 1000 entries, 10MB
L2: 1 SSTable, 10000 entries, 100MB
L3: 1 SSTable, 100000 entries, 1GB
```

### Compaction Flow

**Step 1: Initial writes (memtable → L0)**
```
L0: [sstable_000, sstable_001, sstable_002, sstable_003]  ← 4 SSTables
L1: []
L2: []

Trigger: L0 has 4 SSTables (max_l0_sstables reached)
```

**Step 2: Compact L0 → L1**
```
L0: []
L1: [sstable_004]  ← All L0 merged into one L1 SSTable
L2: []

Result: Duplicates resolved, tombstones removed
```

**Step 3: More writes**
```
L0: [sstable_005, sstable_006, sstable_007, sstable_008]  ← 4 SSTables again
L1: [sstable_004]
L2: []

Trigger: L0 has 4 SSTables again
```

**Step 4: Compact L0 → L1 (merge with existing L1)**
```
L0: []
L1: [sstable_009]  ← New L0 + old L1 merged
L2: []

Note: If L1 now exceeds limit, triggers L1 → L2 compaction
```

**Step 5: L1 exceeds limit**
```
L1 size: 12MB > 10MB limit

Trigger: Compact L1 → L2
```

**Step 6: Compact L1 → L2**
```
L0: []
L1: []
L2: [sstable_010]  ← L1 moved to L2

Result: Data ages down through levels
```

## Benefits

### 1. Write Amplification
**Without levels:** Every compaction merges ALL data
**With levels:** Only compact one level at a time

### 2. Read Performance  
**Without levels:** Check all SSTables
**With levels:** 
- Check L0 (few SSTables)
- Check L1 (one SSTable)
- Check L2 (one SSTable)
- Early termination possible

### 3. Space Efficiency
- Older data at higher levels (less frequently accessed)
- Newer data at lower levels (more frequently accessed)
- Automatic aging of data

### 4. Tunable Performance
```
Small level_ratio (e.g., 4):
  - More frequent compactions
  - Less space amplification
  - More write amplification

Large level_ratio (e.g., 10):
  - Less frequent compactions
  - More space amplification
  - Less write amplification
```

## Implementation Priority

1. ✅ Update SSTableMetadata to track level
2. ✅ Update SSTableManager to use level-based organization
3. ✅ Implement level size limits
4. ✅ Implement should_compact_level()
5. ✅ Implement compact_level() for L0 → L1
6. ✅ Implement compact_level() for L1+ → L(N+1)
7. ✅ Add automatic compaction after flush
8. ✅ Update read path to check levels in order
9. ✅ Add level-based statistics
10. ✅ Update tests

Ready to implement! 🚀
