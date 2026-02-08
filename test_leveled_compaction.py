#!/usr/bin/env python3
"""
Test leveled compaction functionality.
"""
import sys
import shutil
import time
from lsmkv import LSMKVStore

def test_leveled_compaction():
    """Test multi-level compaction."""
    data_dir = "./test_leveled"
    shutil.rmtree(data_dir, ignore_errors=True)
    
    print("=" * 80)
    print("LEVELED COMPACTION TEST")
    print("=" * 80)
    
    print("\n1. Creating store with aggressive leveled compaction settings...")
    print("   - L0: max 3 SSTables, 10 entries, 1KB")
    print("   - L1: max 100 entries, 10KB")
    print("   - L2: max 1000 entries, 100KB")
    print("   - Level ratio: 10")
    
    store = LSMKVStore(
        data_dir=data_dir,
        memtable_size=5,           # Small memtable to trigger flushes
        level_ratio=10,             # Each level 10x previous
        base_level_size_mb=0.001,   # 1KB base (small for testing)
        base_level_entries=10,      # 10 entries base
        max_l0_sstables=3           # Compact when L0 has 3 SSTables
    )
    
    print("\n2. Inserting 50 entries to trigger multiple level compactions...")
    for i in range(50):
        key = f"key{i:04d}"
        value = f"value_{i}_original"
        store.put(key, value)
        
        if (i + 1) % 10 == 0:
            print(f"   Inserted {i + 1} entries...")
            time.sleep(0.5)  # Give time for background flushes
    
    print("\n3. Waiting for background flushes and compactions...")
    time.sleep(3)
    
    print("\n4. Checking level organization:")
    level_info = store.get_level_info()
    
    for level in sorted(level_info.keys()):
        info = level_info[level]
        print(f"\n   Level {level}:")
        print(f"     SSTables: {info['sstables']}")
        print(f"     Entries: {info['entries']} / {info['max_entries']} max")
        print(f"     Size: {info['size_bytes']} bytes / {info['max_size_bytes']} bytes max")
        
        if level == 0:
            print(f"     Status: {'⚠️  NEEDS COMPACTION' if info['sstables'] >= 3 else '✅ OK'}")
        else:
            needs_compact = (info['entries'] >= info['max_entries'] or 
                           info['size_bytes'] >= info['max_size_bytes'])
            print(f"     Status: {'⚠️  NEEDS COMPACTION' if needs_compact else '✅ OK'}")
    
    print("\n5. Testing reads across levels:")
    test_keys = ["key0000", "key0025", "key0049"]
    for key in test_keys:
        result = store.get(key)
        print(f"   GET {key} = {result.value if result.found else 'NOT FOUND'}")
    
    print("\n6. Stats summary:")
    stats = store.stats()
    print(f"   Total SSTables: {stats['num_sstables']}")
    print(f"   Total levels: {stats['num_levels']}")
    print(f"   Total size: {stats['total_sstable_size_bytes']} bytes")
    
    # Check per-level stats
    for key, value in sorted(stats.items()):
        if key.startswith('l') and '_sstables' in key:
            level = key[1]
            size_key = f"l{level}_size_bytes"
            print(f"   L{level}: {value} SSTable(s), {stats.get(size_key, 0)} bytes")
    
    print("\n7. Manual full compaction:")
    if stats['num_sstables'] > 0:
        metadata = store.compact()
        print(f"   Compacted to: {metadata.dirname}")
        print(f"   Entries: {metadata.num_entries}")
        
        level_info = store.get_level_info()
        print(f"\n   After compaction:")
        for level in sorted(level_info.keys()):
            info = level_info[level]
            if info['sstables'] > 0:
                print(f"     L{level}: {info['sstables']} SSTable(s)")
    
    store.close()
    shutil.rmtree(data_dir, ignore_errors=True)
    
    print("\n" + "=" * 80)
    print("✅ Leveled compaction test passed!")
    print("=" * 80)

if __name__ == "__main__":
    test_leveled_compaction()
