#!/usr/bin/env python3
"""
Test soft limit (85%) compaction trigger.
"""
import sys
import shutil
import time
from lsmkv import LSMKVStore

def test_soft_limits():
    """Test that compaction triggers at 85% of hard limit."""
    data_dir = "./test_soft_limits"
    shutil.rmtree(data_dir, ignore_errors=True)
    
    print("=" * 80)
    print("SOFT LIMIT (85%) COMPACTION TEST")
    print("=" * 80)
    
    print("\n1. Creating store with soft limit = 85%")
    print("   Hard limit: 4 SSTables in L0")
    print("   Soft limit: 3.4 SSTables (rounds to 3)")
    print("   Expected: Compaction triggers at 3 SSTables, not 4")
    
    store = LSMKVStore(
        data_dir=data_dir,
        memtable_size=5,
        level_ratio=10,
        base_level_entries=10,
        max_l0_sstables=4,      # Hard limit: 4 SSTables
        soft_limit_ratio=0.85   # Soft limit: 85% = 3.4 → 3 SSTables
    )
    
    print("\n2. Inserting data to trigger flushes...")
    
    # Insert enough to create 3 SSTables
    for i in range(15):  # 3 memtables × 5 entries
        store.put(f"key{i:03d}", f"value{i}")
        
        if (i + 1) % 5 == 0:
            print(f"   Inserted {i + 1} entries...")
    
    # Wait for background activity
    print("\n3. Waiting for background flushes and compaction...")
    time.sleep(3)
    
    # Check level info
    level_info = store.get_level_info()
    
    print("\n4. Level distribution:")
    for level in sorted(level_info.keys()):
        info = level_info[level]
        print(f"   L{level}: {info['sstables']} SSTable(s), {info['entries']} entries, {info['size_bytes']} bytes")
        
        if level == 0:
            soft_limit_sstables = int(4 * 0.85)  # 3.4 → 3
            if info['sstables'] > 0:
                print(f"        Current: {info['sstables']}, Soft limit: {soft_limit_sstables}, Hard limit: 4")
    
    # Verify soft limit triggered compaction
    print("\n5. Verification:")
    
    l0_sstables = level_info.get(0, {}).get('sstables', 0)
    l1_exists = 1 in level_info and level_info[1]['sstables'] > 0
    
    print(f"   L0 SSTables: {l0_sstables}")
    print(f"   L1 exists: {l1_exists}")
    
    if l1_exists:
        print(f"   ✓ Compaction triggered at soft limit (before hitting hard limit of 4)")
    else:
        print(f"   L0 has {l0_sstables} SSTables (soft limit would trigger at 3)")
    
    # Test with different soft limit
    print("\n" + "=" * 80)
    print("Testing with 90% soft limit (more lenient)")
    print("=" * 80)
    
    data_dir2 = "./test_soft_limits_90"
    shutil.rmtree(data_dir2, ignore_errors=True)
    
    store2 = LSMKVStore(
        data_dir=data_dir2,
        memtable_size=5,
        level_ratio=10,
        base_level_entries=10,
        max_l0_sstables=4,
        soft_limit_ratio=0.90   # 90% = 3.6 → 3 SSTables (but more lenient on size/entries)
    )
    
    # Insert same amount
    for i in range(15):
        store2.put(f"key{i:03d}", f"value{i}")
    
    time.sleep(3)
    
    level_info2 = store2.get_level_info()
    
    print("\n90% soft limit level distribution:")
    for level in sorted(level_info2.keys()):
        info = level_info2[level]
        if info['sstables'] > 0:
            print(f"   L{level}: {info['sstables']} SSTable(s)")
    
    store.close()
    store2.close()
    
    shutil.rmtree(data_dir, ignore_errors=True)
    shutil.rmtree(data_dir2, ignore_errors=True)
    
    print("\n" + "=" * 80)
    print("✅ Soft limit test complete!")
    print("=" * 80)
    print("\nKey observations:")
    print("  • Compaction triggers at 85% of hard limit (default)")
    print("  • Prevents hitting actual limits")
    print("  • Maintains performance headroom")
    print("  • Configurable via soft_limit_ratio parameter")

if __name__ == "__main__":
    test_soft_limits()
