import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!/usr/bin/env python3
"""
Test script for SSTable compaction functionality.
"""
import os
import shutil
from lsmkv import LSMKVStore


def cleanup_test_data():
    """Remove test data directory."""
    test_dir = "./test_compact_data"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def test_basic_compact():
    """Test basic compaction of multiple SSTables."""
    print("Test 1: Basic Compaction")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    # Create first SSTable
    store.put("key1", "value1")
    store.put("key2", "value2")
    store.flush()
    
    # Create second SSTable
    store.put("key3", "value3")
    store.put("key4", "value4")
    store.flush()
    
    # Create third SSTable
    store.put("key5", "value5")
    store.flush()
    
    assert len(store.sstable_manager) == 3
    print("✓ Created 3 SSTables")
    
    # Compact
    metadata = store.compact()
    assert metadata.num_entries == 5
    assert metadata.min_key == "key1"
    assert metadata.max_key == "key5"
    print(f"✓ Compacted to 1 SSTable with {metadata.num_entries} entries")
    
    # Verify only one SSTable exists
    assert len(store.sstable_manager) == 1
    print("✓ Only 1 SSTable remains")
    
    # Verify all data is accessible
    assert store.get("key1").value == "value1"
    assert store.get("key3").value == "value3"
    assert store.get("key5").value == "value5"
    print("✓ All data accessible after compaction")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 1 passed!\n")


def test_compact_with_updates():
    """Test that compaction keeps latest version of updated keys."""
    print("Test 2: Compaction with Updates")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    # Create multiple versions of the same keys
    store.put("user:1", "Alice_v1")
    store.put("user:2", "Bob_v1")
    store.flush()
    
    store.put("user:1", "Alice_v2")
    store.put("user:3", "Charlie")
    store.flush()
    
    store.put("user:1", "Alice_v3")
    store.put("user:2", "Bob_v2")
    store.flush()
    
    assert len(store.sstable_manager) == 3
    print("✓ Created 3 SSTables with duplicate keys")
    
    # Compact
    metadata = store.compact()
    # Should have 3 unique keys
    assert metadata.num_entries == 3
    print(f"✓ Compacted to {metadata.num_entries} unique entries")
    
    # Verify latest versions are kept
    assert store.get("user:1").value == "Alice_v3"
    assert store.get("user:2").value == "Bob_v2"
    assert store.get("user:3").value == "Charlie"
    print("✓ Latest versions preserved")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 2 passed!\n")


def test_compact_removes_tombstones():
    """Test that compaction removes deleted entries."""
    print("Test 3: Compaction Removes Tombstones")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    # Add data
    store.put("key1", "value1")
    store.put("key2", "value2")
    store.put("key3", "value3")
    store.put("key4", "value4")
    store.flush()
    
    # Delete some keys
    store.delete("key2")
    store.delete("key4")
    store.flush()
    
    assert len(store.sstable_manager) == 2
    print("✓ Created 2 SSTables (data + tombstones)")
    
    # Get total entries before compaction
    total_entries_before = sum(len(st.read_all()) for st in store.sstable_manager.sstables)
    print(f"✓ Total entries before: {total_entries_before}")
    
    # Compact
    metadata = store.compact()
    # Should only have 2 live entries (key1 and key3)
    assert metadata.num_entries == 2
    print(f"✓ Compacted to {metadata.num_entries} live entries")
    
    # Verify deleted keys are gone
    assert store.get("key1").found == True
    assert store.get("key2").found == False
    assert store.get("key3").found == True
    assert store.get("key4").found == False
    print("✓ Tombstones removed, deletions preserved")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 3 passed!\n")


def test_compact_with_mixed_operations():
    """Test compaction with a mix of inserts, updates, and deletes."""
    print("Test 4: Mixed Operations Compaction")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    # Batch 1: Initial data
    store.put("a", "1")
    store.put("b", "2")
    store.put("c", "3")
    store.flush()
    
    # Batch 2: Update a, delete b, add d
    store.put("a", "updated")
    store.delete("b")
    store.put("d", "4")
    store.flush()
    
    # Batch 3: Delete c, add e
    store.delete("c")
    store.put("e", "5")
    store.flush()
    
    assert len(store.sstable_manager) == 3
    print("✓ Created 3 SSTables with mixed operations")
    
    # Compact
    metadata = store.compact()
    # Should have: a (updated), d, e (b and c deleted)
    assert metadata.num_entries == 3
    print(f"✓ Compacted to {metadata.num_entries} entries")
    
    # Verify final state
    assert store.get("a").value == "updated"
    assert store.get("b").found == False
    assert store.get("c").found == False
    assert store.get("d").value == "4"
    assert store.get("e").value == "5"
    print("✓ Final state correct")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 4 passed!\n")


def test_compact_persistence():
    """Test that compacted data persists across restarts."""
    print("Test 5: Compaction Persistence")
    print("-" * 40)
    
    cleanup_test_data()
    
    # Create store, add data, compact
    store1 = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    store1.put("key1", "v1")
    store1.flush()
    store1.put("key1", "v2")
    store1.put("key2", "value2")
    store1.flush()
    store1.compact()
    store1.close()
    print("✓ Data compacted")
    
    # Restart and verify
    store2 = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    assert len(store2.sstable_manager) == 1
    assert store2.get("key1").value == "v2"
    assert store2.get("key2").value == "value2"
    print("✓ Compacted data loaded on restart")
    
    store2.close()
    cleanup_test_data()
    print("✓ Test 5 passed!\n")


def test_compact_empty_error():
    """Test that compacting with no SSTables raises error."""
    print("Test 6: Empty Compaction Error")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    try:
        store.compact()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "no sstables" in str(e).lower()
        print("✓ Empty compaction raises ValueError")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 6 passed!\n")


def test_compact_all_deleted():
    """Test compaction when all entries are deleted."""
    print("Test 7: All Entries Deleted")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    # Add and delete all data
    store.put("key1", "value1")
    store.put("key2", "value2")
    store.flush()
    
    store.delete("key1")
    store.delete("key2")
    store.flush()
    
    # Try to compact - should fail since all entries are deleted
    try:
        store.compact()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "no live entries" in str(e).lower()
        print("✓ All-deleted compaction raises ValueError")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 7 passed!\n")


def test_compact_reduces_size():
    """Test that compaction reduces total size on disk."""
    print("Test 8: Size Reduction")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_compact_data", memtable_size=1000)
    
    # Create overlapping data
    for i in range(5):
        store.put("key", f"value_{i}")
        store.flush()
    
    stats_before = store.stats()
    size_before = stats_before["total_sstable_size_bytes"]
    num_before = stats_before["num_sstables"]
    print(f"✓ Before: {num_before} SSTables, {size_before} bytes")
    
    # Compact
    store.compact()
    
    stats_after = store.stats()
    size_after = stats_after["total_sstable_size_bytes"]
    num_after = stats_after["num_sstables"]
    print(f"✓ After: {num_after} SSTable, {size_after} bytes")
    
    assert num_after == 1
    assert size_after < size_before
    print(f"✓ Size reduced by {size_before - size_after} bytes")
    
    # Verify data
    assert store.get("key").value == "value_4"
    print("✓ Latest value preserved")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 8 passed!\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 40)
    print("SSTable Compaction Test Suite")
    print("=" * 40 + "\n")
    
    try:
        test_basic_compact()
        test_compact_with_updates()
        test_compact_removes_tombstones()
        test_compact_with_mixed_operations()
        test_compact_persistence()
        test_compact_empty_error()
        test_compact_all_deleted()
        test_compact_reduces_size()
        
        print("=" * 40)
        print("All compaction tests passed! ✓")
        print("=" * 40)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
