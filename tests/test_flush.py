import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!/usr/bin/env python3
"""
Test script for SSTable flush functionality.
"""
import os
import shutil
from lsmkv import LSMKVStore


def cleanup_test_data():
    """Remove test data directory."""
    test_dir = "./test_flush_data"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def test_basic_flush():
    """Test basic flush operation."""
    print("Test 1: Basic Flush")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    
    # Add data to memtable
    store.put("key1", "value1")
    store.put("key2", "value2")
    store.put("key3", "value3")
    
    stats = store.stats()
    assert stats["active_memtable_size"] == 3
    assert stats["num_sstables"] == 0
    print("✓ Added 3 entries to memtable")
    
    # Flush to SSTable
    metadata = store.flush()
    assert metadata.num_entries == 3
    assert metadata.min_key == "key1"
    assert metadata.max_key == "key3"
    print(f"✓ Flushed to {metadata.dirname}")
    
    # Check stats after flush
    stats = store.stats()
    assert stats["active_memtable_size"] == 0
    assert stats["num_sstables"] == 1
    print("✓ Memtable cleared, SSTable created")
    
    # Verify data can still be retrieved
    result = store.get("key1")
    assert result.found == True
    assert result.value == "value1"
    
    result = store.get("key2")
    assert result.found == True
    assert result.value == "value2"
    print("✓ Data retrieved from SSTable")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 1 passed!\n")


def test_flush_persistence():
    """Test that flushed data persists across restarts."""
    print("Test 2: Flush Persistence")
    print("-" * 40)
    
    cleanup_test_data()
    
    # Create store, add data, and flush
    store1 = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    store1.put("persistent1", "value1")
    store1.put("persistent2", "value2")
    store1.flush()
    store1.close()
    print("✓ Data flushed to SSTable")
    
    # Create new store instance (simulates restart)
    store2 = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    
    # Verify data loaded from SSTable
    result = store2.get("persistent1")
    assert result.found == True
    assert result.value == "value1"
    
    result = store2.get("persistent2")
    assert result.found == True
    assert result.value == "value2"
    
    print("✓ Data loaded from SSTable on restart")
    store2.close()
    cleanup_test_data()
    print("✓ Test 2 passed!\n")


def test_multiple_flushes():
    """Test multiple flush operations."""
    print("Test 3: Multiple Flushes")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    
    # First flush
    store.put("batch1_key1", "value1")
    store.put("batch1_key2", "value2")
    store.flush()
    print("✓ First flush completed")
    
    # Second flush
    store.put("batch2_key1", "value3")
    store.put("batch2_key2", "value4")
    store.flush()
    print("✓ Second flush completed")
    
    # Third flush
    store.put("batch3_key1", "value5")
    store.flush()
    print("✓ Third flush completed")
    
    stats = store.stats()
    assert stats["num_sstables"] == 3
    print(f"✓ Created {stats['num_sstables']} SSTables")
    
    # Verify all data is accessible
    assert store.get("batch1_key1").value == "value1"
    assert store.get("batch2_key2").value == "value4"
    assert store.get("batch3_key1").value == "value5"
    print("✓ All data accessible from multiple SSTables")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 3 passed!\n")


def test_flush_with_deletes():
    """Test flushing with deleted entries (tombstones)."""
    print("Test 4: Flush with Deletes")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    
    # Add and delete some data
    store.put("key1", "value1")
    store.put("key2", "value2")
    store.put("key3", "value3")
    store.delete("key2")
    print("✓ Added 3 entries, deleted 1")
    
    # Flush
    metadata = store.flush()
    assert metadata.num_entries == 3  # Including tombstone
    print("✓ Flushed including tombstone")
    
    # Verify deleted key is not found
    result = store.get("key2")
    assert result.found == False
    print("✓ Deleted key not found")
    
    # Verify other keys exist
    assert store.get("key1").found == True
    assert store.get("key3").found == True
    print("✓ Other keys still accessible")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 4 passed!\n")


def test_mixed_memtable_sstable():
    """Test reading from both memtable and SSTables."""
    print("Test 5: Mixed Memtable and SSTable Reads")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    
    # Add data and flush
    store.put("old1", "value1")
    store.put("old2", "value2")
    store.flush()
    print("✓ Flushed old data to SSTable")
    
    # Add new data to memtable
    store.put("new1", "value3")
    store.put("new2", "value4")
    print("✓ Added new data to memtable")
    
    # Verify can read from both
    assert store.get("old1").value == "value1"  # From SSTable
    assert store.get("new1").value == "value3"  # From memtable
    print("✓ Can read from both memtable and SSTable")
    
    # Update an old key
    store.put("old1", "updated_value")
    assert store.get("old1").value == "updated_value"
    print("✓ Updated value in memtable overrides SSTable")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 5 passed!\n")


def test_empty_flush():
    """Test that flushing empty memtable raises error."""
    print("Test 6: Empty Flush Error")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_flush_data", memtable_size=1000)
    
    try:
        store.flush()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "empty" in str(e).lower()
        print("✓ Empty flush raises ValueError")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 6 passed!\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 40)
    print("SSTable Flush Test Suite")
    print("=" * 40 + "\n")
    
    try:
        test_basic_flush()
        test_flush_persistence()
        test_multiple_flushes()
        test_flush_with_deletes()
        test_mixed_memtable_sstable()
        test_empty_flush()
        
        print("=" * 40)
        print("All flush tests passed! ✓")
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
