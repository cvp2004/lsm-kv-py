import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#!/usr/bin/env python3
"""
Test script for the LSM KV Store.
"""
import os
import shutil
from lsmkv import LSMKVStore
from lsmkv.core.dto import GetResult


def cleanup_test_data():
    """Remove test data directory."""
    test_dir = "./test_data"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def test_basic_operations():
    """Test basic PUT, GET, DELETE operations."""
    print("Test 1: Basic Operations")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_data", memtable_size=1000)
    
    # Test PUT
    assert store.put("user:1", "Alice") == True
    assert store.put("user:2", "Bob") == True
    assert store.put("user:3", "Charlie") == True
    print("✓ PUT operations successful")
    
    # Test GET
    result = store.get("user:1")
    assert result.found == True
    assert result.value == "Alice"
    
    result = store.get("user:2")
    assert result.found == True
    assert result.value == "Bob"
    
    result = store.get("nonexistent")
    assert result.found == False
    print("✓ GET operations successful")
    
    # Test DELETE
    assert store.delete("user:2") == True
    result = store.get("user:2")
    assert result.found == False
    print("✓ DELETE operations successful")
    
    # Test UPDATE
    assert store.put("user:1", "Alice Updated") == True
    result = store.get("user:1")
    assert result.value == "Alice Updated"
    print("✓ UPDATE operations successful")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 1 passed!\n")


def test_wal_recovery():
    """Test WAL durability and recovery."""
    print("Test 2: WAL Recovery")
    print("-" * 40)
    
    cleanup_test_data()
    
    # Create store and add data
    store1 = LSMKVStore(data_dir="./test_data", memtable_size=1000)
    store1.put("key1", "value1")
    store1.put("key2", "value2")
    store1.put("key3", "value3")
    store1.delete("key2")
    store1.close()
    print("✓ Data written to WAL")
    
    # Create new store instance (simulates restart)
    store2 = LSMKVStore(data_dir="./test_data", memtable_size=1000)
    
    # Verify data recovered from WAL
    result = store2.get("key1")
    assert result.found == True
    assert result.value == "value1"
    
    result = store2.get("key2")
    assert result.found == False  # Was deleted
    
    result = store2.get("key3")
    assert result.found == True
    assert result.value == "value3"
    
    print("✓ Data successfully recovered from WAL")
    store2.close()
    cleanup_test_data()
    print("✓ Test 2 passed!\n")


def test_stats():
    """Test statistics reporting."""
    print("Test 3: Statistics")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_data", memtable_size=1000)
    
    # Initially empty
    stats = store.stats()
    assert stats["active_memtable_size"] == 0
    assert stats["active_memtable_full"] == False
    print(f"✓ Initial stats: {stats}")
    
    # Add some data
    for i in range(3):
        store.put(f"key{i}", f"value{i}")
    
    stats = store.stats()
    assert stats["active_memtable_size"] == 3
    assert stats["active_memtable_full"] == False
    print(f"✓ After 3 inserts: {stats}")
    
    # Fill close to capacity (avoid auto-flush)
    for i in range(3, 999):
        store.put(f"key{i}", f"value{i}")
    
    stats = store.stats()
    assert stats["active_memtable_size"] == 999
    assert stats["active_memtable_full"] == False  # Not quite full
    print(f"✓ Near-full memtable: {stats}")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 3 passed!\n")


def test_large_dataset():
    """Test with larger dataset."""
    print("Test 4: Large Dataset")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(data_dir="./test_data", memtable_size=1000)
    
    # Insert many records
    num_records = 50
    for i in range(num_records):
        store.put(f"key_{i:04d}", f"value_{i:04d}")
    
    print(f"✓ Inserted {num_records} records")
    
    # Verify all records
    for i in range(num_records):
        result = store.get(f"key_{i:04d}")
        assert result.found == True
        assert result.value == f"value_{i:04d}"
    
    print(f"✓ Verified all {num_records} records")
    
    # Delete half
    for i in range(0, num_records, 2):
        store.delete(f"key_{i:04d}")
    
    print(f"✓ Deleted {num_records // 2} records")
    
    # Verify deletions
    for i in range(num_records):
        result = store.get(f"key_{i:04d}")
        if i % 2 == 0:
            assert result.found == False
        else:
            assert result.found == True
    
    print(f"✓ Verified deletions")
    
    stats = store.stats()
    print(f"✓ Final stats: {stats}")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 4 passed!\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 40)
    print("LSM KV Store Test Suite")
    print("=" * 40 + "\n")
    
    try:
        test_basic_operations()
        test_wal_recovery()
        test_stats()
        test_large_dataset()
        
        print("=" * 40)
        print("All tests passed! ✓")
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
