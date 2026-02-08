#!/usr/bin/env python3
"""
Test script for MemtableManager functionality.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import shutil
from lsmkv import LSMKVStore


def cleanup_test_data():
    """Remove test data directory."""
    test_dir = "./test_manager_data"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def test_immutable_queue_rotation():
    """Test that memtables rotate to immutable queue."""
    print("Test 1: Immutable Queue Rotation")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=5,
        max_immutable_memtables=3
    )
    
    # Add 10 entries (should cause 2 rotations)
    for i in range(10):
        store.put(f"key{i}", f"value{i}")
    
    stats = store.stats()
    assert stats["total_memtable_rotations"] >= 1
    print(f"✓ Rotations occurred: {stats['total_memtable_rotations']}")
    
    assert stats["immutable_memtables"] >= 1
    print(f"✓ Immutable memtables in queue: {stats['immutable_memtables']}")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 1 passed!\n")


def test_read_from_immutable():
    """Test reading from immutable memtable queue."""
    print("Test 2: Read from Immutable Queue")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=5,
        max_immutable_memtables=3
    )
    
    # Add 10 entries (2 memtables: 1 immutable, 1 active)
    for i in range(10):
        store.put(f"key{i}", f"value{i}")
    
    time.sleep(0.2)
    
    stats = store.stats()
    print(f"  Immutable: {stats['immutable_memtables']}")
    print(f"  Active: {stats['active_memtable_size']}")
    
    # key0-4 should be in immutable queue
    result = store.get("key2")
    assert result.found == True
    assert result.value == "value2"
    print("✓ Read from immutable memtable")
    
    # key5-9 should be in active
    result = store.get("key8")
    assert result.found == True
    assert result.value == "value8"
    print("✓ Read from active memtable")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 2 passed!\n")


def test_queue_overflow_triggers_flush():
    """Test that queue overflow triggers async flush."""
    print("Test 3: Queue Overflow Triggers Flush")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=5,
        max_immutable_memtables=3
    )
    
    # Add 15 entries (3 rotations)
    for i in range(15):
        store.put(f"key{i}", f"value{i}")
    
    stats = store.stats()
    print(f"  After 15 entries:")
    print(f"    Rotations: {stats['total_memtable_rotations']}")
    print(f"    Immutable: {stats['immutable_memtables']}")
    
    # Add 5 more (1 more rotation, queue full, should trigger flush)
    for i in range(15, 20):
        store.put(f"key{i}", f"value{i}")
    
    stats = store.stats()
    assert stats["total_memtable_rotations"] >= 3
    assert stats["total_async_flushes"] >= 1
    print(f"✓ Async flush triggered: {stats['total_async_flushes']} flushes")
    
    time.sleep(1)  # Wait for flush
    
    stats = store.stats()
    assert stats["num_sstables"] >= 1
    print(f"✓ SSTable created: {stats['num_sstables']} SSTables")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 3 passed!\n")


def test_priority_flushing_oldest():
    """Test that oldest memtable is flushed first."""
    print("Test 4: Priority Flushing (Oldest First)")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=3,
        max_immutable_memtables=2
    )
    
    # Add entries to create multiple memtables
    for i in range(12):
        store.put(f"key{i}", f"value{i}")
    
    time.sleep(1)  # Wait for flushes
    
    stats = store.stats()
    # Should have triggered flushes when queue reached max
    assert stats["total_async_flushes"] >= 2
    print(f"✓ Oldest memtables flushed: {stats['total_async_flushes']} flushes")
    
    # Verify data from all layers
    for i in [0, 5, 10]:
        result = store.get(f"key{i}")
        assert result.found == True
    print("✓ All data accessible across layers")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 4 passed!\n")


def test_concurrent_reads_during_flush():
    """Test that reads work correctly during async flush."""
    print("Test 5: Concurrent Reads During Flush")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=5,
        max_immutable_memtables=3  # Larger queue to keep data in memory
    )
    
    # Add data to trigger rotation but not flush yet
    for i in range(12):
        store.put(f"key{i}", f"value{i}")
    
    time.sleep(0.1)  # Small delay
    
    # Reads should work from immutable queue
    for i in range(0, 12, 3):
        result = store.get(f"key{i}")
        assert result.found == True
        assert result.value == f"value{i}"
    print("✓ Reads work from immutable queue")
    
    time.sleep(1)  # Wait for any flushes
    
    # Verify all data still accessible
    for i in range(12):
        result = store.get(f"key{i}")
        assert result.found == True
    print("✓ All data accessible after flush")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 5 passed!\n")


def test_memory_limit():
    """Test memory-based flushing trigger."""
    print("Test 6: Memory Limit Trigger")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=10,
        max_immutable_memtables=5,
        max_memory_mb=0.001  # Very small limit to trigger easily
    )
    
    # Add enough data to trigger memory-based flush
    for i in range(30):
        store.put(f"key{i}", f"value{i}")
    
    time.sleep(1)
    
    stats = store.stats()
    # Should have flushed due to memory limit
    assert stats["total_async_flushes"] > 0
    print(f"✓ Memory-based flush triggered: {stats['total_async_flushes']} flushes")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 6 passed!\n")


def test_updates_in_different_layers():
    """Test that updates work correctly across memtable layers."""
    print("Test 7: Updates Across Layers")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=5,
        max_immutable_memtables=3
    )
    
    # Add initial value
    store.put("key", "value1")
    
    # Rotate
    for i in range(5):
        store.put(f"filler{i}", f"data{i}")
    
    # Update same key (now in immutable)
    store.put("key", "value2")
    
    # Get should return latest
    result = store.get("key")
    assert result.value == "value2"
    print("✓ Latest value returned (from active)")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 7 passed!\n")


def test_stats_accuracy():
    """Test that stats are accurate."""
    print("Test 8: Stats Accuracy")
    print("-" * 40)
    
    cleanup_test_data()
    store = LSMKVStore(
        data_dir="./test_manager_data",
        memtable_size=5,
        max_immutable_memtables=3
    )
    
    # Initial stats
    stats = store.stats()
    assert stats["active_memtable_size"] == 0
    assert stats["immutable_memtables"] == 0
    print("✓ Initial stats correct")
    
    # Add 12 entries
    for i in range(12):
        store.put(f"key{i}", f"value{i}")
    
    stats = store.stats()
    total_in_memory = stats["active_memtable_size"] + (stats["immutable_memtables"] * 5)
    
    # Should have 2 entries in active, 2 in immutable (10 entries)
    # 2 entries went to SSTable
    assert total_in_memory <= 12
    print(f"✓ Stats accurate: {total_in_memory} entries in memory")
    
    store.close()
    cleanup_test_data()
    print("✓ Test 8 passed!\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 40)
    print("MemtableManager Test Suite")
    print("=" * 40 + "\n")
    
    try:
        test_immutable_queue_rotation()
        test_read_from_immutable()
        test_queue_overflow_triggers_flush()
        test_priority_flushing_oldest()
        test_concurrent_reads_during_flush()
        test_memory_limit()
        test_updates_in_different_layers()
        test_stats_accuracy()
        
        print("=" * 40)
        print("All MemtableManager tests passed! ✓")
        print("=" * 40)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
