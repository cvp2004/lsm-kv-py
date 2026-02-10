#!/usr/bin/env python3
"""
Test suite for non-blocking background compaction.

Validates:
1. Compaction runs in background thread
2. Main thread reads/writes are not blocked during compaction
3. Snapshot-based compaction maintains data consistency
4. Manifest updates happen only after SSTable is persisted
"""
import os
import sys
import time
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lsmkv import LSMKVStore


def cleanup_test_dir(test_dir: str):
    """Clean up test directory."""
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def test_compaction_is_background():
    """Test that compaction doesn't block the add_sstable call."""
    print("\n" + "=" * 70)
    print("TEST: Compaction runs in background")
    print("=" * 70)
    
    test_dir = "./test_data_background_compact"
    cleanup_test_dir(test_dir)
    
    try:
        # Configure with very small limits to trigger compaction
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=100,  # Very small to trigger flushes
            max_l0_sstables=2,  # Very low to trigger compaction
            soft_limit_ratio=0.5,  # 50% = 1 SSTable triggers compaction
            level_ratio=2,
            max_immutable_memtables=1
        )
        
        write_times = []
        
        # Write data in bursts to trigger multiple flushes and compactions
        for burst in range(5):
            start = time.time()
            for i in range(20):
                key = f"key_{burst}_{i:04d}"
                value = f"value_{burst}_{i}" * 10
                store.put(key, value)
            elapsed = time.time() - start
            write_times.append(elapsed)
            print(f"  Burst {burst + 1}: wrote 20 keys in {elapsed:.3f}s")
            time.sleep(0.5)  # Allow compaction to start
        
        # Writes should be fast (not blocked by compaction)
        avg_time = sum(write_times) / len(write_times)
        max_time = max(write_times)
        
        print(f"\n  Average write burst time: {avg_time:.3f}s")
        print(f"  Max write burst time: {max_time:.3f}s")
        
        # Wait for any pending compactions
        store.sstable_manager.wait_for_compaction(timeout=10.0)
        
        # Verify data integrity
        print(f"\n  Verifying data integrity...")
        errors = 0
        for burst in range(5):
            for i in range(20):
                key = f"key_{burst}_{i:04d}"
                expected = f"value_{burst}_{i}" * 10
                result = store.get(key)
                if not result.found or result.value != expected:
                    errors += 1
        
        print(f"  Verification complete: {errors} errors")
        
        # Check compaction stats
        stats = store.sstable_manager
        print(f"\n  Background compactions triggered: {stats.background_compactions}")
        print(f"  Total compactions completed: {stats.total_compactions}")
        
        store.close()
        
        # All writes should complete in reasonable time (not blocked)
        assert max_time < 5.0, f"Max write time {max_time}s indicates blocking"
        assert errors == 0, f"Found {errors} data integrity errors"
        
        print("\n  PASSED: Compaction runs in background without blocking writes")
        
    finally:
        cleanup_test_dir(test_dir)


def test_concurrent_reads_during_compaction():
    """Test that reads are not blocked during compaction."""
    print("\n" + "=" * 70)
    print("TEST: Concurrent reads during compaction")
    print("=" * 70)
    
    test_dir = "./test_data_concurrent_reads"
    cleanup_test_dir(test_dir)
    
    try:
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=200,
            max_l0_sstables=3,
            soft_limit_ratio=0.67,  # Trigger at 2 SSTables
            level_ratio=2,
            max_immutable_memtables=2
        )
        
        # Pre-populate data
        print("  Pre-populating data...")
        for i in range(100):
            store.put(f"key_{i:04d}", f"value_{i}" * 20)
        
        time.sleep(1)  # Allow flushes
        
        print(f"  Initial state: {store.sstable_manager.count()} SSTables")
        
        read_results = {"count": 0, "success": 0, "latencies": []}
        write_results = {"count": 0}
        stop_flag = threading.Event()
        
        def reader_thread():
            """Continuously read keys."""
            while not stop_flag.is_set():
                for i in range(100):
                    if stop_flag.is_set():
                        break
                    start = time.time()
                    result = store.get(f"key_{i:04d}")
                    latency = time.time() - start
                    read_results["count"] += 1
                    read_results["latencies"].append(latency)
                    if result.found:
                        read_results["success"] += 1
        
        def writer_thread():
            """Continuously write new keys to trigger compaction."""
            batch = 0
            while not stop_flag.is_set():
                for i in range(50):
                    if stop_flag.is_set():
                        break
                    store.put(f"new_key_{batch}_{i}", f"new_value_{batch}_{i}" * 10)
                    write_results["count"] += 1
                batch += 1
                time.sleep(0.1)
        
        # Start threads
        reader = threading.Thread(target=reader_thread)
        writer = threading.Thread(target=writer_thread)
        
        print("  Starting concurrent reads and writes...")
        reader.start()
        writer.start()
        
        # Run for 5 seconds
        time.sleep(5)
        stop_flag.set()
        
        reader.join(timeout=5)
        writer.join(timeout=5)
        
        # Wait for compactions
        store.sstable_manager.wait_for_compaction(timeout=10)
        
        # Analyze results
        avg_latency = sum(read_results["latencies"]) / len(read_results["latencies"]) if read_results["latencies"] else 0
        max_latency = max(read_results["latencies"]) if read_results["latencies"] else 0
        p99_latency = sorted(read_results["latencies"])[int(len(read_results["latencies"]) * 0.99)] if read_results["latencies"] else 0
        
        print(f"\n  Read operations: {read_results['count']}")
        print(f"  Successful reads: {read_results['success']}")
        print(f"  Write operations: {write_results['count']}")
        print(f"  Average read latency: {avg_latency * 1000:.2f}ms")
        print(f"  Max read latency: {max_latency * 1000:.2f}ms")
        print(f"  P99 read latency: {p99_latency * 1000:.2f}ms")
        print(f"  Background compactions: {store.sstable_manager.background_compactions}")
        
        store.close()
        
        # Verify reads were fast (not blocked)
        assert p99_latency < 1.0, f"P99 latency {p99_latency}s too high, indicates blocking"
        assert read_results["count"] > 100, "Not enough reads completed"
        
        print("\n  PASSED: Reads continue during compaction with low latency")
        
    finally:
        cleanup_test_dir(test_dir)


def test_data_consistency_during_compaction():
    """Test that data remains consistent during compaction."""
    print("\n" + "=" * 70)
    print("TEST: Data consistency during compaction")
    print("=" * 70)
    
    test_dir = "./test_data_consistency"
    cleanup_test_dir(test_dir)
    
    try:
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=150,
            max_l0_sstables=2,
            soft_limit_ratio=0.5,
            level_ratio=2,
            max_immutable_memtables=2
        )
        
        # Write initial data
        print("  Writing initial data...")
        for i in range(50):
            store.put(f"key_{i:04d}", f"initial_value_{i}")
        
        time.sleep(0.5)
        
        # Update half the keys (creates newer versions)
        print("  Updating keys...")
        for i in range(0, 50, 2):
            store.put(f"key_{i:04d}", f"updated_value_{i}")
        
        time.sleep(0.5)
        
        # Delete some keys
        print("  Deleting keys...")
        for i in range(5, 50, 10):
            store.delete(f"key_{i:04d}")
        
        time.sleep(0.5)
        
        # Add more data to trigger compaction
        print("  Adding more data to trigger compaction...")
        for i in range(100, 200):
            store.put(f"new_key_{i}", f"new_value_{i}" * 5)
        
        # Wait for compaction
        print("  Waiting for compaction to complete...")
        store.sstable_manager.wait_for_compaction(timeout=15)
        time.sleep(1)
        
        # Verify consistency
        print("  Verifying data consistency...")
        errors = []
        
        # Check updated keys
        for i in range(0, 50, 2):
            result = store.get(f"key_{i:04d}")
            if i in [5, 15, 25, 35, 45]:
                # Should be deleted
                if result.found:
                    errors.append(f"key_{i:04d} should be deleted but found")
            else:
                # Should have updated value
                expected = f"updated_value_{i}"
                if not result.found:
                    errors.append(f"key_{i:04d} not found (expected: {expected})")
                elif result.value != expected:
                    errors.append(f"key_{i:04d} has wrong value: {result.value}")
        
        # Check non-updated keys
        for i in range(1, 50, 2):
            result = store.get(f"key_{i:04d}")
            if i in [5, 15, 25, 35, 45]:
                if result.found:
                    errors.append(f"key_{i:04d} should be deleted")
            else:
                expected = f"initial_value_{i}"
                if not result.found:
                    errors.append(f"key_{i:04d} not found")
                elif result.value != expected:
                    errors.append(f"key_{i:04d} has wrong value")
        
        # Check new keys
        for i in range(100, 200):
            result = store.get(f"new_key_{i}")
            expected = f"new_value_{i}" * 5
            if not result.found:
                errors.append(f"new_key_{i} not found")
            elif result.value != expected:
                errors.append(f"new_key_{i} has wrong value")
        
        if errors:
            print(f"  ERRORS ({len(errors)}):")
            for err in errors[:10]:
                print(f"    - {err}")
        else:
            print("  All data verified correctly")
        
        print(f"\n  Background compactions: {store.sstable_manager.background_compactions}")
        print(f"  Total compactions: {store.sstable_manager.total_compactions}")
        
        store.close()
        
        assert len(errors) == 0, f"Data consistency errors: {errors[:5]}"
        
        print("\n  PASSED: Data remains consistent during compaction")
        
    finally:
        cleanup_test_dir(test_dir)


def test_manifest_atomic_update():
    """Test that manifests are updated atomically after compaction."""
    print("\n" + "=" * 70)
    print("TEST: Manifest atomic updates")
    print("=" * 70)
    
    test_dir = "./test_data_manifest_atomic"
    cleanup_test_dir(test_dir)
    
    try:
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=100,
            max_l0_sstables=2,
            soft_limit_ratio=0.5,
            level_ratio=2,
            max_immutable_memtables=1
        )
        
        # Write data to create SSTables
        print("  Creating SSTables...")
        for i in range(60):
            store.put(f"key_{i:04d}", f"value_{i}" * 10)
            time.sleep(0.01)
        
        time.sleep(1)  # Allow flushes
        
        initial_sstables = store.sstable_manager.count()
        print(f"  Initial SSTables: {initial_sstables}")
        
        # Trigger more writes to force compaction
        print("  Triggering compaction...")
        for i in range(60, 120):
            store.put(f"key_{i:04d}", f"value_{i}" * 10)
            time.sleep(0.01)
        
        # Wait for compaction
        print("  Waiting for compaction...")
        store.sstable_manager.wait_for_compaction(timeout=15)
        time.sleep(1)
        
        final_sstables = store.sstable_manager.count()
        print(f"  Final SSTables: {final_sstables}")
        
        # Check manifest consistency with on-disk state
        manifest_entries = {}
        for level in store.sstable_manager.levels:
            manifest = store.sstable_manager.level_manifest_manager.get_level_manifest(level)
            manifest_entries[level] = len(manifest.entries)
        
        in_memory = {level: len(sstables) for level, sstables in store.sstable_manager.levels.items()}
        
        print(f"  Manifest entries per level: {manifest_entries}")
        print(f"  In-memory SSTables per level: {in_memory}")
        
        store.close()
        
        # Verify manifest matches in-memory state
        for level in manifest_entries:
            assert manifest_entries[level] == in_memory.get(level, 0), \
                f"Level {level} manifest ({manifest_entries[level]}) != memory ({in_memory.get(level, 0)})"
        
        print("\n  PASSED: Manifest updates are atomic and consistent")
        
    finally:
        cleanup_test_dir(test_dir)


def test_high_concurrency_stress():
    """High concurrency stress test."""
    print("\n" + "=" * 70)
    print("TEST: High concurrency stress test")
    print("=" * 70)
    
    test_dir = "./test_data_stress"
    cleanup_test_dir(test_dir)
    
    try:
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=500,
            max_l0_sstables=4,
            soft_limit_ratio=0.75,
            level_ratio=4,
            max_immutable_memtables=3
        )
        
        num_writers = 4
        num_readers = 4
        keys_per_writer = 500
        test_duration = 10  # seconds
        
        write_counts = [0] * num_writers
        read_counts = [0] * num_readers
        read_hits = [0] * num_readers
        errors = []
        stop_flag = threading.Event()
        
        def writer(writer_id):
            key_idx = 0
            while not stop_flag.is_set() and key_idx < keys_per_writer:
                key = f"w{writer_id}_key_{key_idx:05d}"
                value = f"value_{writer_id}_{key_idx}" * 5
                try:
                    store.put(key, value)
                    write_counts[writer_id] += 1
                except Exception as e:
                    errors.append(f"Write error: {e}")
                key_idx += 1
                time.sleep(0.001)
        
        def reader(reader_id):
            while not stop_flag.is_set():
                # Read from any writer's keys
                writer_id = reader_id % num_writers
                key_idx = read_counts[reader_id] % max(1, write_counts[writer_id])
                key = f"w{writer_id}_key_{key_idx:05d}"
                try:
                    result = store.get(key)
                    read_counts[reader_id] += 1
                    if result.found:
                        read_hits[reader_id] += 1
                except Exception as e:
                    errors.append(f"Read error: {e}")
                time.sleep(0.0005)
        
        print(f"  Starting {num_writers} writers and {num_readers} readers...")
        
        with ThreadPoolExecutor(max_workers=num_writers + num_readers) as executor:
            # Start writers and readers
            futures = []
            for i in range(num_writers):
                futures.append(executor.submit(writer, i))
            for i in range(num_readers):
                futures.append(executor.submit(reader, i))
            
            # Run for test duration or until writers complete
            time.sleep(test_duration)
            stop_flag.set()
            
            # Wait for completion
            for future in as_completed(futures, timeout=30):
                try:
                    future.result()
                except Exception as e:
                    errors.append(f"Thread error: {e}")
        
        # Wait for compactions
        store.sstable_manager.wait_for_compaction(timeout=15)
        
        total_writes = sum(write_counts)
        total_reads = sum(read_counts)
        total_hits = sum(read_hits)
        
        print(f"\n  Total writes: {total_writes}")
        print(f"  Total reads: {total_reads}")
        print(f"  Read hits: {total_hits} ({100 * total_hits / max(1, total_reads):.1f}%)")
        print(f"  Errors: {len(errors)}")
        print(f"  Background compactions: {store.sstable_manager.background_compactions}")
        print(f"  Total compactions: {store.sstable_manager.total_compactions}")
        
        stats = store.stats()
        print(f"  Total SSTables: {stats.get('sstable_count', 0)}")
        print(f"  Levels: {stats.get('levels', {})}")
        
        store.close()
        
        assert len(errors) == 0, f"Errors occurred: {errors[:5]}"
        assert total_writes > 0, "No writes completed"
        assert total_reads > 0, "No reads completed"
        
        print("\n  PASSED: High concurrency stress test completed")
        
    finally:
        cleanup_test_dir(test_dir)


def main():
    """Run all non-blocking compaction tests."""
    print("\n" + "=" * 70)
    print("NON-BLOCKING BACKGROUND COMPACTION TEST SUITE")
    print("=" * 70)
    
    tests = [
        ("Background Compaction", test_compaction_is_background),
        ("Concurrent Reads During Compaction", test_concurrent_reads_during_compaction),
        ("Data Consistency During Compaction", test_data_consistency_during_compaction),
        ("Manifest Atomic Updates", test_manifest_atomic_update),
        ("High Concurrency Stress", test_high_concurrency_stress),
    ]
    
    results = []
    
    for name, test_func in tests:
        try:
            test_func()
            results.append((name, "PASSED", None))
        except Exception as e:
            import traceback
            results.append((name, "FAILED", str(e)))
            traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for _, status, _ in results if status == "PASSED")
    failed = sum(1 for _, status, _ in results if status == "FAILED")
    
    for name, status, error in results:
        symbol = "✓" if status == "PASSED" else "✗"
        print(f"  {symbol} {name}: {status}")
        if error:
            print(f"      Error: {error}")
    
    print(f"\nTotal: {passed} passed, {failed} failed")
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
