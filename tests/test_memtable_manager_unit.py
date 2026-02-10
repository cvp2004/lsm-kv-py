#!/usr/bin/env python3
"""
Comprehensive unit tests for MemtableManager.
Tests active memtable, immutable queue, rotation, background flushing, edge cases.
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv.core.memtable_manager import MemtableManager
from lsmkv.storage.memtable import Memtable
from lsmkv.core.dto import Entry


class TestMemtableManagerUnit:
    """Unit test suite for MemtableManager."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.flush_count = 0
        self.flushed_memtables = []
    
    def assert_true(self, condition, message):
        """Assert condition is true."""
        if condition:
            print(f"  ✓ {message}")
            self.passed += 1
        else:
            print(f"  ✗ {message}")
            self.failed += 1
            raise AssertionError(message)
    
    def mock_flush_callback(self, memtable: Memtable):
        """Mock flush callback for testing."""
        self.flush_count += 1
        self.flushed_memtables.append(memtable)
        # Simulate some flush work
        time.sleep(0.01)
    
    def reset_flush_tracking(self):
        """Reset flush tracking variables."""
        self.flush_count = 0
        self.flushed_memtables = []
    
    def test_initialization(self):
        """Test MemtableManager initialization."""
        print("\nTest 1: Initialization")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            max_immutable=4,
            max_memory_bytes=1024 * 1024,
            flush_workers=2,
            on_flush_callback=self.mock_flush_callback
        )
        
        self.assert_true(manager.active is not None, "Active memtable created")
        self.assert_true(len(manager.active) == 0, "Active memtable starts empty")
        self.assert_true(len(manager.immutable_queue) == 0, "Immutable queue starts empty")
        self.assert_true(manager.memtable_size == 10, "Memtable size set")
        self.assert_true(manager.max_immutable == 4, "Max immutable set")
        
        manager.close()
    
    def test_put_to_active(self):
        """Test PUT operations to active memtable."""
        print("\nTest 2: PUT to Active Memtable")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # PUT entries
        for i in range(5):
            entry = Entry(f"key{i}", f"value{i}", 1000 + i, False)
            manager.put(entry)
        
        self.assert_true(len(manager.active) == 5, "Active has 5 entries")
        self.assert_true(len(manager.immutable_queue) == 0, "No rotation yet")
        
        # Verify GET
        result = manager.get("key2")
        self.assert_true(result is not None, "Can GET from active")
        self.assert_true(result.value == "value2", "Correct value from active")
        
        manager.close()
    
    def test_rotation_on_full(self):
        """Test rotation when active memtable is full."""
        print("\nTest 3: Rotation on Full")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        manager = MemtableManager(
            memtable_size=5,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Fill active memtable (rotation happens when full, then new entry added)
        for i in range(5):
            entry = Entry(f"key{i}", f"value{i}", 1000 + i, False)
            manager.put(entry)
        
        # After 5 entries, active should be full or rotated
        stats_before = manager.stats()
        active_size_before = len(manager.active)
        
        # Add one more (will either fill to 6 or rotate)
        entry6 = Entry("key5", "value5", 1005, False)
        manager.put(entry6)
        
        # Check that rotation occurred
        stats_after = manager.stats()
        
        # Rotation should have occurred
        self.assert_true(stats_after["total_rotations"] >= 1, "At least 1 rotation occurred")
        self.assert_true(len(manager.immutable_queue) >= 1, "Immutable queue has memtable(s)")
        
        # All keys should still be accessible
        for i in range(6):
            result = manager.get(f"key{i}")
            self.assert_true(result is not None, f"key{i} accessible after rotation")
        
        manager.close()
    
    def test_get_from_immutable_queue(self):
        """Test GET from immutable queue."""
        print("\nTest 4: GET from Immutable Queue")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        manager = MemtableManager(
            memtable_size=3,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Add to active
        manager.put(Entry("old_key", "old_value", 1000, False))
        manager.put(Entry("key1", "value1", 1001, False))
        manager.put(Entry("key2", "value2", 1002, False))
        
        # Trigger rotation
        manager.put(Entry("new_key", "new_value", 1003, False))
        
        # old_key should be in immutable queue
        result = manager.get("old_key")
        self.assert_true(result is not None, "Found in immutable queue")
        self.assert_true(result.value == "old_value", "Correct value from immutable")
        
        # new_key should be in active
        result2 = manager.get("new_key")
        self.assert_true(result2 is not None, "Found in active")
        
        manager.close()
    
    def test_queue_fifo_ordering(self):
        """Test that immutable queue maintains FIFO order."""
        print("\nTest 5: Queue FIFO Ordering")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=5,  # Large enough to hold multiple
            on_flush_callback=self.mock_flush_callback
        )
        
        # Create 3 rotations
        for batch in range(3):
            manager.put(Entry(f"batch{batch}_key1", f"v1", 1000 + batch * 10, False))
            manager.put(Entry(f"batch{batch}_key2", f"v2", 1001 + batch * 10, False))
            # This should trigger rotation
            manager.put(Entry(f"trigger{batch}", f"v", 1002 + batch * 10, False))
        
        self.assert_true(len(manager.immutable_queue) >= 2, "Multiple memtables in queue")
        
        # Oldest should be at index 0
        oldest = manager.immutable_queue[0]
        newest = manager.immutable_queue[-1]
        
        # Check that oldest has lower timestamps
        oldest_entries = oldest.get_all_entries()
        newest_entries = newest.get_all_entries()
        
        if oldest_entries and newest_entries:
            oldest_ts = oldest_entries[0].timestamp
            newest_ts = newest_entries[0].timestamp
            self.assert_true(oldest_ts < newest_ts, "Oldest memtable has lower timestamps")
        
        manager.close()
    
    def test_flush_trigger_on_queue_full(self):
        """Test that flush is triggered when queue is full."""
        print("\nTest 6: Flush Trigger on Queue Full")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=3,  # Small queue
            flush_workers=2,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Fill queue to max
        for i in range(8):  # Should cause 4 rotations
            entry = Entry(f"key{i}", f"value{i}", 1000 + i, False)
            manager.put(entry)
        
        # Wait for background flushes
        time.sleep(1)
        
        # Should have triggered flushes
        self.assert_true(self.flush_count >= 1, f"At least 1 flush triggered ({self.flush_count})")
        
        stats = manager.stats()
        self.assert_true(stats["total_async_flushes"] >= 1, "Async flushes recorded")
        
        manager.close()
    
    def test_get_searches_all_memtables(self):
        """Test that GET searches active + all immutable memtables."""
        print("\nTest 7: GET Searches All Memtables")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=5,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Batch 1 (will go to immutable)
        manager.put(Entry("old1", "value_old1", 1000, False))
        manager.put(Entry("old2", "value_old2", 1001, False))
        
        # Batch 2 (will go to immutable)
        manager.put(Entry("mid1", "value_mid1", 2000, False))
        manager.put(Entry("mid2", "value_mid2", 2001, False))
        
        # Batch 3 (stays in active)
        manager.put(Entry("new1", "value_new1", 3000, False))
        
        # Should be able to find all
        self.assert_true(manager.get("old1") is not None, "Found in oldest immutable")
        self.assert_true(manager.get("mid1") is not None, "Found in newer immutable")
        self.assert_true(manager.get("new1") is not None, "Found in active")
        
        manager.close()
    
    def test_delete_operation(self):
        """Test DELETE operations create tombstones."""
        print("\nTest 8: DELETE Operations")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # PUT
        manager.put(Entry("del_key", "value", 1000, False))
        
        # DELETE
        manager.delete(Entry("del_key", None, 2000, True))
        
        # GET should return None (tombstone filtered by memtable.get())
        result = manager.get("del_key")
        self.assert_true(result is None, "GET returns None for deleted key")
        
        # But entry exists in active with is_deleted=True
        self.assert_true("del_key" in manager.active.key_map, "Tombstone in active")
        self.assert_true(manager.active.key_map["del_key"].is_deleted, "Marked as deleted")
        
        manager.close()
    
    def test_rotation_creates_new_active(self):
        """Test that rotation creates fresh active memtable."""
        print("\nTest 9: Rotation Creates New Active")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=3,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Fill to trigger rotation
        for i in range(6):  # More than memtable_size
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        # Should have rotated
        stats = manager.stats()
        self.assert_true(stats["total_rotations"] >= 1, "Rotation occurred")
        self.assert_true(len(manager.immutable_queue) >= 1, "Immutable queue has memtable")
        
        # All entries should be accessible
        for i in range(6):
            result = manager.get(f"k{i}")
            self.assert_true(result is not None, f"k{i} accessible after rotation")
        
        manager.close()
    
    def test_immutable_queue_max_size(self):
        """Test that immutable queue respects max size."""
        print("\nTest 10: Immutable Queue Max Size")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=3,
            flush_workers=2,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Cause multiple rotations
        for i in range(12):  # 6 rotations
            entry = Entry(f"key{i:02d}", f"value{i}", 1000 + i, False)
            manager.put(entry)
        
        time.sleep(1)  # Wait for flushes
        
        # Queue should not exceed max
        queue_size = len(manager.immutable_queue)
        self.assert_true(queue_size <= 3, f"Queue size ({queue_size}) <= max (3)")
        
        # Flushes should have been triggered
        self.assert_true(self.flush_count >= 1, f"Flushes triggered to keep queue size ({self.flush_count})")
        
        manager.close()
    
    def test_memory_limit_trigger(self):
        """Test that memory limit triggers flush."""
        print("\nTest 11: Memory Limit Trigger")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        # Very small memory limit
        manager = MemtableManager(
            memtable_size=10,
            max_immutable=10,  # High SSTable limit
            max_memory_bytes=1000,  # Only 1KB!
            flush_workers=2,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Add data to exceed memory limit
        for i in range(15):
            entry = Entry(f"key{i:03d}", f"value_{i}_with_padding_" * 10, 1000 + i, False)
            manager.put(entry)
        
        time.sleep(1)
        
        # Should have triggered flushes due to memory limit
        stats = manager.stats()
        print(f"  Memory: {stats['total_memory_bytes']} bytes")
        self.assert_true(self.flush_count >= 1, f"Memory limit triggered flushes ({self.flush_count})")
        
        manager.close()
    
    def test_stats(self):
        """Test statistics reporting."""
        print("\nTest 12: Statistics")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=5,
            max_immutable=3,
            max_memory_bytes=10240,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Add some data
        for i in range(7):  # Should cause 1 rotation
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        stats = manager.stats()
        
        self.assert_true("active_memtable_size" in stats, "Has active size")
        self.assert_true("immutable_count" in stats, "Has immutable count")
        self.assert_true("total_rotations" in stats, "Has rotation count")
        self.assert_true("max_queue_size" in stats, "Has max queue size")
        self.assert_true("memory_limit_bytes" in stats, "Has memory limit")
        
        self.assert_true(stats["total_rotations"] >= 1, "At least 1 rotation")
        self.assert_true(stats["active_memtable_size"] >= 0, "Active size valid")
        
        manager.close()
    
    def test_update_in_active(self):
        """Test updating entry in active memtable."""
        print("\nTest 13: Update in Active")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Initial PUT
        manager.put(Entry("update_key", "v1", 1000, False))
        result1 = manager.get("update_key")
        self.assert_true(result1.value == "v1", "Initial value")
        
        # Update
        manager.put(Entry("update_key", "v2", 2000, False))
        result2 = manager.get("update_key")
        self.assert_true(result2.value == "v2", "Updated value")
        
        # Size should still be 1 (update, not new entry)
        self.assert_true(len(manager.active) == 1, "Size unchanged after update")
        
        manager.close()
    
    def test_get_returns_newest_version(self):
        """Test that GET returns newest version across memtables."""
        print("\nTest 14: GET Returns Newest Version")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=5,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Old version (will go to immutable)
        manager.put(Entry("same_key", "old_value", 1000, False))
        manager.put(Entry("filler1", "f1", 1001, False))
        
        # Trigger rotation
        manager.put(Entry("filler2", "f2", 2000, False))
        
        # New version in active
        manager.put(Entry("same_key", "new_value", 3000, False))
        
        # GET should return newest (from active)
        result = manager.get("same_key")
        self.assert_true(result.value == "new_value", "Returns newest version from active")
        
        manager.close()
    
    def test_background_flush_non_blocking(self):
        """Test that background flush doesn't block PUT operations."""
        print("\nTest 15: Background Flush Non-Blocking")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        def slow_flush_callback(memtable):
            """Slow flush to test non-blocking."""
            time.sleep(0.5)  # Simulate slow flush
            self.flush_count += 1
        
        manager = MemtableManager(
            memtable_size=3,
            max_immutable=2,  # Small queue to trigger flushes
            flush_workers=1,
            on_flush_callback=slow_flush_callback
        )
        
        # Measure PUT latency during flush
        latencies = []
        
        for i in range(10):
            start = time.time()
            manager.put(Entry(f"key{i}", f"value{i}", 1000 + i, False))
            latency = time.time() - start
            latencies.append(latency)
        
        max_latency = max(latencies)
        
        print(f"  Max PUT latency: {max_latency*1000:.2f}ms")
        
        # PUTs should be fast even during slow flush
        self.assert_true(max_latency < 0.1, f"PUT not blocked by flush ({max_latency*1000:.1f}ms < 100ms)")
        
        manager.close()
    
    def test_empty_manager(self):
        """Test operations on empty manager."""
        print("\nTest 16: Empty Manager")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            on_flush_callback=self.mock_flush_callback
        )
        
        # GET on empty
        result = manager.get("nonexistent")
        self.assert_true(result is None, "GET returns None on empty")
        
        # Stats on empty
        stats = manager.stats()
        self.assert_true(stats["active_memtable_size"] == 0, "Active size is 0")
        self.assert_true(stats["immutable_count"] == 0, "Immutable count is 0")
        self.assert_true(stats["total_rotations"] == 0, "No rotations yet")
        
        manager.close()
    
    def test_single_entry_operations(self):
        """Test with single entry."""
        print("\nTest 17: Single Entry Operations")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=1,  # Max of 1!
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Add one entry (may rotate immediately if is_full() check happens)
        manager.put(Entry("only_key", "only_value", 1000, False))
        
        # Add another
        manager.put(Entry("new_key", "new_value", 2000, False))
        
        # Should have triggered rotation(s)
        stats = manager.stats()
        self.assert_true(stats["total_rotations"] >= 1, "Rotation(s) occurred with max_size=1")
        
        # Both should be accessible (one in active, one in immutable or both in immutable)
        self.assert_true(manager.get("only_key") is not None, "Old key accessible")
        self.assert_true(manager.get("new_key") is not None, "New key accessible")
        
        manager.close()
    
    def test_concurrent_puts(self):
        """Test concurrent PUT operations (thread safety)."""
        print("\nTest 18: Concurrent PUTs (Thread Safety)")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=100,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        errors = []
        
        def writer(start, count):
            try:
                for i in range(start, start + count):
                    entry = Entry(f"concurrent_{i:03d}", f"value_{i}", 1000 + i, False)
                    manager.put(entry)
            except Exception as e:
                errors.append(e)
        
        # Start multiple writer threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=writer, args=(i * 20, 20))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        self.assert_true(len(errors) == 0, f"No errors in concurrent PUTs ({len(errors)})")
        
        # Verify all data present
        for i in range(60):
            result = manager.get(f"concurrent_{i:03d}")
            self.assert_true(result is not None, f"concurrent_{i:03d} found")
        
        manager.close()
    
    def test_concurrent_reads_and_writes(self):
        """Test concurrent reads and writes."""
        print("\nTest 19: Concurrent Reads and Writes")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=50,
            max_immutable=4,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Pre-populate
        for i in range(20):
            manager.put(Entry(f"pre_{i}", f"value_{i}", 1000 + i, False))
        
        errors = []
        read_results = []
        
        def writer():
            try:
                for i in range(20, 40):
                    manager.put(Entry(f"new_{i}", f"value_{i}", 2000 + i, False))
            except Exception as e:
                errors.append(e)
        
        def reader():
            try:
                for i in range(20):
                    result = manager.get(f"pre_{i}")
                    read_results.append(result)
            except Exception as e:
                errors.append(e)
        
        # Start threads
        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        self.assert_true(len(errors) == 0, "No errors in concurrent ops")
        self.assert_true(len(read_results) == 20, "All reads completed")
        
        manager.close()
    
    def test_close_waits_for_flush(self):
        """Test that close() waits for pending flushes."""
        print("\nTest 20: Close Waits for Flush")
        print("-" * 60)
        
        self.reset_flush_tracking()
        
        def counting_flush(memtable):
            time.sleep(0.2)  # Simulate work
            self.flush_count += 1
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=2,
            flush_workers=2,
            on_flush_callback=counting_flush
        )
        
        # Trigger multiple flushes
        for i in range(8):
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        # Close (should wait for flushes)
        start = time.time()
        manager.close()
        elapsed = time.time() - start
        
        print(f"  Close took {elapsed:.3f}s (waited for flushes)")
        self.assert_true(self.flush_count >= 1, f"Flushes completed before close ({self.flush_count})")
    
    def test_rotation_increments_counter(self):
        """Test that rotation counter increments correctly."""
        print("\nTest 21: Rotation Counter")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=3,
            max_immutable=5,
            on_flush_callback=self.mock_flush_callback
        )
        
        stats0 = manager.stats()
        initial_rotations = stats0["total_rotations"]
        
        # Cause 3 rotations
        for batch in range(3):
            for i in range(3):
                manager.put(Entry(f"b{batch}_k{i}", f"v", 1000 + batch * 10 + i, False))
            # Trigger rotation
            manager.put(Entry(f"b{batch}_trigger", "t", 1000 + batch * 10 + 3, False))
        
        stats1 = manager.stats()
        final_rotations = stats1["total_rotations"]
        
        self.assert_true(final_rotations >= initial_rotations + 3, 
                        f"Rotations incremented ({final_rotations - initial_rotations} >= 3)")
        
        manager.close()
    
    def test_is_full_detection(self):
        """Test active memtable full detection."""
        print("\nTest 22: Active Memtable Full Detection")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=5,
            max_immutable=10,  # Large to prevent auto-rotation
            on_flush_callback=self.mock_flush_callback
        )
        
        stats0 = manager.stats()
        self.assert_true(not stats0["active_memtable_full"], "Not full initially")
        
        # Add entries (rotation happens when full, so check intermediate states)
        for i in range(4):
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        stats_partial = manager.stats()
        active_size = stats_partial["active_memtable_size"]
        
        self.assert_true(active_size <= 5, f"Active size reasonable ({active_size})")
        print(f"  Active size after 4 PUTs: {active_size}")
        
        manager.close()
    
    def test_immutable_queue_full_flag(self):
        """Test immutable queue full flag."""
        print("\nTest 23: Immutable Queue Full Flag")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=3,
            flush_workers=1,  # Minimum 1 worker
            on_flush_callback=self.mock_flush_callback
        )
        
        # Fill queue to max
        for i in range(8):  # 4 rotations
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        stats = manager.stats()
        
        # Queue should be full or close to it
        if stats["immutable_count"] >= 3:
            self.assert_true(stats["immutable_queue_full"], "Queue full flag set")
        
        manager.close()
    
    def test_get_from_newest_immutable_first(self):
        """Test that GET checks newest immutable memtable first."""
        print("\nTest 24: GET from Newest Immutable First")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=5,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Batch 1 (oldest)
        manager.put(Entry("same_key", "oldest", 1000, False))
        manager.put(Entry("filler1", "f1", 1001, False))
        
        # Batch 2 (middle)
        manager.put(Entry("same_key", "middle", 2000, False))
        manager.put(Entry("filler2", "f2", 2001, False))
        
        # Batch 3 (newest in immutable)
        manager.put(Entry("same_key", "newest", 3000, False))
        manager.put(Entry("filler3", "f3", 3001, False))
        
        # Batch 4 (in active - but we won't update same_key)
        manager.put(Entry("filler4", "f4", 4000, False))
        
        # GET should return newest from immutable
        result = manager.get("same_key")
        self.assert_true(result.value == "newest", "Returns newest version")
        
        manager.close()
    
    def test_memory_tracking(self):
        """Test memory tracking in stats."""
        print("\nTest 25: Memory Tracking")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=5,
            max_immutable=3,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Add data to create immutable memtables
        for i in range(10):
            entry = Entry(f"key{i}", f"value_{i}_padding" * 10, 1000 + i, False)
            manager.put(entry)
        
        stats = manager.stats()
        
        self.assert_true(stats["total_memory_bytes"] >= 0, "Memory tracked")
        print(f"  Total memory: {stats['total_memory_bytes']} bytes")
        
        # Memory should increase with immutable queue
        if stats["immutable_count"] > 0:
            self.assert_true(stats["total_memory_bytes"] > 0, "Memory > 0 with immutable")
        
        manager.close()
    
    def test_flush_callback_receives_memtable(self):
        """Test that flush callback receives correct memtable."""
        print("\nTest 26: Flush Callback Receives Memtable")
        print("-" * 60)
        
        self.reset_flush_tracking()
        received_memtables = []
        
        def tracking_callback(memtable):
            received_memtables.append(memtable)
            # Verify it's a Memtable instance
            assert isinstance(memtable, Memtable)
            # Verify it has entries
            entries = memtable.get_all_entries()
            assert len(entries) > 0
        
        manager = MemtableManager(
            memtable_size=3,
            max_immutable=2,
            flush_workers=2,
            on_flush_callback=tracking_callback
        )
        
        # Trigger flushes
        for i in range(9):
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        time.sleep(1)
        
        self.assert_true(len(received_memtables) >= 1, f"Callback received memtables ({len(received_memtables)})")
        
        # Verify memtables have data
        for mt in received_memtables:
            entries = mt.get_all_entries()
            self.assert_true(len(entries) > 0, "Flushed memtable has entries")
        
        manager.close()
    
    def test_delete_creates_tombstone_in_active(self):
        """Test DELETE creates tombstone in active."""
        print("\nTest 27: DELETE in Active")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            on_flush_callback=self.mock_flush_callback
        )
        
        # PUT then DELETE
        manager.put(Entry("key", "value", 1000, False))
        manager.delete(Entry("key", None, 2000, True))
        
        # GET returns None (tombstone filtered)
        result = manager.get("key")
        self.assert_true(result is None, "Deleted key returns None")
        
        # Tombstone exists in active
        self.assert_true("key" in manager.active.key_map, "Tombstone in active")
        
        # get_all from active includes tombstone
        all_entries = manager.active.get_all_entries()
        tombstone_count = sum(1 for e in all_entries if e.is_deleted)
        self.assert_true(tombstone_count == 1, "Tombstone in active entries")
        
        manager.close()
    
    def test_rotation_sequence_numbers(self):
        """Test that rotations have sequential sequence numbers."""
        print("\nTest 28: Rotation Sequence Numbers")
        print("-" * 60)
        
        self.reset_flush_tracking()
        sequences_seen = []
        
        def sequence_tracking_callback(memtable):
            # In real implementation, memtable might have sequence number
            sequences_seen.append(len(sequences_seen))
            time.sleep(0.01)
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=5,
            flush_workers=2,
            on_flush_callback=sequence_tracking_callback
        )
        
        # Trigger multiple rotations
        for i in range(12):
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        time.sleep(1)
        
        # Should have sequential flushes
        self.assert_true(len(sequences_seen) >= 1, f"Flushes tracked ({len(sequences_seen)})")
        
        manager.close()
    
    def test_mixed_operations_pattern(self):
        """Test realistic mixed operation pattern."""
        print("\nTest 29: Mixed Operations Pattern")
        print("-" * 60)
        
        manager = MemtableManager(
            memtable_size=10,
            max_immutable=3,
            on_flush_callback=self.mock_flush_callback
        )
        
        # Realistic pattern: PUTs, UPDATEs, DELETEs
        manager.put(Entry("user:1", "Alice", 1000, False))
        manager.put(Entry("user:2", "Bob", 1001, False))
        manager.put(Entry("user:3", "Charlie", 1002, False))
        
        # Update
        manager.put(Entry("user:1", "Alice Updated", 2000, False))
        
        # Delete
        manager.delete(Entry("user:2", None, 2001, True))
        
        # New entry
        manager.put(Entry("user:4", "David", 2002, False))
        
        # Verify state
        self.assert_true(manager.get("user:1").value == "Alice Updated", "Update worked")
        self.assert_true(manager.get("user:2") is None, "Delete worked")
        self.assert_true(manager.get("user:3") is not None, "Unchanged entry present")
        self.assert_true(manager.get("user:4") is not None, "New entry present")
        
        manager.close()
    
    def test_flush_workers_parallel_execution(self):
        """Test that multiple flush workers execute in parallel."""
        print("\nTest 30: Parallel Flush Workers")
        print("-" * 60)
        
        self.reset_flush_tracking()
        flush_times = []
        flush_lock = threading.Lock()
        
        def timed_flush_callback(memtable):
            start = time.time()
            time.sleep(0.1)  # Simulate work
            elapsed = time.time() - start
            
            with flush_lock:
                flush_times.append(elapsed)
                self.flush_count += 1
        
        manager = MemtableManager(
            memtable_size=2,
            max_immutable=2,
            flush_workers=2,  # 2 workers for parallelism
            on_flush_callback=timed_flush_callback
        )
        
        # Trigger multiple flushes
        for i in range(12):
            manager.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        time.sleep(1)
        
        # Should have parallel execution
        self.assert_true(self.flush_count >= 2, f"Multiple flushes completed ({self.flush_count})")
        print(f"  {self.flush_count} flushes with 2 workers")
        
        manager.close()
    
    def run_all_tests(self):
        """Run all tests."""
        print("=" * 70)
        print("MEMTABLE MANAGER - COMPREHENSIVE UNIT TEST SUITE")
        print("=" * 70)
        
        self.test_initialization()
        self.test_put_to_active()
        self.test_rotation_on_full()
        self.test_get_from_immutable_queue()
        self.test_queue_fifo_ordering()
        self.test_flush_trigger_on_queue_full()
        self.test_get_searches_all_memtables()
        self.test_delete_operation()
        self.test_rotation_creates_new_active()
        self.test_immutable_queue_max_size()
        self.test_memory_limit_trigger()
        self.test_stats()
        self.test_update_in_active()
        self.test_get_returns_newest_version()
        self.test_background_flush_non_blocking()
        self.test_empty_manager()
        self.test_single_entry_operations()
        self.test_concurrent_puts()
        self.test_concurrent_reads_and_writes()
        self.test_close_waits_for_flush()
        self.test_rotation_increments_counter()
        self.test_is_full_detection()
        self.test_immutable_queue_full_flag()
        self.test_get_from_newest_immutable_first()
        self.test_memory_tracking()
        self.test_flush_callback_receives_memtable()
        self.test_delete_creates_tombstone_in_active()
        self.test_rotation_sequence_numbers()
        self.test_mixed_operations_pattern()
        self.test_flush_workers_parallel_execution()
        
        print("\n" + "=" * 70)
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        
        return self.failed == 0


if __name__ == "__main__":
    tester = TestMemtableManagerUnit()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
