#!/usr/bin/env python3
"""
Comprehensive unit tests for Memtable.
Tests skiplist operations, tombstones, edge cases.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv.storage.memtable import Memtable
from lsmkv.core.dto import Entry


class TestMemtable:
    """Test suite for Memtable."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
    
    def assert_true(self, condition, message):
        """Assert condition is true."""
        if condition:
            print(f"  ✓ {message}")
            self.passed += 1
        else:
            print(f"  ✗ {message}")
            self.failed += 1
            raise AssertionError(message)
    
    def test_memtable_creation(self):
        """Test Memtable creation."""
        print("\nTest 1: Memtable Creation")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        self.assert_true(memtable.max_size == 100, "Max size set correctly")
        self.assert_true(len(memtable) == 0, "New memtable is empty")
        self.assert_true(not memtable.is_full(), "New memtable not full")
    
    def test_put_and_get(self):
        """Test PUT and GET operations."""
        print("\nTest 2: PUT and GET Operations")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # PUT
        entry1 = Entry("key1", "value1", 1000, False)
        memtable.put(entry1)
        
        self.assert_true(len(memtable) == 1, "Size is 1 after PUT")
        
        # GET
        result = memtable.get("key1")
        self.assert_true(result is not None, "GET returns entry")
        self.assert_true(result.value == "value1", "GET returns correct value")
        
        # GET non-existent
        result2 = memtable.get("nonexistent")
        self.assert_true(result2 is None, "GET returns None for missing key")
    
    def test_update_entry(self):
        """Test updating existing entry."""
        print("\nTest 3: Update Entry")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Initial PUT
        entry1 = Entry("update_key", "value1", 1000, False)
        memtable.put(entry1)
        
        # Update
        entry2 = Entry("update_key", "value2", 2000, False)
        memtable.put(entry2)
        
        self.assert_true(len(memtable) == 1, "Size still 1 (updated, not added)")
        
        # GET should return latest
        result = memtable.get("update_key")
        self.assert_true(result.value == "value2", "Latest value returned")
        self.assert_true(result.timestamp == 2000, "Latest timestamp")
    
    def test_delete_tombstone(self):
        """Test deletion with tombstone."""
        print("\nTest 4: Delete with Tombstone")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # PUT
        entry1 = Entry("to_delete", "value", 1000, False)
        memtable.put(entry1)
        
        # DELETE (tombstone)
        tombstone = Entry("to_delete", None, 2000, True)
        memtable.delete(tombstone)
        
        self.assert_true(len(memtable) == 1, "Tombstone counted in size")
        
        # GET returns None for tombstones (by design)
        result = memtable.get("to_delete")
        self.assert_true(result is None, "GET returns None for tombstone")
        
        # But tombstone exists in key_map
        self.assert_true("to_delete" in memtable.key_map, "Tombstone stored")
        self.assert_true(memtable.key_map["to_delete"].is_deleted == True, "Marked as deleted")
    
    def test_is_full(self):
        """Test is_full() method."""
        print("\nTest 5: Is Full Detection")
        print("-" * 60)
        
        memtable = Memtable(max_size=5)
        
        self.assert_true(not memtable.is_full(), "Not full when empty")
        
        # Fill up
        for i in range(4):
            entry = Entry(f"key{i}", f"value{i}", 1000 + i, False)
            memtable.put(entry)
        
        self.assert_true(not memtable.is_full(), "Not full at 4/5")
        
        # Add one more
        entry5 = Entry("key4", "value4", 1004, False)
        memtable.put(entry5)
        
        self.assert_true(memtable.is_full(), "Full at 5/5")
    
    def test_get_all_entries(self):
        """Test get_all_entries returns sorted entries."""
        print("\nTest 6: Get All Entries (Sorted)")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Add in random order
        keys = ["key_c", "key_a", "key_e", "key_b", "key_d"]
        for key in keys:
            entry = Entry(key, f"value_{key}", 1000, False)
            memtable.put(entry)
        
        # Get all (should be sorted)
        all_entries = memtable.get_all_entries()
        
        self.assert_true(len(all_entries) == 5, "All 5 entries returned")
        
        # Verify sorted order
        expected_order = ["key_a", "key_b", "key_c", "key_d", "key_e"]
        for i, expected_key in enumerate(expected_order):
            self.assert_true(all_entries[i].key == expected_key, f"Entry {i} sorted: {expected_key}")
    
    def test_clear(self):
        """Test clearing memtable."""
        print("\nTest 7: Clear Memtable")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Add entries
        for i in range(10):
            entry = Entry(f"key{i}", f"value{i}", 1000 + i, False)
            memtable.put(entry)
        
        self.assert_true(len(memtable) == 10, "10 entries added")
        
        # Clear
        memtable.clear()
        
        self.assert_true(len(memtable) == 0, "Memtable empty after clear")
        self.assert_true(not memtable.is_full(), "Not full after clear")
        self.assert_true(memtable.get("key5") is None, "All entries cleared")
    
    def test_empty_memtable_operations(self):
        """Test operations on empty memtable."""
        print("\nTest 8: Empty Memtable Operations")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        self.assert_true(len(memtable) == 0, "Length is 0")
        self.assert_true(memtable.get("any_key") is None, "GET returns None")
        self.assert_true(len(memtable.get_all_entries()) == 0, "get_all_entries returns empty list")
        self.assert_true(not memtable.is_full(), "Empty not full")
    
    def test_single_entry_memtable(self):
        """Test memtable with single entry."""
        print("\nTest 9: Single Entry Memtable")
        print("-" * 60)
        
        memtable = Memtable(max_size=1)
        
        entry = Entry("only_key", "only_value", 1000, False)
        memtable.put(entry)
        
        self.assert_true(len(memtable) == 1, "Size is 1")
        self.assert_true(memtable.is_full(), "Full with 1 entry at max_size=1")
        
        result = memtable.get("only_key")
        self.assert_true(result.value == "only_value", "Value retrieved")
        
        entries = memtable.get_all_entries()
        self.assert_true(len(entries) == 1, "get_all_entries returns 1")
    
    def test_special_characters_in_memtable(self):
        """Test memtable with special character keys/values."""
        print("\nTest 10: Special Characters")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        special_entries = [
            Entry("", "empty_key", 1000, False),
            Entry("key", "", 1001, False),
            Entry("key:colon", "value:colon", 1002, False),
            Entry("key\ttab", "value\ttab", 1003, False),
            Entry("unicode_🎉", "value_🚀", 1004, False),
            Entry(" space ", " space ", 1005, False),
        ]
        
        for entry in special_entries:
            memtable.put(entry)
        
        self.assert_true(len(memtable) == 6, "All special entries added")
        
        # Retrieve each
        for entry in special_entries:
            result = memtable.get(entry.key)
            self.assert_true(result is not None, f"Found special key: {repr(entry.key[:20])}")
    
    def test_large_values(self):
        """Test memtable with large values."""
        print("\nTest 11: Large Values")
        print("-" * 60)
        
        memtable = Memtable(max_size=10)
        
        # Large value (10KB)
        large_value = "x" * 10000
        entry = Entry("large_key", large_value, 1000, False)
        memtable.put(entry)
        
        result = memtable.get("large_key")
        self.assert_true(len(result.value) == 10000, "Large value stored and retrieved")
    
    def test_concurrent_reads(self):
        """Test concurrent read operations."""
        print("\nTest 12: Concurrent Reads")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Populate
        for i in range(50):
            entry = Entry(f"key{i:03d}", f"value{i}", 1000 + i, False)
            memtable.put(entry)
        
        import threading
        errors = []
        
        def reader(start, count):
            try:
                for i in range(start, start + count):
                    result = memtable.get(f"key{i:03d}")
                    if result is None:
                        errors.append(f"Missing key{i:03d}")
            except Exception as e:
                errors.append(e)
        
        # Start multiple reader threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=reader, args=(i * 10, 10))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        self.assert_true(len(errors) == 0, f"No errors in concurrent reads ({len(errors)})")
    
    def test_memtable_size_tracking(self):
        """Test that size is tracked correctly."""
        print("\nTest 13: Size Tracking")
        print("-" * 60)
        
        memtable = Memtable(max_size=10)
        
        # Add entries one by one
        for i in range(10):
            memtable.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
            expected_size = i + 1
            self.assert_true(len(memtable) == expected_size, f"Size is {expected_size}")
        
        # Update existing (shouldn't change size)
        memtable.put(Entry("k5", "updated", 2000, False))
        self.assert_true(len(memtable) == 10, "Size unchanged after update")
    
    def test_sorted_iteration_order(self):
        """Test that entries are returned in sorted order."""
        print("\nTest 14: Sorted Iteration Order")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Add in reverse order
        for i in range(20, 0, -1):
            entry = Entry(f"key_{i:03d}", f"value_{i}", 1000 + i, False)
            memtable.put(entry)
        
        # Get all (should be sorted ascending)
        all_entries = memtable.get_all_entries()
        
        for i in range(19):
            current_key = all_entries[i].key
            next_key = all_entries[i + 1].key
            self.assert_true(current_key < next_key, f"Entry {i} < Entry {i+1}: {current_key} < {next_key}")
    
    def test_tombstone_handling(self):
        """Test tombstone operations."""
        print("\nTest 15: Tombstone Handling")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Add entry
        entry = Entry("key", "value", 1000, False)
        memtable.put(entry)
        
        # Add tombstone
        tombstone = Entry("key", None, 2000, True)
        memtable.delete(tombstone)
        
        # GET should return tombstone
        # GET returns None for tombstones (by design - get() filters deleted entries)
        result = memtable.get("key")
        self.assert_true(result is None, "GET returns None for tombstone")
        
        # But tombstone exists in key_map
        self.assert_true("key" in memtable.key_map, "Tombstone in key_map")
        
        # get_all_entries should include tombstone
        all_entries = memtable.get_all_entries()
        self.assert_true(len(all_entries) == 1, "Tombstone in all_entries")
        self.assert_true(all_entries[0].is_deleted, "Entry is tombstone")
    
    def test_mixed_operations(self):
        """Test mixed PUT and DELETE operations."""
        print("\nTest 16: Mixed PUT and DELETE")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # PUT key1
        memtable.put(Entry("key1", "v1", 1000, False))
        
        # DELETE key1
        memtable.delete(Entry("key1", None, 2000, True))
        
        # PUT key2
        memtable.put(Entry("key2", "v2", 3000, False))
        
        # PUT key1 again (after delete)
        memtable.put(Entry("key1", "v1_new", 4000, False))
        
        self.assert_true(len(memtable) == 2, "2 keys in memtable")
        
        # key1 should have latest value (not tombstone)
        result1 = memtable.get("key1")
        self.assert_true(result1.value == "v1_new", "Latest value after re-PUT")
        self.assert_true(not result1.is_deleted, "Not deleted after re-PUT")
    
    def test_max_size_enforcement(self):
        """Test that max_size is respected."""
        print("\nTest 17: Max Size Enforcement")
        print("-" * 60)
        
        memtable = Memtable(max_size=5)
        
        # Add exactly max_size entries
        for i in range(5):
            entry = Entry(f"key{i}", f"value{i}", 1000 + i, False)
            memtable.put(entry)
        
        self.assert_true(len(memtable) == 5, "Has 5 entries")
        self.assert_true(memtable.is_full(), "Is full at max_size")
        
        # Can still add more (memtable doesn't prevent, just reports full)
        entry6 = Entry("key5", "value5", 1005, False)
        memtable.put(entry6)
        
        self.assert_true(len(memtable) == 6, "Can add beyond max_size")
        self.assert_true(memtable.is_full(), "Still reports as full")
    
    def test_get_all_entries_with_tombstones(self):
        """Test get_all_entries includes tombstones."""
        print("\nTest 18: Get All Entries with Tombstones")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Mix of live entries and tombstones
        memtable.put(Entry("key1", "v1", 1000, False))
        memtable.delete(Entry("key2", None, 1001, True))
        memtable.put(Entry("key3", "v3", 1002, False))
        memtable.delete(Entry("key4", None, 1003, True))
        
        all_entries = memtable.get_all_entries()
        
        self.assert_true(len(all_entries) == 4, "All 4 entries (including tombstones)")
        
        tombstone_count = sum(1 for e in all_entries if e.is_deleted)
        self.assert_true(tombstone_count == 2, "2 tombstones in results")
    
    def test_empty_key_and_value(self):
        """Test empty keys and values."""
        print("\nTest 19: Empty Keys and Values")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Empty key
        entry1 = Entry("", "value", 1000, False)
        memtable.put(entry1)
        result1 = memtable.get("")
        self.assert_true(result1 is not None, "Empty key can be stored")
        self.assert_true(result1.value == "value", "Empty key value correct")
        
        # Empty value
        entry2 = Entry("key", "", 1001, False)
        memtable.put(entry2)
        result2 = memtable.get("key")
        self.assert_true(result2.value == "", "Empty value can be stored")
    
    def test_unicode_keys_and_values(self):
        """Test Unicode keys and values."""
        print("\nTest 20: Unicode Keys and Values")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        # Unicode in key and value
        entry1 = Entry("key_🎉", "value_🚀", 1000, False)
        memtable.put(entry1)
        
        result = memtable.get("key_🎉")
        self.assert_true(result is not None, "Unicode key found")
        self.assert_true(result.value == "value_🚀", "Unicode value correct")
        
        # Chinese characters
        entry2 = Entry("键_key", "值_value", 1001, False)
        memtable.put(entry2)
        
        result2 = memtable.get("键_key")
        self.assert_true(result2 is not None, "Chinese key found")
    
    def test_clear_and_reuse(self):
        """Test clearing and reusing memtable."""
        print("\nTest 21: Clear and Reuse")
        print("-" * 60)
        
        memtable = Memtable(max_size=10)
        
        # Cycle 1
        for i in range(10):
            memtable.put(Entry(f"cycle1_{i}", f"v{i}", 1000 + i, False))
        
        self.assert_true(len(memtable) == 10, "Cycle 1: 10 entries")
        
        # Clear
        memtable.clear()
        
        # Cycle 2
        for i in range(5):
            memtable.put(Entry(f"cycle2_{i}", f"v{i}", 2000 + i, False))
        
        self.assert_true(len(memtable) == 5, "Cycle 2: 5 entries")
        self.assert_true(memtable.get("cycle1_0") is None, "Old data cleared")
        self.assert_true(memtable.get("cycle2_0") is not None, "New data present")
    
    def test_len_operator(self):
        """Test __len__ operator."""
        print("\nTest 22: __len__ Operator")
        print("-" * 60)
        
        memtable = Memtable(max_size=100)
        
        self.assert_true(len(memtable) == 0, "len() works on empty")
        
        memtable.put(Entry("k1", "v1", 1000, False))
        self.assert_true(len(memtable) == 1, "len() works with 1 entry")
        
        for i in range(2, 11):
            memtable.put(Entry(f"k{i}", f"v{i}", 1000 + i, False))
        
        self.assert_true(len(memtable) == 10, "len() works with 10 entries")
    
    def run_all_tests(self):
        """Run all tests."""
        print("=" * 70)
        print("MEMTABLE - COMPREHENSIVE TEST SUITE")
        print("=" * 70)
        
        self.test_memtable_creation()
        self.test_put_and_get()
        self.test_update_entry()
        self.test_delete_tombstone()
        self.test_is_full()
        self.test_get_all_entries()
        self.test_clear()
        self.test_empty_memtable_operations()
        self.test_single_entry_memtable()
        self.test_special_characters_in_memtable()
        self.test_large_values()
        self.test_concurrent_reads()
        self.test_memtable_size_tracking()
        self.test_sorted_iteration_order()
        self.test_tombstone_handling()
        self.test_mixed_operations()
        self.test_max_size_enforcement()
        self.test_get_all_entries_with_tombstones()
        self.test_empty_key_and_value()
        self.test_unicode_keys_and_values()
        self.test_clear_and_reuse()
        self.test_len_operator()
        
        print("\n" + "=" * 70)
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        
        return self.failed == 0


if __name__ == "__main__":
    tester = TestMemtable()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
