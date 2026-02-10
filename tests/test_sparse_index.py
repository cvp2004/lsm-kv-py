#!/usr/bin/env python3
"""
Comprehensive unit tests for SparseIndex.
Tests bisect-based operations, floor/ceil, edge cases.
"""
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv.storage.sparse_index import SparseIndex, IndexEntry


class TestSparseIndex:
    """Test suite for SparseIndex."""
    
    def __init__(self):
        self.test_dir = None
        self.passed = 0
        self.failed = 0
    
    def setup(self):
        """Setup test environment."""
        self.test_dir = tempfile.mkdtemp()
    
    def teardown(self):
        """Cleanup test environment."""
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def assert_true(self, condition, message):
        """Assert condition is true."""
        if condition:
            print(f"  ✓ {message}")
            self.passed += 1
        else:
            print(f"  ✗ {message}")
            self.failed += 1
            raise AssertionError(message)
    
    def test_basic_operations(self):
        """Test basic index operations."""
        print("\nTest 1: Basic Index Operations")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        
        # Add entries
        index.add_entry("key_010", 0)
        index.add_entry("key_020", 100)
        index.add_entry("key_030", 200)
        
        self.assert_true(len(index) == 3, "Index has 3 entries")
        self.assert_true(index.block_size == 4, "Block size is 4")
    
    def test_floor_operation(self):
        """Test floor operation (find_block_offset)."""
        print("\nTest 2: Floor Operation (find_block_offset)")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        index.add_entry("key_010", 0)
        index.add_entry("key_020", 100)
        index.add_entry("key_030", 200)
        index.add_entry("key_040", 300)
        
        # Test cases: (search_key, expected_offset)
        test_cases = [
            ("key_005", 0, "Before first"),
            ("key_010", 0, "Exact match first"),
            ("key_015", 0, "Between 010 and 020"),
            ("key_020", 100, "Exact match middle"),
            ("key_025", 100, "Between 020 and 030"),
            ("key_040", 300, "Exact match last"),
            ("key_045", 300, "After last"),
            ("key_999", 300, "Way after last"),
        ]
        
        for key, expected, desc in test_cases:
            result = index.find_block_offset(key)
            self.assert_true(result == expected, f"{desc}: {key} → {result} (expected {expected})")
    
    def test_ceil_operation(self):
        """Test ceil operation (find_ceil_offset)."""
        print("\nTest 3: Ceil Operation (find_ceil_offset)")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        index.add_entry("key_010", 0)
        index.add_entry("key_020", 100)
        index.add_entry("key_030", 200)
        index.add_entry("key_040", 300)
        
        # Test cases
        test_cases = [
            ("key_005", 0, "Before first"),
            ("key_010", 0, "Exact match first"),
            ("key_015", 100, "Between 010 and 020"),
            ("key_020", 100, "Exact match middle"),
            ("key_025", 200, "Between 020 and 030"),
            ("key_040", 300, "Exact match last"),
            ("key_045", None, "After last"),
        ]
        
        for key, expected, desc in test_cases:
            result = index.find_ceil_offset(key)
            self.assert_true(result == expected, f"{desc}: {key} → {result} (expected {expected})")
    
    def test_scan_range(self):
        """Test get_scan_range operation."""
        print("\nTest 4: Scan Range Operation")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        index.add_entry("key_010", 0)
        index.add_entry("key_020", 100)
        index.add_entry("key_030", 200)
        index.add_entry("key_040", 300)
        
        # Test cases
        test_cases = [
            ("key_005", 0, 0, "Before first"),
            ("key_010", 0, 100, "Exact first"),
            ("key_015", 0, 100, "Between 010-020"),
            ("key_020", 100, 200, "Exact middle"),
            ("key_035", 200, 300, "Between 030-040"),
            ("key_040", 300, None, "Exact last"),
            ("key_045", 300, None, "After last"),
        ]
        
        for key, expected_start, expected_end, desc in test_cases:
            start, end = index.get_scan_range(key)
            self.assert_true(
                start == expected_start and end == expected_end,
                f"{desc}: {key} → [{start}, {end}] (expected [{expected_start}, {expected_end}])"
            )
    
    def test_empty_index(self):
        """Test empty index behavior."""
        print("\nTest 5: Empty Index")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        
        self.assert_true(len(index) == 0, "Empty index has length 0")
        self.assert_true(index.find_block_offset("any_key") == 0, "Empty index returns 0 for floor")
        self.assert_true(index.find_ceil_offset("any_key") is None, "Empty index returns None for ceil")
        
        start, end = index.get_scan_range("any_key")
        self.assert_true(start == 0 and end is None, "Empty index scan range is [0, None]")
    
    def test_single_entry_index(self):
        """Test index with single entry."""
        print("\nTest 6: Single Entry Index")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        index.add_entry("key_050", 500)
        
        # Before the entry
        self.assert_true(index.find_block_offset("key_040") == 0, "Floor before entry is 0")
        self.assert_true(index.find_ceil_offset("key_040") == 500, "Ceil before entry is 500")
        
        # Exact match
        self.assert_true(index.find_block_offset("key_050") == 500, "Floor exact match")
        self.assert_true(index.find_ceil_offset("key_050") == 500, "Ceil exact match")
        
        # After the entry
        self.assert_true(index.find_block_offset("key_060") == 500, "Floor after entry is 500")
        self.assert_true(index.find_ceil_offset("key_060") is None, "Ceil after entry is None")
    
    def test_serialization(self):
        """Test to_bytes and from_bytes."""
        print("\nTest 7: Serialization")
        print("-" * 60)
        
        # Create index (using sorted keys for predictable bisect behavior)
        index1 = SparseIndex(block_size=8)
        index1.add_entry("key_010", 0)
        index1.add_entry("key_020", 100)
        index1.add_entry("key_030", 200)
        index1.add_entry("key_040", 300)
        
        # Serialize
        data = index1.to_bytes()
        self.assert_true(len(data) > 0, "Serialization produces data")
        
        # Deserialize
        index2 = SparseIndex.from_bytes(data)
        
        # Verify
        self.assert_true(index2.block_size == 8, "Block size preserved")
        self.assert_true(len(index2) == 4, "Entry count preserved")
        
        # Verify entries
        self.assert_true(index2.find_block_offset("key_010") == 0, "First entry preserved")
        self.assert_true(index2.find_block_offset("key_020") == 100, "Second entry preserved")
        self.assert_true(index2.find_block_offset("key_040") == 300, "Last entry preserved")
    
    def test_file_persistence(self):
        """Test save_to_file and load_from_file."""
        print("\nTest 8: File Persistence")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "index.idx")
        
        # Create and save
        index1 = SparseIndex(block_size=4)
        for i in range(10):
            index1.add_entry(f"key_{i:03d}", i * 100)
        
        index1.save_to_file(filepath)
        
        self.assert_true(os.path.exists(filepath), "Index file created")
        file_size = os.path.getsize(filepath)
        self.assert_true(file_size > 0, f"File has content ({file_size} bytes)")
        
        # Load
        index2 = SparseIndex.load_from_file(filepath)
        
        # Verify
        self.assert_true(len(index2) == 10, "All entries loaded")
        self.assert_true(index2.find_block_offset("key_005") == 500, "Data correct after load")
    
    def test_comparison_operators(self):
        """Test IndexEntry comparison operators."""
        print("\nTest 9: IndexEntry Comparison Operators")
        print("-" * 60)
        
        entry1 = IndexEntry("key_010", 0)
        entry2 = IndexEntry("key_020", 100)
        entry3 = IndexEntry("key_010", 200)  # Same key, different offset
        
        # Test comparisons
        self.assert_true(entry1 < entry2, "entry1 < entry2")
        self.assert_true(entry1 <= entry2, "entry1 <= entry2")
        self.assert_true(entry2 > entry1, "entry2 > entry1")
        self.assert_true(entry2 >= entry1, "entry2 >= entry1")
        self.assert_true(entry1 == entry3, "entry1 == entry3 (same key)")
        
        # Test comparison with strings
        self.assert_true(entry1 < "key_020", "entry < string")
        self.assert_true(entry1 == "key_010", "entry == string")
        self.assert_true(entry2 > "key_010", "entry > string")
    
    def test_large_index(self):
        """Test index with many entries."""
        print("\nTest 10: Large Index (1000 entries)")
        print("-" * 60)
        
        index = SparseIndex(block_size=10)
        
        # Add 1000 entries
        for i in range(1000):
            index.add_entry(f"key_{i:06d}", i * 100)
        
        self.assert_true(len(index) == 1000, "1000 entries added")
        
        # Test lookups (should be fast with bisect)
        import time
        start = time.time()
        
        result1 = index.find_block_offset("key_000500")
        result2 = index.find_block_offset("key_000999")
        
        elapsed = time.time() - start
        
        self.assert_true(result1 == 50000, "Lookup 1 correct")
        self.assert_true(result2 == 99900, "Lookup 2 correct")
        self.assert_true(elapsed < 0.001, f"Lookups fast ({elapsed*1000:.3f}ms)")
    
    def test_block_size_variations(self):
        """Test different block sizes."""
        print("\nTest 11: Block Size Variations")
        print("-" * 60)
        
        # Test different block sizes
        for block_size in [1, 2, 4, 8, 16]:
            index = SparseIndex(block_size=block_size)
            index.add_entry("a", 0)
            index.add_entry("b", 10)
            
            self.assert_true(index.block_size == block_size, f"Block size {block_size} preserved")
    
    def test_edge_case_empty_keys(self):
        """Test edge cases with empty and special keys."""
        print("\nTest 12: Edge Cases - Empty and Special Keys")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        
        # Add empty key
        index.add_entry("", 0)
        index.add_entry("a", 100)
        index.add_entry("z", 200)
        
        self.assert_true(index.find_block_offset("") == 0, "Empty key floor works")
        self.assert_true(index.find_ceil_offset("") == 0, "Empty key ceil works")
        self.assert_true(index.find_block_offset("m") == 100, "Middle key floor works")
    
    def test_duplicate_offsets(self):
        """Test multiple keys with same offset (shouldn't happen but handle it)."""
        print("\nTest 13: Multiple Keys at Same Offset")
        print("-" * 60)
        
        index = SparseIndex(block_size=4)
        index.add_entry("key1", 0)
        index.add_entry("key2", 0)  # Same offset
        index.add_entry("key3", 100)
        
        # Should still work correctly
        self.assert_true(index.find_block_offset("key1") == 0, "Finds first key")
        self.assert_true(index.find_block_offset("key2") == 0, "Finds second key")
    
    def test_serialization_edge_cases(self):
        """Test serialization with edge cases."""
        print("\nTest 14: Serialization Edge Cases")
        print("-" * 60)
        
        # Empty index
        index1 = SparseIndex(block_size=4)
        data1 = index1.to_bytes()
        index1_restored = SparseIndex.from_bytes(data1)
        self.assert_true(len(index1_restored) == 0, "Empty index serialization works")
        
        # Index with special characters
        index2 = SparseIndex(block_size=4)
        index2.add_entry("key:with:colons", 0)
        index2.add_entry("key|with|pipes", 100)
        index2.add_entry("unicode_🎉", 200)
        
        data2 = index2.to_bytes()
        index2_restored = SparseIndex.from_bytes(data2)
        
        self.assert_true(len(index2_restored) == 3, "Special chars index serialized")
        self.assert_true(
            index2_restored.find_block_offset("key:with:colons") == 0,
            "Special chars preserved"
        )
    
    def test_str_representation(self):
        """Test string representation."""
        print("\nTest 15: String Representation")
        print("-" * 60)
        
        index = SparseIndex(block_size=8)
        index.add_entry("key1", 0)
        index.add_entry("key2", 100)
        
        str_repr = str(index)
        self.assert_true("SparseIndex" in str_repr, "Contains class name")
        self.assert_true("block_size=8" in str_repr, "Shows block size")
        self.assert_true("entries=2" in str_repr, "Shows entry count")
        print(f"  String repr: {str_repr}")
    
    def test_bisect_correctness(self):
        """Verify bisect is being used correctly."""
        print("\nTest 16: Bisect Correctness Verification")
        print("-" * 60)
        
        # Create sorted index
        index = SparseIndex(block_size=4)
        keys = [f"key_{i:03d}" for i in range(0, 100, 10)]
        
        for i, key in enumerate(keys):
            index.add_entry(key, i * 1000)
        
        # Test that bisect gives O(log n) performance
        import time
        
        # Warm up
        for _ in range(10):
            index.find_block_offset("key_050")
        
        # Time many lookups
        start = time.time()
        for _ in range(1000):
            index.find_block_offset("key_050")
        elapsed = time.time() - start
        
        avg_time = elapsed / 1000
        self.assert_true(avg_time < 0.0001, f"Bisect is fast ({avg_time*1000000:.1f}µs per lookup)")
    
    def test_index_entry_serialization(self):
        """Test IndexEntry serialization."""
        print("\nTest 17: IndexEntry Serialization")
        print("-" * 60)
        
        # Create entry
        entry1 = IndexEntry("test_key_123", 12345)
        
        # Serialize
        data = entry1.to_bytes()
        self.assert_true(len(data) > 0, "Entry serializes to bytes")
        
        # Deserialize
        entry2, bytes_consumed = IndexEntry.from_bytes(data, 0)
        
        self.assert_true(entry2.key == "test_key_123", "Key preserved")
        self.assert_true(entry2.offset == 12345, "Offset preserved")
        self.assert_true(bytes_consumed == len(data), "Bytes consumed correct")
    
    def test_multiple_entries_serialization(self):
        """Test serialization of multiple entries in sequence."""
        print("\nTest 18: Multiple Entry Serialization")
        print("-" * 60)
        
        entries = [
            IndexEntry("key1", 0),
            IndexEntry("key2", 100),
            IndexEntry("key3", 200),
        ]
        
        # Serialize all
        data = b""
        for entry in entries:
            data += entry.to_bytes()
        
        # Deserialize all
        offset = 0
        restored = []
        while offset < len(data):
            entry, offset = IndexEntry.from_bytes(data, offset)
            restored.append(entry)
        
        self.assert_true(len(restored) == 3, "All entries restored")
        self.assert_true(restored[0].key == "key1", "First entry correct")
        self.assert_true(restored[2].offset == 200, "Last entry correct")
    
    def run_all_tests(self):
        """Run all tests."""
        print("=" * 70)
        print("SPARSE INDEX - COMPREHENSIVE TEST SUITE")
        print("=" * 70)
        
        try:
            self.setup()
            
            self.test_basic_operations()
            self.test_floor_operation()
            self.test_ceil_operation()
            self.test_scan_range()
            self.test_empty_index()
            self.test_single_entry_index()
            self.test_serialization()
            self.test_file_persistence()
            self.test_comparison_operators()
            self.test_large_index()
            self.test_block_size_variations()
            self.test_edge_case_empty_keys()
            self.test_duplicate_offsets()
            self.test_serialization_edge_cases()
            self.test_str_representation()
            self.test_bisect_correctness()
            self.test_index_entry_serialization()
            self.test_multiple_entries_serialization()
            
        finally:
            self.teardown()
        
        print("\n" + "=" * 70)
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        
        return self.failed == 0


if __name__ == "__main__":
    tester = TestSparseIndex()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
