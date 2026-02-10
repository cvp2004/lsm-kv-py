#!/usr/bin/env python3
"""
Comprehensive unit tests for BloomFilter.
Tests all functionality, edge cases, and error conditions.
"""
import os
import sys
import shutil
import tempfile

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv.storage.bloom_filter import BloomFilter


class TestBloomFilter:
    """Test suite for BloomFilter."""
    
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
        """Test basic add and contains operations."""
        print("\nTest 1: Basic Operations")
        print("-" * 60)
        
        bf = BloomFilter(100, 0.01)
        
        # Add items
        bf.add("key1")
        bf.add("key2")
        bf.add("key3")
        
        # Test positive cases
        self.assert_true(bf.might_contain("key1"), "Contains added key1")
        self.assert_true(bf.might_contain("key2"), "Contains added key2")
        self.assert_true(bf.might_contain("key3"), "Contains added key3")
        
        # Test negative cases
        self.assert_true(not bf.might_contain("key4"), "Doesn't contain key4")
        self.assert_true(not bf.might_contain("key5"), "Doesn't contain key5")
        
        # Test __contains__ operator
        self.assert_true("key1" in bf, "__contains__ works for present key")
        self.assert_true("key999" not in bf, "__contains__ works for absent key")
    
    def test_file_backed_filter(self):
        """Test file-backed Bloom filter with mmap."""
        print("\nTest 2: File-Backed Filter")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "test_bloom.bf")
        
        # Create file-backed filter
        bf = BloomFilter(100, 0.01, filepath=filepath)
        
        # Add items
        bf.add("user1")
        bf.add("user2")
        
        # Close to ensure sync
        bf.close()
        
        # Verify file exists
        self.assert_true(os.path.exists(filepath), "Bloom filter file created")
        self.assert_true(os.path.getsize(filepath) > 0, "File has content")
        
        # Load existing filter
        bf2 = BloomFilter.load_from_file(filepath)
        
        # Verify data persisted
        self.assert_true(bf2.might_contain("user1"), "Loaded filter contains user1")
        self.assert_true(bf2.might_contain("user2"), "Loaded filter contains user2")
        self.assert_true(not bf2.might_contain("user3"), "Loaded filter doesn't contain user3")
    
    def test_false_positive_rate(self):
        """Test that false positive rate is within expected bounds."""
        print("\nTest 3: False Positive Rate")
        print("-" * 60)
        
        # Create filter with 1% FPR
        bf = BloomFilter(1000, 0.01)
        
        # Add 1000 items
        for i in range(1000):
            bf.add(f"key{i:04d}")
        
        # Test 1000 items that weren't added
        false_positives = 0
        test_count = 1000
        
        for i in range(1000, 2000):
            if bf.might_contain(f"key{i:04d}"):
                false_positives += 1
        
        fpr = false_positives / test_count
        print(f"  False positive rate: {fpr:.4f} (expected ~0.01)")
        
        # Should be roughly 1% (allow some variance)
        self.assert_true(fpr < 0.05, f"FPR {fpr:.4f} is acceptable (< 5%)")
    
    def test_empty_filter(self):
        """Test empty filter behavior."""
        print("\nTest 4: Empty Filter")
        print("-" * 60)
        
        bf = BloomFilter(100, 0.01)
        
        # Should not contain anything
        self.assert_true(not bf.might_contain("anything"), "Empty filter contains nothing")
        self.assert_true(not bf.might_contain(""), "Empty filter doesn't contain empty string")
    
    def test_special_characters(self):
        """Test filter with special characters and edge cases."""
        print("\nTest 5: Special Characters")
        print("-" * 60)
        
        bf = BloomFilter(100, 0.01)
        
        # Test special keys
        special_keys = [
            "",  # Empty string
            " ",  # Space
            "key with spaces",
            "key:with:colons",
            "key|with|pipes",
            "key\nwith\nnewlines",
            "key\twith\ttabs",
            "unicode_key_🎉",
            "very" * 100,  # Long key
        ]
        
        for key in special_keys:
            bf.add(key)
        
        for key in special_keys:
            self.assert_true(bf.might_contain(key), f"Contains special key: {repr(key[:20])}")
    
    def test_large_capacity(self):
        """Test filter with large capacity."""
        print("\nTest 6: Large Capacity")
        print("-" * 60)
        
        # Large capacity filter
        bf = BloomFilter(100000, 0.01)
        
        # Add many items
        for i in range(1000):
            bf.add(f"item{i:06d}")
        
        # Verify some items
        self.assert_true(bf.might_contain("item000000"), "Contains first item")
        self.assert_true(bf.might_contain("item000500"), "Contains middle item")
        self.assert_true(bf.might_contain("item000999"), "Contains last item")
        self.assert_true(not bf.might_contain("item001000"), "Doesn't contain non-added item")
    
    def test_save_and_load(self):
        """Test save and load functionality."""
        print("\nTest 7: Save and Load")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "save_load.bf")
        
        # Create and populate filter
        bf1 = BloomFilter(100, 0.01)
        test_keys = [f"save_key_{i}" for i in range(10)]
        
        for key in test_keys:
            bf1.add(key)
        
        # Save to file
        bf1.save_to_file(filepath)
        
        self.assert_true(os.path.exists(filepath), "File saved successfully")
        
        # Load from file
        bf2 = BloomFilter.load_from_file(filepath)
        
        # Verify all keys present
        for key in test_keys:
            self.assert_true(bf2.might_contain(key), f"Loaded filter contains {key}")
        
        # Verify negative case
        self.assert_true(not bf2.might_contain("not_saved"), "Loaded filter correct negatives")
    
    def test_copy_template(self):
        """Test copying filter to new file."""
        print("\nTest 8: Copy Template")
        print("-" * 60)
        
        filepath1 = os.path.join(self.test_dir, "original.bf")
        filepath2 = os.path.join(self.test_dir, "copy.bf")
        
        # Create file-backed filter
        bf1 = BloomFilter(100, 0.01, filepath=filepath1)
        bf1.add("original_key")
        
        # Copy to new file
        bf1.save_to_file(filepath2)
        
        self.assert_true(os.path.exists(filepath2), "Copy created")
        
        # Load copy
        bf2 = BloomFilter.load_from_file(filepath2)
        self.assert_true(bf2.might_contain("original_key"), "Copy contains original data")
    
    def test_concurrent_lookups(self):
        """Test multiple lookups don't interfere."""
        print("\nTest 9: Concurrent Lookups")
        print("-" * 60)
        
        bf = BloomFilter(100, 0.01)
        
        # Add items
        for i in range(50):
            bf.add(f"concurrent_{i}")
        
        # Multiple lookups
        results = []
        for i in range(100):
            results.append(bf.might_contain(f"concurrent_{i}"))
        
        # First 50 should be found
        found_count = sum(results[:50])
        self.assert_true(found_count == 50, f"All 50 added keys found ({found_count})")
        
        # Next 50 should mostly not be found (some false positives OK)
        not_found_count = sum(not r for r in results[50:])
        self.assert_true(not_found_count >= 45, f"Most non-added keys rejected ({not_found_count}/50)")
    
    def test_str_representation(self):
        """Test string representation."""
        print("\nTest 10: String Representation")
        print("-" * 60)
        
        bf = BloomFilter(100, 0.01)
        str_repr = str(bf)
        
        self.assert_true("BloomFilter" in str_repr, "String contains class name")
        self.assert_true("100" in str_repr or "capacity" in str_repr, "String shows capacity")
        print(f"  String repr: {str_repr}")
    
    def run_all_tests(self):
        """Run all tests."""
        print("=" * 70)
        print("BLOOM FILTER - COMPREHENSIVE TEST SUITE")
        print("=" * 70)
        
        try:
            self.setup()
            
            self.test_basic_operations()
            self.test_file_backed_filter()
            self.test_false_positive_rate()
            self.test_empty_filter()
            self.test_special_characters()
            self.test_large_capacity()
            self.test_save_and_load()
            self.test_copy_template()
            self.test_concurrent_lookups()
            self.test_str_representation()
            
        finally:
            self.teardown()
        
        print("\n" + "=" * 70)
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        
        return self.failed == 0


if __name__ == "__main__":
    tester = TestBloomFilter()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
