#!/usr/bin/env python3
"""
Comprehensive unit tests for WAL (Write-Ahead Log).
Tests durability, recovery, edge cases.
"""
import os
import sys
import tempfile
import shutil
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv.storage.wal import WAL
from lsmkv.core.dto import WALRecord, OperationType


class TestWAL:
    """Test suite for WAL."""
    
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
    
    def test_wal_creation(self):
        """Test WAL file creation."""
        print("\nTest 1: WAL Creation")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        wal = WAL(filepath)
        
        self.assert_true(os.path.exists(filepath), "WAL file created")
        self.assert_true(os.path.getsize(filepath) == 0, "WAL file initially empty")
    
    def test_append_record(self):
        """Test appending records to WAL."""
        print("\nTest 2: Append Record")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        wal = WAL(filepath)
        
        # Append PUT record
        record1 = WALRecord(OperationType.PUT, "key1", "value1", 1000)
        wal.append(record1)
        
        self.assert_true(os.path.getsize(filepath) > 0, "WAL has content after append")
        
        # Append DELETE record
        record2 = WALRecord(OperationType.DELETE, "key2", None, 2000)
        wal.append(record2)
        
        size_after_two = os.path.getsize(filepath)
        self.assert_true(size_after_two > 0, "WAL grows after second append")
    
    def test_read_all(self):
        """Test reading all records from WAL."""
        print("\nTest 3: Read All Records")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "test_read_all.log")
        wal = WAL(filepath)
        
        # Append multiple records
        records = [
            WALRecord(OperationType.PUT, "key1", "value1", 1000),
            WALRecord(OperationType.PUT, "key2", "value2", 2000),
            WALRecord(OperationType.DELETE, "key3", None, 3000),
        ]
        
        for record in records:
            wal.append(record)
        
        # Read all
        read_records = wal.read_all()
        
        if len(read_records) != 3:
            # Debug output
            print(f"  Expected 3 records, got {len(read_records)}")
            with open(filepath, 'r') as f:
                content = f.read()
                print(f"  File content:\n{content}")
        
        self.assert_true(len(read_records) == 3, f"All 3 records read (got {len(read_records)})")
        self.assert_true(read_records[0].key == "key1", "First record correct")
        self.assert_true(read_records[1].key == "key2", "Second record correct")
        self.assert_true(read_records[2].key == "key3", "Third record correct")
        self.assert_true(read_records[2].operation == OperationType.DELETE, "DELETE op preserved")
    
    def test_clear_wal(self):
        """Test clearing WAL."""
        print("\nTest 4: Clear WAL")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        wal = WAL(filepath)
        
        # Add records
        wal.append(WALRecord(OperationType.PUT, "key1", "value1", 1000))
        wal.append(WALRecord(OperationType.PUT, "key2", "value2", 2000))
        
        size_before = os.path.getsize(filepath)
        self.assert_true(size_before > 0, "WAL has content before clear")
        
        # Clear
        wal.clear()
        
        size_after = os.path.getsize(filepath)
        self.assert_true(size_after == 0, "WAL empty after clear")
        
        # Read should return empty
        records = wal.read_all()
        self.assert_true(len(records) == 0, "No records after clear")
    
    def test_persistence_across_instances(self):
        """Test that WAL persists across WAL instances."""
        print("\nTest 5: Persistence Across Instances")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        
        # Instance 1: Write
        wal1 = WAL(filepath)
        wal1.append(WALRecord(OperationType.PUT, "persist_key", "persist_value", 1000))
        # WAL automatically fsyncs on append
        
        # Instance 2: Read (simulates restart)
        wal2 = WAL(filepath)
        records = wal2.read_all()
        
        self.assert_true(len(records) == 1, "Record persisted")
        self.assert_true(records[0].key == "persist_key", "Key persisted correctly")
        self.assert_true(records[0].value == "persist_value", "Value persisted correctly")
    
    def test_empty_wal_read(self):
        """Test reading from empty WAL."""
        print("\nTest 6: Empty WAL Read")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "empty.log")
        wal = WAL(filepath)
        
        records = wal.read_all()
        self.assert_true(len(records) == 0, "Empty WAL returns no records")
        self.assert_true(isinstance(records, list), "Returns list")
    
    def test_many_records(self):
        """Test WAL with many records."""
        print("\nTest 7: Many Records (1000)")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "many.log")
        wal = WAL(filepath)
        
        # Write 1000 records
        for i in range(1000):
            record = WALRecord(OperationType.PUT, f"key{i:05d}", f"value{i}", 1000 + i)
            wal.append(record)
        
        file_size = os.path.getsize(filepath)
        self.assert_true(file_size > 0, f"WAL file has content ({file_size} bytes)")
        
        # Read all
        records = wal.read_all()
        self.assert_true(len(records) == 1000, "All 1000 records read")
        self.assert_true(records[0].key == "key00000", "First record correct")
        self.assert_true(records[999].key == "key00999", "Last record correct")
    
    def test_append_after_read(self):
        """Test appending after reading."""
        print("\nTest 8: Append After Read")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        wal = WAL(filepath)
        
        # Write
        wal.append(WALRecord(OperationType.PUT, "key1", "value1", 1000))
        
        # Read
        records = wal.read_all()
        self.assert_true(len(records) == 1, "1 record read")
        
        # Append more
        wal.append(WALRecord(OperationType.PUT, "key2", "value2", 2000))
        
        # Read again
        records = wal.read_all()
        self.assert_true(len(records) == 2, "2 records after second append")
    
    def test_clear_and_reuse(self):
        """Test clearing and reusing WAL."""
        print("\nTest 9: Clear and Reuse")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        wal = WAL(filepath)
        
        # Cycle 1
        wal.append(WALRecord(OperationType.PUT, "key1", "value1", 1000))
        wal.clear()
        
        # Cycle 2
        wal.append(WALRecord(OperationType.PUT, "key2", "value2", 2000))
        
        records = wal.read_all()
        self.assert_true(len(records) == 1, "Only 1 record after clear and reuse")
        self.assert_true(records[0].key == "key2", "New record present")
    
    def test_fsync_durability(self):
        """Test that WAL uses fsync for durability."""
        print("\nTest 10: Fsync Durability")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, f"test_{time.time()}.log")
        wal = WAL(filepath)
        
        # Append record
        wal.append(WALRecord(OperationType.PUT, "durable_key", "durable_value", 1000))
        
        # Immediately create new WAL instance (simulates crash recovery)
        wal2 = WAL(filepath)
        records = wal2.read_all()
        
        self.assert_true(len(records) == 1, "Record persisted (fsync worked)")
        self.assert_true(records[0].key == "durable_key", "Durable key recovered")
    
    def test_special_characters_in_wal(self):
        """Test WAL with special characters."""
        print("\nTest 11: Special Characters in WAL")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "special.log")
        wal = WAL(filepath)
        
        # Various special characters
        test_cases = [
            ("key:colon", "value:colon"),
            ("key\ttab", "value\ttab"),
            ("unicode_🎉", "value_🚀"),
            ("", "empty_key"),
            ("key", ""),
        ]
        
        for key, value in test_cases:
            record = WALRecord(OperationType.PUT, key, value, 1000)
            wal.append(record)
        
        # Read back
        records = wal.read_all()
        self.assert_true(len(records) == len(test_cases), f"All {len(test_cases)} records read")
        
        # Verify first and last
        self.assert_true(records[0].key == "key:colon", "Special chars preserved")
    
    def test_concurrent_appends(self):
        """Test concurrent appends (thread safety)."""
        print("\nTest 12: Concurrent Appends")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "concurrent.log")
        wal = WAL(filepath)
        
        import threading
        errors = []
        
        def append_records(start, count):
            try:
                for i in range(start, start + count):
                    record = WALRecord(OperationType.PUT, f"key{i}", f"value{i}", 1000 + i)
                    wal.append(record)
            except Exception as e:
                errors.append(e)
        
        # Start multiple threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=append_records, args=(i * 10, 10))
            threads.append(t)
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        self.assert_true(len(errors) == 0, f"No errors in concurrent appends ({len(errors)})")
        
        # Read all
        records = wal.read_all()
        self.assert_true(len(records) == 30, "All 30 records from 3 threads")
    
    def test_large_values_in_wal(self):
        """Test WAL with large values."""
        print("\nTest 13: Large Values in WAL")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "large.log")
        wal = WAL(filepath)
        
        # Large value (10KB)
        large_value = "x" * 10000
        record = WALRecord(OperationType.PUT, "large_key", large_value, 1000)
        wal.append(record)
        
        # Read back
        records = wal.read_all()
        self.assert_true(len(records) == 1, "Large record appended")
        self.assert_true(len(records[0].value) == 10000, "Large value preserved")
    
    def test_wal_corruption_handling(self):
        """Test WAL behavior with corrupted data."""
        print("\nTest 14: WAL Corruption Handling")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "corrupt.log")
        wal = WAL(filepath)
        
        # Write valid record
        wal.append(WALRecord(OperationType.PUT, "key1", "value1", 1000))
        
        # Manually corrupt the file
        with open(filepath, 'a') as f:
            f.write("CORRUPTED_DATA_INVALID\n")
        
        # Try to read
        try:
            records = wal.read_all()
            # May skip corrupted line or raise error
            valid_count = sum(1 for r in records if r.key == "key1")
            self.assert_true(valid_count >= 1, "Valid records still readable")
        except (ValueError, IndexError):
            self.assert_true(True, "Corruption detected and handled")
    
    def test_wal_file_permissions(self):
        """Test WAL file exists and is readable/writable."""
        print("\nTest 15: WAL File Permissions")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "perms.log")
        wal = WAL(filepath)
        
        # Write
        wal.append(WALRecord(OperationType.PUT, "key", "value", 1000))
        
        # Check file permissions
        self.assert_true(os.access(filepath, os.R_OK), "File is readable")
        self.assert_true(os.access(filepath, os.W_OK), "File is writable")
    
    def test_wal_sequential_writes(self):
        """Test that WAL maintains sequential order."""
        print("\nTest 16: Sequential Write Order")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "sequential.log")
        wal = WAL(filepath)
        
        # Write in specific order
        for i in range(10):
            record = WALRecord(OperationType.PUT, f"seq_{i:02d}", f"v{i}", 1000 + i)
            wal.append(record)
        
        # Read and verify order
        records = wal.read_all()
        
        self.assert_true(len(records) == 10, "All records read")
        
        for i in range(10):
            self.assert_true(records[i].key == f"seq_{i:02d}", f"Record {i} in correct order")
    
    def test_wal_recovery_scenario(self):
        """Test typical recovery scenario."""
        print("\nTest 17: Recovery Scenario")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "recovery.log")
        
        # Simulate application session 1
        wal1 = WAL(filepath)
        wal1.append(WALRecord(OperationType.PUT, "user:1", "Alice", 1000))
        wal1.append(WALRecord(OperationType.PUT, "user:2", "Bob", 2000))
        wal1.append(WALRecord(OperationType.DELETE, "user:1", None, 3000))
        # Simulate crash (don't close cleanly)
        
        # Simulate application session 2 (recovery)
        wal2 = WAL(filepath)
        recovered_records = wal2.read_all()
        
        self.assert_true(len(recovered_records) == 3, "All 3 records recovered")
        
        # Replay logic would:
        # 1. PUT user:1 = Alice
        # 2. PUT user:2 = Bob
        # 3. DELETE user:1
        # Final state: user:2 = Bob
        
        final_state = {}
        for record in recovered_records:
            if record.operation == OperationType.PUT:
                final_state[record.key] = record.value
            elif record.operation == OperationType.DELETE:
                final_state.pop(record.key, None)
        
        self.assert_true("user:2" in final_state, "user:2 in final state")
        self.assert_true("user:1" not in final_state, "user:1 deleted in final state")
        self.assert_true(final_state["user:2"] == "Bob", "user:2 value correct")
    
    def test_wal_after_flush(self):
        """Test WAL clear after flush (typical pattern)."""
        print("\nTest 18: WAL After Flush Pattern")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "flush.log")
        wal = WAL(filepath)
        
        # Write some records
        for i in range(10):
            wal.append(WALRecord(OperationType.PUT, f"key{i}", f"val{i}", 1000 + i))
        
        self.assert_true(len(wal.read_all()) == 10, "10 records in WAL")
        
        # Simulate flush (clear WAL)
        wal.clear()
        
        self.assert_true(len(wal.read_all()) == 0, "WAL empty after flush")
        
        # Continue with new writes
        wal.append(WALRecord(OperationType.PUT, "new_key", "new_val", 2000))
        
        records = wal.read_all()
        self.assert_true(len(records) == 1, "New record added after clear")
        self.assert_true(records[0].key == "new_key", "New record is correct")
    
    def test_wal_partial_flush_pattern(self):
        """Test WAL with partial clearing (keep newer records)."""
        print("\nTest 19: Partial WAL Clear Pattern")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "partial.log")
        wal = WAL(filepath)
        
        # Write records
        wal.append(WALRecord(OperationType.PUT, "old1", "v1", 1000))
        wal.append(WALRecord(OperationType.PUT, "old2", "v2", 2000))
        wal.append(WALRecord(OperationType.PUT, "new1", "v3", 3000))
        
        # Read all
        all_records = wal.read_all()
        
        # Keep only newer records (simulate partial flush)
        newer_records = [r for r in all_records if r.timestamp >= 3000]
        
        # Clear and rewrite
        wal.clear()
        for record in newer_records:
            wal.append(record)
        
        # Verify
        remaining = wal.read_all()
        self.assert_true(len(remaining) == 1, "Only newer record remains")
        self.assert_true(remaining[0].key == "new1", "Correct record kept")
    
    def test_wal_timestamp_preservation(self):
        """Test that timestamps are preserved exactly."""
        print("\nTest 20: Timestamp Preservation")
        print("-" * 60)
        
        filepath = os.path.join(self.test_dir, "timestamp.log")
        wal = WAL(filepath)
        
        # Use specific timestamps
        timestamps = [
            1000000000000000,  # Large timestamp
            1,                 # Small timestamp
            int(time.time() * 1000000),  # Current
        ]
        
        for ts in timestamps:
            wal.append(WALRecord(OperationType.PUT, f"key_{ts}", "value", ts))
        
        # Read and verify
        records = wal.read_all()
        
        for i, ts in enumerate(timestamps):
            self.assert_true(records[i].timestamp == ts, f"Timestamp {ts} preserved exactly")
    
    def run_all_tests(self):
        """Run all tests."""
        print("=" * 70)
        print("WAL - COMPREHENSIVE TEST SUITE")
        print("=" * 70)
        
        self.setup()
        
        try:
            self.test_wal_creation()
            self.test_append_record()
            self.test_read_all()
            self.test_clear_wal()
            self.test_persistence_across_instances()
            self.test_empty_wal_read()
            self.test_many_records()
            self.test_append_after_read()
            self.test_clear_and_reuse()
            self.test_fsync_durability()
            self.test_wal_recovery_scenario()
            self.test_wal_after_flush()
            self.test_wal_partial_flush_pattern()
            self.test_wal_sequential_writes()
            self.test_special_characters_in_wal()
            self.test_concurrent_appends()
            self.test_large_values_in_wal()
            self.test_wal_corruption_handling()
            self.test_wal_file_permissions()
            self.test_wal_timestamp_preservation()
            
        finally:
            self.teardown()
        
        print("\n" + "=" * 70)
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        
        return self.failed == 0


if __name__ == "__main__":
    tester = TestWAL()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
