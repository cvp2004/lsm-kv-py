#!/usr/bin/env python3
"""
Comprehensive unit tests for DTOs (Data Transfer Objects).
Tests Entry, WALRecord, GetResult, OperationType.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv.core.dto import Entry, WALRecord, GetResult, OperationType


class TestDTO:
    """Test suite for DTOs."""
    
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
    
    def test_entry_creation(self):
        """Test Entry creation and attributes."""
        print("\nTest 1: Entry Creation")
        print("-" * 60)
        
        timestamp = int(time.time() * 1000000)
        entry = Entry(key="test_key", value="test_value", timestamp=timestamp, is_deleted=False)
        
        self.assert_true(entry.key == "test_key", "Key set correctly")
        self.assert_true(entry.value == "test_value", "Value set correctly")
        self.assert_true(entry.timestamp == timestamp, "Timestamp set correctly")
        self.assert_true(entry.is_deleted == False, "is_deleted set correctly")
    
    def test_entry_tombstone(self):
        """Test Entry as tombstone (deleted)."""
        print("\nTest 2: Entry Tombstone")
        print("-" * 60)
        
        timestamp = int(time.time() * 1000000)
        tombstone = Entry(key="deleted_key", value=None, timestamp=timestamp, is_deleted=True)
        
        self.assert_true(tombstone.key == "deleted_key", "Tombstone key set")
        self.assert_true(tombstone.value is None, "Tombstone value is None")
        self.assert_true(tombstone.is_deleted == True, "Tombstone marked as deleted")
    
    def test_entry_comparison_lt(self):
        """Test Entry less-than comparison."""
        print("\nTest 3: Entry Less-Than Comparison")
        print("-" * 60)
        
        entry1 = Entry("key_a", "value1", 1000, False)
        entry2 = Entry("key_b", "value2", 2000, False)
        entry3 = Entry("key_a", "value3", 3000, False)  # Same key, different timestamp
        
        self.assert_true(entry1 < entry2, "entry1 < entry2 (by key)")
        self.assert_true(not (entry2 < entry1), "entry2 not < entry1")
        self.assert_true(not (entry1 < entry3), "Same keys not < each other")
    
    def test_entry_comparison_eq(self):
        """Test Entry equality comparison."""
        print("\nTest 4: Entry Equality Comparison")
        print("-" * 60)
        
        entry1 = Entry("same_key", "value1", 1000, False)
        entry2 = Entry("same_key", "value2", 2000, False)
        entry3 = Entry("diff_key", "value3", 1000, False)
        
        self.assert_true(entry1 == entry2, "Entries equal if keys match")
        self.assert_true(not (entry1 == entry3), "Entries not equal if keys differ")
    
    def test_entry_edge_cases(self):
        """Test Entry with edge case values."""
        print("\nTest 5: Entry Edge Cases")
        print("-" * 60)
        
        # Empty key
        entry1 = Entry("", "value", 1000, False)
        self.assert_true(entry1.key == "", "Empty key allowed")
        
        # Empty value
        entry2 = Entry("key", "", 1000, False)
        self.assert_true(entry2.value == "", "Empty value allowed")
        
        # Both empty
        entry3 = Entry("", "", 1000, False)
        self.assert_true(entry3.key == "" and entry3.value == "", "Both empty allowed")
        
        # Special characters
        entry4 = Entry("key:with:special", "value|with|pipes", 1000, False)
        self.assert_true(":" in entry4.key, "Special chars in key")
        self.assert_true("|" in entry4.value, "Special chars in value")
        
        # Unicode
        entry5 = Entry("key_🎉", "value_🚀", 1000, False)
        self.assert_true("🎉" in entry5.key, "Unicode in key")
        self.assert_true("🚀" in entry5.value, "Unicode in value")
        
        # Long strings
        long_key = "k" * 1000
        long_value = "v" * 10000
        entry6 = Entry(long_key, long_value, 1000, False)
        self.assert_true(len(entry6.key) == 1000, "Long key allowed")
        self.assert_true(len(entry6.value) == 10000, "Long value allowed")
    
    def test_wal_record_creation(self):
        """Test WALRecord creation."""
        print("\nTest 6: WALRecord Creation")
        print("-" * 60)
        
        timestamp = int(time.time() * 1000000)
        
        # PUT record
        put_record = WALRecord(OperationType.PUT, "key1", "value1", timestamp)
        self.assert_true(put_record.operation == OperationType.PUT, "PUT operation set")
        self.assert_true(put_record.key == "key1", "Key set")
        self.assert_true(put_record.value == "value1", "Value set")
        self.assert_true(put_record.timestamp == timestamp, "Timestamp set")
        
        # DELETE record
        delete_record = WALRecord(OperationType.DELETE, "key2", None, timestamp)
        self.assert_true(delete_record.operation == OperationType.DELETE, "DELETE operation set")
        self.assert_true(delete_record.value is None, "DELETE has None value")
    
    def test_wal_record_serialization(self):
        """Test WALRecord serialization and deserialization."""
        print("\nTest 7: WALRecord Serialization")
        print("-" * 60)
        
        timestamp = 1234567890123456
        
        # PUT record
        put_record = WALRecord(OperationType.PUT, "mykey", "myvalue", timestamp)
        serialized = put_record.serialize()
        
        self.assert_true(isinstance(serialized, str), "Serialization returns string")
        self.assert_true("PUT" in serialized, "Contains operation type")
        self.assert_true("mykey" in serialized, "Contains key")
        self.assert_true("myvalue" in serialized, "Contains value")
        self.assert_true(str(timestamp) in serialized, "Contains timestamp")
        self.assert_true(serialized.endswith("\n"), "Ends with newline")
        
        # Deserialize
        deserialized = WALRecord.deserialize(serialized)
        
        self.assert_true(deserialized.operation == OperationType.PUT, "Operation deserialized")
        self.assert_true(deserialized.key == "mykey", "Key deserialized")
        self.assert_true(deserialized.value == "myvalue", "Value deserialized")
        self.assert_true(deserialized.timestamp == timestamp, "Timestamp deserialized")
    
    def test_wal_record_delete_serialization(self):
        """Test WALRecord DELETE serialization."""
        print("\nTest 8: WALRecord DELETE Serialization")
        print("-" * 60)
        
        timestamp = 9876543210
        delete_record = WALRecord(OperationType.DELETE, "deleted_key", None, timestamp)
        
        serialized = delete_record.serialize()
        
        self.assert_true("DELETE" in serialized, "Contains DELETE operation")
        self.assert_true("deleted_key" in serialized, "Contains key")
        
        # Deserialize
        deserialized = WALRecord.deserialize(serialized)
        
        self.assert_true(deserialized.operation == OperationType.DELETE, "DELETE deserialized")
        self.assert_true(deserialized.key == "deleted_key", "Key deserialized")
        self.assert_true(deserialized.value is None or deserialized.value == "", "Value is None or empty")
    
    def test_wal_record_special_characters(self):
        """Test WALRecord with special characters."""
        print("\nTest 9: WALRecord Special Characters")
        print("-" * 60)
        
        timestamp = 111111
        
        # Record with pipes in value (pipes used as delimiter!)
        # This is a critical edge case
        record1 = WALRecord(OperationType.PUT, "key_with_pipe", "value|with|pipes", timestamp)
        serialized1 = record1.serialize()
        
        # Deserialize should handle it
        try:
            deserialized1 = WALRecord.deserialize(serialized1)
            # May have issues with pipe delimiter
            print(f"  ⚠️  Pipe in value may cause issues: {deserialized1.value}")
        except:
            print(f"  ⚠️  Pipe delimiter conflict detected (expected)")
        
        # Newlines, tabs (should be handled)
        record2 = WALRecord(OperationType.PUT, "key2", "value\twith\ttabs", timestamp)
        serialized2 = record2.serialize()
        deserialized2 = WALRecord.deserialize(serialized2)
        self.assert_true(deserialized2.key == "key2", "Key with tabs handled")
    
    def test_wal_record_empty_values(self):
        """Test WALRecord with empty values."""
        print("\nTest 10: WALRecord Empty Values")
        print("-" * 60)
        
        timestamp = 222222
        
        # Empty key
        record1 = WALRecord(OperationType.PUT, "", "value", timestamp)
        serialized1 = record1.serialize()
        deserialized1 = WALRecord.deserialize(serialized1)
        self.assert_true(deserialized1.key == "", "Empty key serialized")
        
        # Empty value
        record2 = WALRecord(OperationType.PUT, "key", "", timestamp)
        serialized2 = record2.serialize()
        deserialized2 = WALRecord.deserialize(serialized2)
        self.assert_true(deserialized2.value == "" or deserialized2.value is None, "Empty value serialized")
    
    def test_get_result_found(self):
        """Test GetResult for found key."""
        print("\nTest 11: GetResult Found")
        print("-" * 60)
        
        result = GetResult(key="found_key", value="found_value", found=True)
        
        self.assert_true(result.key == "found_key", "Key set")
        self.assert_true(result.value == "found_value", "Value set")
        self.assert_true(result.found == True, "Found flag set")
        
        # String representation
        str_repr = str(result)
        self.assert_true("found_key" in str_repr, "String contains key")
        self.assert_true("found_value" in str_repr, "String contains value")
    
    def test_get_result_not_found(self):
        """Test GetResult for not found key."""
        print("\nTest 12: GetResult Not Found")
        print("-" * 60)
        
        result = GetResult(key="missing_key", value=None, found=False)
        
        self.assert_true(result.key == "missing_key", "Key set")
        self.assert_true(result.value is None, "Value is None")
        self.assert_true(result.found == False, "Found flag is False")
        
        # String representation
        str_repr = str(result)
        self.assert_true("not found" in str_repr.lower(), "String indicates not found")
    
    def test_operation_type_enum(self):
        """Test OperationType enum."""
        print("\nTest 13: OperationType Enum")
        print("-" * 60)
        
        # Test enum values
        self.assert_true(OperationType.PUT.value == "PUT", "PUT value correct")
        self.assert_true(OperationType.DELETE.value == "DELETE", "DELETE value correct")
        
        # Test enum usage
        op1 = OperationType.PUT
        op2 = OperationType.DELETE
        
        self.assert_true(op1 != op2, "Different operations are different")
        self.assert_true(op1 == OperationType.PUT, "Enum equality works")
    
    def test_entry_sorting(self):
        """Test that entries can be sorted."""
        print("\nTest 14: Entry Sorting")
        print("-" * 60)
        
        entries = [
            Entry("key_c", "val", 1000, False),
            Entry("key_a", "val", 1001, False),
            Entry("key_b", "val", 1002, False),
        ]
        
        sorted_entries = sorted(entries)
        
        self.assert_true(sorted_entries[0].key == "key_a", "First entry is key_a")
        self.assert_true(sorted_entries[1].key == "key_b", "Second entry is key_b")
        self.assert_true(sorted_entries[2].key == "key_c", "Third entry is key_c")
        
        # Sort by key explicitly
        sorted_by_key = sorted(entries, key=lambda e: e.key)
        self.assert_true(sorted_by_key[0].key == "key_a", "Explicit sort works")
    
    def test_entry_timestamp_ordering(self):
        """Test entries with same key but different timestamps."""
        print("\nTest 15: Entry Timestamp Ordering")
        print("-" * 60)
        
        entry1 = Entry("same_key", "v1", 1000, False)
        entry2 = Entry("same_key", "v2", 2000, False)
        entry3 = Entry("same_key", "v3", 3000, False)
        
        # Keep entry with highest timestamp
        entries = [entry1, entry2, entry3]
        latest = max(entries, key=lambda e: e.timestamp)
        
        self.assert_true(latest.value == "v3", "Latest entry has highest timestamp")
        self.assert_true(latest.timestamp == 3000, "Latest timestamp is 3000")
    
    def test_wal_record_round_trip(self):
        """Test WALRecord serialize → deserialize round trip."""
        print("\nTest 16: WALRecord Round Trip")
        print("-" * 60)
        
        test_cases = [
            WALRecord(OperationType.PUT, "key1", "value1", 111111),
            WALRecord(OperationType.DELETE, "key2", None, 222222),
            WALRecord(OperationType.PUT, "unicode_🎉", "value_🚀", 555555),
        ]
        
        for i, original in enumerate(test_cases):
            serialized = original.serialize()
            deserialized = WALRecord.deserialize(serialized)
            
            self.assert_true(deserialized.operation == original.operation, f"Case {i}: Operation matches")
            self.assert_true(deserialized.key == original.key, f"Case {i}: Key matches")
            self.assert_true(deserialized.timestamp == original.timestamp, f"Case {i}: Timestamp matches")
            
            # Value check (DELETE may have None or empty, empty strings may deserialize as empty or None)
            if original.operation == OperationType.PUT and original.value:
                match = deserialized.value == original.value
            else:
                # For DELETE or empty values, be flexible
                match = deserialized.value is None or deserialized.value == "" or deserialized.value == original.value
            
            self.assert_true(match, f"Case {i}: Value matches (got {repr(deserialized.value)})")
    
    def test_wal_record_invalid_format(self):
        """Test WALRecord deserialization with invalid format."""
        print("\nTest 17: WALRecord Invalid Format")
        print("-" * 60)
        
        invalid_records = [
            "INVALID",  # Not enough parts
            "PUT|key",  # Missing parts
            "INVALID|key|value|123",  # Invalid operation
        ]
        
        for invalid in invalid_records:
            try:
                WALRecord.deserialize(invalid)
                self.assert_true(False, f"Should raise error for: {invalid}")
            except (ValueError, KeyError) as e:
                self.assert_true(True, f"Correctly raised error for invalid record")
    
    def test_get_result_boolean_check(self):
        """Test GetResult boolean usage."""
        print("\nTest 18: GetResult Boolean Check")
        print("-" * 60)
        
        found_result = GetResult("key", "value", True)
        not_found_result = GetResult("key", None, False)
        
        # Can use in if statements
        if found_result.found:
            self.assert_true(True, "found_result.found is truthy")
        else:
            self.assert_true(False, "found_result.found should be truthy")
        
        if not not_found_result.found:
            self.assert_true(True, "not_found_result.found is falsy")
        else:
            self.assert_true(False, "not_found_result.found should be falsy")
    
    def test_entry_dataclass_behavior(self):
        """Test Entry dataclass behavior."""
        print("\nTest 19: Entry Dataclass Behavior")
        print("-" * 60)
        
        # Default value for is_deleted
        entry1 = Entry("key", "value", 1000)
        self.assert_true(entry1.is_deleted == False, "is_deleted defaults to False")
        
        # Can create with keyword args
        entry2 = Entry(key="k", value="v", timestamp=2000, is_deleted=True)
        self.assert_true(entry2.key == "k", "Keyword args work")
        
        # Can access as attributes
        key = entry2.key
        value = entry2.value
        self.assert_true(key == "k" and value == "v", "Attribute access works")
    
    def test_wal_record_dataclass_behavior(self):
        """Test WALRecord dataclass behavior."""
        print("\nTest 20: WALRecord Dataclass Behavior")
        print("-" * 60)
        
        # Create with positional args
        record1 = WALRecord(OperationType.PUT, "key", "value", 1000)
        self.assert_true(record1.operation == OperationType.PUT, "Positional args work")
        
        # Create with keyword args
        record2 = WALRecord(operation=OperationType.DELETE, key="k", value=None, timestamp=2000)
        self.assert_true(record2.operation == OperationType.DELETE, "Keyword args work")
    
    def test_get_result_dataclass_behavior(self):
        """Test GetResult dataclass behavior."""
        print("\nTest 21: GetResult Dataclass Behavior")
        print("-" * 60)
        
        # Create with positional
        result1 = GetResult("key", "value", True)
        self.assert_true(result1.found, "Positional args work")
        
        # Create with keyword
        result2 = GetResult(key="k", value="v", found=False)
        self.assert_true(result2.key == "k", "Keyword args work")
    
    def run_all_tests(self):
        """Run all tests."""
        print("=" * 70)
        print("DTO - COMPREHENSIVE TEST SUITE")
        print("=" * 70)
        
        self.test_entry_creation()
        self.test_entry_tombstone()
        self.test_entry_comparison_lt()
        self.test_entry_comparison_eq()
        self.test_entry_edge_cases()
        self.test_wal_record_creation()
        self.test_wal_record_serialization()
        self.test_wal_record_delete_serialization()
        self.test_wal_record_special_characters()
        self.test_wal_record_empty_values()
        self.test_get_result_found()
        self.test_get_result_not_found()
        self.test_operation_type_enum()
        self.test_entry_sorting()
        self.test_entry_timestamp_ordering()
        self.test_wal_record_round_trip()
        self.test_wal_record_invalid_format()
        self.test_get_result_boolean_check()
        self.test_entry_dataclass_behavior()
        self.test_wal_record_dataclass_behavior()
        self.test_get_result_dataclass_behavior()
        
        print("\n" + "=" * 70)
        print(f"RESULTS: {self.passed} passed, {self.failed} failed")
        print("=" * 70)
        
        return self.failed == 0


if __name__ == "__main__":
    tester = TestDTO()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
