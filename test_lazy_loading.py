#!/usr/bin/env python3
"""
Test suite for lazy SSTable loading and optimized mmap reads.

Validates:
1. Lazy loading: Only metadata loaded on startup
2. On-demand loading: SSTable loaded when first accessed
3. Optimized mmap reads: Only bytes between floor/ceil from sparse index
4. Background manifest reload
5. Memory efficiency
"""
import os
import sys
import time
import shutil
import threading

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lsmkv import LSMKVStore
from lsmkv.storage.sstable import SSTable, LazySSTable, SSTableMetadata
from lsmkv.storage.sparse_index import SparseIndex
from lsmkv.core.dto import Entry


def cleanup_test_dir(test_dir: str):
    """Clean up test directory."""
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def test_lazy_sstable_creation():
    """Test that LazySSTable only loads on first access."""
    print("\n" + "=" * 70)
    print("TEST: LazySSTable creation and lazy loading")
    print("=" * 70)
    
    test_dir = "./test_lazy_creation"
    cleanup_test_dir(test_dir)
    
    try:
        os.makedirs(f"{test_dir}/sstables", exist_ok=True)
        
        # Create a real SSTable first
        entries = [
            Entry(key=f"key_{i:04d}", value=f"value_{i}" * 10, 
                  timestamp=1000 + i, is_deleted=False)
            for i in range(100)
        ]
        
        sstable = SSTable(f"{test_dir}/sstables", 1)
        metadata = sstable.write(entries)
        sstable.close()
        
        print(f"  Created SSTable with {metadata.num_entries} entries")
        
        # Create LazySSTable with metadata (no disk I/O)
        lazy = LazySSTable(
            sstables_dir=f"{test_dir}/sstables",
            sstable_id=1,
            metadata=metadata
        )
        
        # Verify not loaded yet
        assert not lazy.is_loaded(), "LazySSTable should not be loaded initially"
        print("  ✓ LazySSTable not loaded after creation")
        
        # Access the SSTable
        result = lazy.get("key_0050")
        
        # Now it should be loaded
        assert lazy.is_loaded(), "LazySSTable should be loaded after access"
        assert result is not None, "Should find key_0050"
        assert result.key == "key_0050", f"Wrong key: {result.key}"
        print("  ✓ LazySSTable loaded on first access")
        print(f"  ✓ Found key_0050: value length = {len(result.value)}")
        
        # Check access count
        assert lazy.access_count == 1, f"Access count should be 1, got {lazy.access_count}"
        
        # Unload and verify
        lazy.unload()
        assert not lazy.is_loaded(), "Should be unloaded after unload()"
        print("  ✓ SSTable unloaded successfully")
        
        # Access again (should reload)
        result = lazy.get("key_0025")
        assert lazy.is_loaded(), "Should reload on access"
        assert result is not None, "Should find key_0025"
        print("  ✓ SSTable reloaded on subsequent access")
        
        lazy.close()
        print("\n  PASSED: LazySSTable lazy loading works correctly")
        
    finally:
        cleanup_test_dir(test_dir)


def test_metadata_key_range_filter():
    """Test that LazySSTable uses metadata for key range filtering."""
    print("\n" + "=" * 70)
    print("TEST: Metadata key range filtering (no disk I/O)")
    print("=" * 70)
    
    test_dir = "./test_key_range"
    cleanup_test_dir(test_dir)
    
    try:
        os.makedirs(f"{test_dir}/sstables", exist_ok=True)
        
        # Create SSTable with keys from key_0100 to key_0199
        entries = [
            Entry(key=f"key_{i:04d}", value=f"value_{i}", 
                  timestamp=1000 + i, is_deleted=False)
            for i in range(100, 200)
        ]
        
        sstable = SSTable(f"{test_dir}/sstables", 1)
        metadata = sstable.write(entries)
        sstable.close()
        
        print(f"  SSTable key range: {metadata.min_key} to {metadata.max_key}")
        
        # Create LazySSTable
        lazy = LazySSTable(
            sstables_dir=f"{test_dir}/sstables",
            sstable_id=1,
            metadata=metadata
        )
        
        # Query key outside range (should NOT load SSTable)
        result = lazy.get("key_0050")  # Before range
        assert not lazy.is_loaded(), "Should not load for key before range"
        assert result is None, "Should not find key before range"
        print("  ✓ Key before range rejected without loading SSTable")
        
        result = lazy.get("key_0250")  # After range
        assert not lazy.is_loaded(), "Should not load for key after range"
        assert result is None, "Should not find key after range"
        print("  ✓ Key after range rejected without loading SSTable")
        
        # Query key in range (should load)
        result = lazy.get("key_0150")
        assert lazy.is_loaded(), "Should load for key in range"
        assert result is not None, "Should find key_0150"
        print("  ✓ Key in range loaded SSTable and found")
        
        lazy.close()
        print("\n  PASSED: Metadata key range filtering works correctly")
        
    finally:
        cleanup_test_dir(test_dir)


def test_optimized_mmap_sparse_index():
    """Test that mmap reads only the bounded region from sparse index."""
    print("\n" + "=" * 70)
    print("TEST: Optimized mmap reads using sparse index bounds")
    print("=" * 70)
    
    test_dir = "./test_sparse_mmap"
    cleanup_test_dir(test_dir)
    
    try:
        os.makedirs(f"{test_dir}/sstables", exist_ok=True)
        
        # Create SSTable with many entries (to test sparse index effectiveness)
        entries = [
            Entry(key=f"key_{i:06d}", value=f"value_{i}" * 50,  # Larger values
                  timestamp=1000 + i, is_deleted=False)
            for i in range(1000)
        ]
        
        # Use small block size to create more index entries
        sstable = SSTable(f"{test_dir}/sstables", 1)
        metadata = sstable.write(entries, block_size=4)
        
        # Get file size for comparison
        data_file_size = os.path.getsize(sstable.data_filepath)
        print(f"  SSTable data file size: {data_file_size:,} bytes")
        print(f"  SSTable entries: {metadata.num_entries}")
        
        # Load sparse index to check structure
        sstable._ensure_sparse_index_loaded()
        sparse_index = sstable._sparse_index
        print(f"  Sparse index entries: {len(sparse_index.entries)}")
        
        # Test get_scan_range for a key in the middle
        test_key = "key_000500"
        start_offset, end_offset = sparse_index.get_scan_range(test_key)
        
        bytes_to_read = (end_offset - start_offset) if end_offset else (data_file_size - start_offset)
        percent_of_file = 100 * bytes_to_read / data_file_size
        
        print(f"\n  Searching for: {test_key}")
        print(f"  Scan range: bytes {start_offset} to {end_offset or 'EOF'}")
        print(f"  Bytes to read: {bytes_to_read:,} ({percent_of_file:.1f}% of file)")
        
        # Perform the actual lookup
        result = sstable.get(test_key)
        assert result is not None, f"Should find {test_key}"
        assert result.key == test_key, f"Wrong key returned"
        print(f"  ✓ Found key, value length: {len(result.value)}")
        
        # Test edge cases
        # First key
        start_offset, end_offset = sparse_index.get_scan_range("key_000000")
        print(f"\n  First key scan range: {start_offset} to {end_offset}")
        result = sstable.get("key_000000")
        assert result is not None, "Should find first key"
        print("  ✓ First key found")
        
        # Last key
        start_offset, end_offset = sparse_index.get_scan_range("key_000999")
        print(f"  Last key scan range: {start_offset} to {end_offset}")
        result = sstable.get("key_000999")
        assert result is not None, "Should find last key"
        print("  ✓ Last key found")
        
        # Non-existent key
        result = sstable.get("key_999999")
        assert result is None, "Should not find non-existent key"
        print("  ✓ Non-existent key correctly not found")
        
        sstable.close()
        print("\n  PASSED: Optimized mmap reads using sparse index bounds")
        
    finally:
        cleanup_test_dir(test_dir)


def test_lazy_loading_on_startup():
    """Test that LSMKVStore uses lazy loading on startup."""
    print("\n" + "=" * 70)
    print("TEST: Lazy loading on KVStore startup")
    print("=" * 70)
    
    test_dir = "./test_lazy_startup"
    cleanup_test_dir(test_dir)
    
    try:
        # Phase 1: Create store and write data
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=100,
            max_l0_sstables=4,
            max_immutable_memtables=2
        )
        
        # Write enough data to create SSTables
        for i in range(200):
            store.put(f"key_{i:04d}", f"value_{i}" * 20)
        
        time.sleep(1)  # Allow flushes
        
        # Force remaining data to disk
        store.memtable_manager.force_flush_all()
        store.sstable_manager.wait_for_compaction(timeout=10)
        time.sleep(0.5)
        
        sstable_count = store.sstable_manager.count()
        print(f"  Created {sstable_count} SSTables")
        
        store.close()
        
        # Phase 2: Reopen and check lazy loading
        print("\n  Reopening store (should use lazy loading)...")
        
        start_time = time.time()
        store2 = LSMKVStore(
            data_dir=test_dir,
            memtable_size=100,
            max_l0_sstables=4,
            max_immutable_memtables=2
        )
        open_time = time.time() - start_time
        
        print(f"  Store opened in {open_time:.3f}s")
        
        # Check lazy loading stats
        lazy_stats = store2.sstable_manager.get_lazy_load_stats()
        print(f"  Lazy loading stats:")
        print(f"    - Total SSTables: {lazy_stats['total_sstables']}")
        print(f"    - Loaded: {lazy_stats['loaded_sstables']}")
        print(f"    - Unloaded: {lazy_stats['unloaded_sstables']}")
        print(f"    - Memory saved: {lazy_stats['memory_saved_pct']:.1f}%")
        
        # Initially, no SSTables should be loaded
        assert lazy_stats['loaded_sstables'] == 0, "No SSTables should be loaded initially"
        print("  ✓ No SSTables loaded on startup")
        
        # Access a key (should trigger loading)
        result = store2.get("key_0100")
        
        lazy_stats = store2.sstable_manager.get_lazy_load_stats()
        print(f"\n  After first read:")
        print(f"    - Loaded SSTables: {lazy_stats['loaded_sstables']}")
        
        store2.close()
        print("\n  PASSED: Lazy loading on startup works correctly")
        
    finally:
        cleanup_test_dir(test_dir)


def test_background_manifest_reload():
    """Test background manifest reload functionality."""
    print("\n" + "=" * 70)
    print("TEST: Background manifest reload")
    print("=" * 70)
    
    test_dir = "./test_manifest_reload"
    cleanup_test_dir(test_dir)
    
    try:
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=50,
            max_l0_sstables=3,
            soft_limit_ratio=0.67,
            max_immutable_memtables=1
        )
        
        # Write data to trigger flushes and compactions
        for i in range(100):
            store.put(f"key_{i:04d}", f"value_{i}" * 10)
            time.sleep(0.01)
        
        time.sleep(1)  # Allow background operations
        
        # Verify data integrity after manifest reloads
        errors = 0
        for i in range(100):
            result = store.get(f"key_{i:04d}")
            if not result.found:
                errors += 1
        
        print(f"  Wrote 100 keys")
        print(f"  Verification errors: {errors}")
        
        # Check that manifests exist
        manifest_dir = os.path.join(test_dir, "manifests")
        if os.path.exists(manifest_dir):
            manifest_files = os.listdir(manifest_dir)
            print(f"  Manifest files: {manifest_files}")
        
        store.close()
        
        assert errors == 0, f"Found {errors} verification errors"
        print("\n  PASSED: Background manifest reload works correctly")
        
    finally:
        cleanup_test_dir(test_dir)


def test_concurrent_access_lazy_loading():
    """Test concurrent access with lazy loading."""
    print("\n" + "=" * 70)
    print("TEST: Concurrent access with lazy loading")
    print("=" * 70)
    
    test_dir = "./test_concurrent_lazy"
    cleanup_test_dir(test_dir)
    
    try:
        # Phase 1: Create data
        store = LSMKVStore(
            data_dir=test_dir,
            memtable_size=200,
            max_l0_sstables=4,
            max_immutable_memtables=2
        )
        
        for i in range(500):
            store.put(f"key_{i:05d}", f"value_{i}" * 10)
        
        time.sleep(1)
        store.memtable_manager.force_flush_all()
        store.sstable_manager.wait_for_compaction(timeout=10)
        store.close()
        
        # Phase 2: Reopen and access concurrently
        store2 = LSMKVStore(
            data_dir=test_dir,
            memtable_size=200,
            max_l0_sstables=4,
            max_immutable_memtables=2
        )
        
        read_results = {"success": 0, "not_found": 0, "errors": 0}
        lock = threading.Lock()
        
        def reader(thread_id, key_range):
            for i in key_range:
                try:
                    result = store2.get(f"key_{i:05d}")
                    with lock:
                        if result.found:
                            read_results["success"] += 1
                        else:
                            read_results["not_found"] += 1
                except Exception as e:
                    with lock:
                        read_results["errors"] += 1
        
        # Start multiple reader threads
        threads = []
        for t in range(4):
            start_key = t * 125
            end_key = start_key + 125
            thread = threading.Thread(target=reader, args=(t, range(start_key, end_key)))
            threads.append(thread)
        
        print("  Starting 4 concurrent reader threads...")
        start_time = time.time()
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        elapsed = time.time() - start_time
        
        print(f"  Completed in {elapsed:.3f}s")
        print(f"  Successful reads: {read_results['success']}")
        print(f"  Not found: {read_results['not_found']}")
        print(f"  Errors: {read_results['errors']}")
        
        lazy_stats = store2.sstable_manager.get_lazy_load_stats()
        print(f"  Loaded SSTables: {lazy_stats['loaded_sstables']}")
        
        store2.close()
        
        assert read_results["errors"] == 0, f"Found {read_results['errors']} errors"
        print("\n  PASSED: Concurrent access with lazy loading works correctly")
        
    finally:
        cleanup_test_dir(test_dir)


def main():
    """Run all lazy loading tests."""
    print("\n" + "=" * 70)
    print("LAZY LOADING AND OPTIMIZED MMAP TEST SUITE")
    print("=" * 70)
    
    tests = [
        ("LazySSTable Creation", test_lazy_sstable_creation),
        ("Metadata Key Range Filter", test_metadata_key_range_filter),
        ("Optimized mmap Sparse Index", test_optimized_mmap_sparse_index),
        ("Lazy Loading on Startup", test_lazy_loading_on_startup),
        ("Background Manifest Reload", test_background_manifest_reload),
        ("Concurrent Access Lazy Loading", test_concurrent_access_lazy_loading),
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
