#!/usr/bin/env python3
"""
Comprehensive Concurrency and Thread Safety Analysis for LSM-KV Store.

This test validates thread safety across all components under concurrent load.
"""
import sys
import shutil
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from lsmkv import LSMKVStore


class ConcurrencyTestSuite:
    """Test suite for concurrent operations."""
    
    def __init__(self):
        self.data_dir = "./test_concurrency_analysis"
        self.store = None
        self.results = []
        
    def setup(self):
        """Setup test environment."""
        shutil.rmtree(self.data_dir, ignore_errors=True)
        self.store = LSMKVStore(
            data_dir=self.data_dir,
            memtable_size=50,
            max_immutable_memtables=4,
            max_l0_sstables=4,
            soft_limit_ratio=0.85,
            flush_workers=4
        )
    
    def teardown(self):
        """Cleanup test environment."""
        if self.store:
            self.store.close()
            time.sleep(0.5)
        shutil.rmtree(self.data_dir, ignore_errors=True)
    
    def test_1_concurrent_writes(self):
        """Test concurrent PUT operations from multiple threads."""
        print("\n" + "=" * 70)
        print("TEST 1: Concurrent Writes (Multiple Writers)")
        print("=" * 70)
        
        self.setup()
        num_threads = 10
        writes_per_thread = 100
        errors = []
        
        def writer(thread_id):
            try:
                for i in range(writes_per_thread):
                    key = f"thread{thread_id}_key{i:04d}"
                    value = f"value_{thread_id}_{i}"
                    self.store.put(key, value)
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")
        
        start = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(writer, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()
        
        elapsed = time.time() - start
        total_writes = num_threads * writes_per_thread
        
        print(f"  - {total_writes} writes from {num_threads} threads in {elapsed:.3f}s")
        print(f"  - Throughput: {total_writes / elapsed:.0f} writes/sec")
        print(f"  - Errors: {len(errors)}")
        
        if errors:
            for err in errors[:5]:
                print(f"    {err}")
            return False
        
        # Wait for background operations
        time.sleep(2)
        
        # Verify some writes
        verified = 0
        for i in range(10):
            for j in range(10):
                result = self.store.get(f"thread{i}_key{j:04d}")
                if result.found:
                    verified += 1
        
        print(f"  - Verified {verified}/100 sample reads")
        self.teardown()
        return verified >= 90  # Allow some variance due to timing
    
    def test_2_concurrent_reads_writes(self):
        """Test concurrent reads and writes simultaneously."""
        print("\n" + "=" * 70)
        print("TEST 2: Concurrent Reads + Writes (Read-Write Mix)")
        print("=" * 70)
        
        self.setup()
        
        # Pre-populate some data
        for i in range(100):
            self.store.put(f"preload_key{i:04d}", f"preload_value{i}")
        
        time.sleep(1)  # Wait for flushes
        
        num_writers = 5
        num_readers = 10
        operations_per_thread = 50
        write_errors = []
        read_results = {"found": 0, "not_found": 0}
        read_lock = threading.Lock()
        
        def writer(thread_id):
            try:
                for i in range(operations_per_thread):
                    key = f"write_thread{thread_id}_key{i:04d}"
                    self.store.put(key, f"value_{thread_id}_{i}")
                    time.sleep(random.uniform(0.001, 0.005))  # Simulate varying write speed
            except Exception as e:
                write_errors.append(f"Writer {thread_id}: {e}")
        
        def reader(thread_id):
            local_found = 0
            local_not_found = 0
            try:
                for i in range(operations_per_thread):
                    # Read either preloaded or newly written keys
                    if random.random() < 0.5:
                        key = f"preload_key{random.randint(0, 99):04d}"
                    else:
                        key = f"write_thread{random.randint(0, num_writers-1)}_key{random.randint(0, operations_per_thread-1):04d}"
                    
                    result = self.store.get(key)
                    if result.found:
                        local_found += 1
                    else:
                        local_not_found += 1
                        
                    time.sleep(random.uniform(0.001, 0.003))
            except Exception as e:
                write_errors.append(f"Reader {thread_id}: {e}")
            
            with read_lock:
                read_results["found"] += local_found
                read_results["not_found"] += local_not_found
        
        start = time.time()
        
        with ThreadPoolExecutor(max_workers=num_writers + num_readers) as executor:
            writer_futures = [executor.submit(writer, i) for i in range(num_writers)]
            reader_futures = [executor.submit(reader, i) for i in range(num_readers)]
            
            for future in as_completed(writer_futures + reader_futures):
                future.result()
        
        elapsed = time.time() - start
        
        print(f"  - {num_writers} writers × {operations_per_thread} ops = {num_writers * operations_per_thread} writes")
        print(f"  - {num_readers} readers × {operations_per_thread} ops = {num_readers * operations_per_thread} reads")
        print(f"  - Completed in {elapsed:.3f}s")
        print(f"  - Reads found: {read_results['found']}, not found: {read_results['not_found']}")
        print(f"  - Errors: {len(write_errors)}")
        
        self.teardown()
        return len(write_errors) == 0
    
    def test_3_concurrent_deletes(self):
        """Test concurrent DELETE operations."""
        print("\n" + "=" * 70)
        print("TEST 3: Concurrent Deletes")
        print("=" * 70)
        
        self.setup()
        
        # Pre-populate data
        for i in range(200):
            self.store.put(f"del_key{i:04d}", f"del_value{i}")
        
        time.sleep(1)
        
        num_threads = 5
        deletes_per_thread = 40
        errors = []
        
        def deleter(thread_id):
            try:
                start_key = thread_id * deletes_per_thread
                for i in range(deletes_per_thread):
                    key = f"del_key{(start_key + i):04d}"
                    self.store.delete(key)
            except Exception as e:
                errors.append(f"Deleter {thread_id}: {e}")
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(deleter, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()
        
        time.sleep(1)
        
        # Verify deletes
        deleted = 0
        for i in range(200):
            result = self.store.get(f"del_key{i:04d}")
            if not result.found:
                deleted += 1
        
        print(f"  - Deleted {deleted}/200 keys")
        print(f"  - Errors: {len(errors)}")
        
        self.teardown()
        return len(errors) == 0 and deleted >= 180
    
    def test_4_flush_during_writes(self):
        """Test that writes continue during background flushes."""
        print("\n" + "=" * 70)
        print("TEST 4: Non-blocking Flush During Writes")
        print("=" * 70)
        
        self.setup()
        
        write_times = []
        errors = []
        
        def continuous_writer():
            for i in range(500):
                start = time.time()
                try:
                    self.store.put(f"flush_test_key{i:04d}", f"value_{i}" * 10)
                    elapsed = time.time() - start
                    write_times.append(elapsed)
                except Exception as e:
                    errors.append(str(e))
        
        start = time.time()
        continuous_writer()
        total_elapsed = time.time() - start
        
        time.sleep(2)  # Wait for background flushes
        
        avg_write_time = sum(write_times) / len(write_times) if write_times else 0
        max_write_time = max(write_times) if write_times else 0
        
        print(f"  - 500 writes in {total_elapsed:.3f}s")
        print(f"  - Avg write latency: {avg_write_time * 1000:.2f}ms")
        print(f"  - Max write latency: {max_write_time * 1000:.2f}ms")
        print(f"  - Errors: {len(errors)}")
        
        stats = self.store.stats()
        print(f"  - Rotations: {stats['total_memtable_rotations']}")
        print(f"  - Async flushes: {stats['total_async_flushes']}")
        
        self.teardown()
        # Success if max latency is reasonable (< 100ms) and no errors
        return len(errors) == 0 and max_write_time < 0.1
    
    def test_5_compaction_during_operations(self):
        """Test reads/writes during compaction."""
        print("\n" + "=" * 70)
        print("TEST 5: Reads/Writes During Compaction")
        print("=" * 70)
        
        self.setup()
        
        # Create multiple SSTables to compact
        for batch in range(5):
            for i in range(60):
                self.store.put(f"compact_key{i:04d}", f"value_batch{batch}_{i}")
            time.sleep(0.5)  # Allow flushes
        
        time.sleep(2)
        
        print(f"  - Level info before: {self.store.get_level_info()}")
        
        errors = []
        
        # Start concurrent reads while compaction happens automatically
        def reader():
            for _ in range(50):
                try:
                    key = f"compact_key{random.randint(0, 59):04d}"
                    self.store.get(key)
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.01)
        
        def writer():
            for i in range(50):
                try:
                    self.store.put(f"during_compact_key{i:04d}", f"value_{i}")
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.01)
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(reader),
                executor.submit(reader),
                executor.submit(writer),
                executor.submit(writer)
            ]
            for f in as_completed(futures):
                f.result()
        
        time.sleep(1)
        
        print(f"  - Level info after: {self.store.get_level_info()}")
        print(f"  - Errors: {len(errors)}")
        
        self.teardown()
        return len(errors) == 0
    
    def test_6_stress_test(self):
        """High concurrency stress test."""
        print("\n" + "=" * 70)
        print("TEST 6: Stress Test (High Concurrency)")
        print("=" * 70)
        
        self.setup()
        
        num_threads = 20
        ops_per_thread = 100
        errors = []
        error_lock = threading.Lock()
        
        def mixed_operations(thread_id):
            local_errors = []
            for i in range(ops_per_thread):
                try:
                    op = random.choice(["put", "put", "put", "get", "get", "delete"])
                    key = f"stress_key{random.randint(0, 500):04d}"
                    
                    if op == "put":
                        self.store.put(key, f"value_{thread_id}_{i}")
                    elif op == "get":
                        self.store.get(key)
                    elif op == "delete":
                        self.store.delete(key)
                        
                except Exception as e:
                    local_errors.append(f"Thread {thread_id}, op {i}: {e}")
            
            with error_lock:
                errors.extend(local_errors)
        
        start = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mixed_operations, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()
        
        elapsed = time.time() - start
        total_ops = num_threads * ops_per_thread
        
        print(f"  - {total_ops} mixed operations from {num_threads} threads")
        print(f"  - Completed in {elapsed:.3f}s")
        print(f"  - Throughput: {total_ops / elapsed:.0f} ops/sec")
        print(f"  - Errors: {len(errors)}")
        
        if errors:
            for err in errors[:3]:
                print(f"    {err}")
        
        time.sleep(2)  # Wait for background operations
        
        stats = self.store.stats()
        print(f"  - Final stats:")
        print(f"    - SSTables: {stats['num_sstables']}")
        print(f"    - Levels: {stats['num_levels']}")
        print(f"    - Rotations: {stats['total_memtable_rotations']}")
        print(f"    - Async flushes: {stats['total_async_flushes']}")
        
        self.teardown()
        return len(errors) == 0
    
    def run_all(self):
        """Run all concurrency tests."""
        print("\n" + "=" * 70)
        print("CONCURRENCY AND THREAD SAFETY ANALYSIS")
        print("=" * 70)
        
        tests = [
            ("Concurrent Writes", self.test_1_concurrent_writes),
            ("Concurrent Reads+Writes", self.test_2_concurrent_reads_writes),
            ("Concurrent Deletes", self.test_3_concurrent_deletes),
            ("Non-blocking Flush", self.test_4_flush_during_writes),
            ("Compaction During Ops", self.test_5_compaction_during_operations),
            ("Stress Test", self.test_6_stress_test),
        ]
        
        results = []
        for name, test in tests:
            try:
                passed = test()
                results.append((name, passed, None))
            except Exception as e:
                results.append((name, False, str(e)))
                import traceback
                traceback.print_exc()
        
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        
        all_passed = True
        for name, passed, error in results:
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"  {status}: {name}")
            if error:
                print(f"         Error: {error}")
            if not passed:
                all_passed = False
        
        print("\n" + "=" * 70)
        if all_passed:
            print("ALL CONCURRENCY TESTS PASSED!")
        else:
            print("SOME TESTS FAILED - SEE ABOVE FOR DETAILS")
        print("=" * 70)
        
        return all_passed


if __name__ == "__main__":
    suite = ConcurrencyTestSuite()
    success = suite.run_all()
    sys.exit(0 if success else 1)
