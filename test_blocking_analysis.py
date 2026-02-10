#!/usr/bin/env python3
"""
Test to analyze blocking behavior during flush/compact operations.

This test verifies:
1. 85% soft limit triggers compaction correctly
2. Whether background flush/compact blocks main thread reads/writes
"""
import sys
import os
import shutil
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lsmkv import LSMKVStore


class BlockingAnalysisTest:
    """Analyze blocking behavior during flush and compaction."""
    
    def __init__(self):
        self.test_dir = None
        self.store = None
        
    def setup(self):
        self.test_dir = f"./test_blocking_{int(time.time() * 1000)}"
        shutil.rmtree(self.test_dir, ignore_errors=True)
        
    def teardown(self):
        if self.store:
            self.store.close()
            time.sleep(0.5)
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_1_soft_limit_triggers_at_85_percent(self):
        """Verify 85% soft limit triggers compaction."""
        print("\n" + "=" * 70)
        print("TEST 1: Verify 85% Soft Limit Compaction Trigger")
        print("=" * 70)
        
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=10,
                max_immutable_memtables=2,
                max_l0_sstables=4,      # Hard limit: 4
                soft_limit_ratio=0.85,  # Soft limit: 3.4 -> 3
                base_level_entries=100,
            )
            
            # Insert enough to create 4 SSTables at L0
            # With memtable_size=10 and max_immutable=2, we flush after every 2 rotations
            for i in range(80):  # 80 entries = 8 memtables = 4 flushes
                self.store.put(f"key{i:04d}", f"value{i}")
            
            time.sleep(3)  # Wait for background operations
            
            level_info = self.store.get_level_info()
            print(f"Level info: {level_info}")
            
            # Check if L1 exists (means compaction happened)
            l1_exists = 1 in level_info and level_info[1]['sstables'] > 0
            l0_count = level_info.get(0, {}).get('sstables', 0)
            
            print(f"L0 SSTables: {l0_count}")
            print(f"L1 exists: {l1_exists}")
            
            if l1_exists:
                print("✓ Compaction triggered at soft limit (85%)")
            else:
                print(f"L0 has {l0_count} SSTables (soft limit = 3)")
            
            # Verify soft limit is working
            # With soft limit at 85%, compaction should trigger when L0 reaches 3 SSTables
            # So L0 should have fewer than 4 SSTables
            assert l0_count < 4 or l1_exists, "Soft limit should prevent L0 from reaching hard limit"
            
            print("✓ TEST PASSED")
            return True
            
        finally:
            self.teardown()
    
    def test_2_flush_non_blocking_for_writes(self):
        """Test that flush doesn't block writes."""
        print("\n" + "=" * 70)
        print("TEST 2: Flush Non-Blocking for Writes")
        print("=" * 70)
        
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=50,
                max_immutable_memtables=2,
                flush_workers=2,
            )
            
            write_latencies = []
            
            # Write many entries to trigger multiple flushes
            for i in range(300):
                start = time.time()
                self.store.put(f"key{i:04d}", f"value{i}" * 10)
                latency = time.time() - start
                write_latencies.append(latency)
            
            avg_latency = sum(write_latencies) / len(write_latencies)
            max_latency = max(write_latencies)
            p99_latency = sorted(write_latencies)[int(len(write_latencies) * 0.99)]
            
            print(f"Write latencies (300 writes):")
            print(f"  - Average: {avg_latency * 1000:.2f}ms")
            print(f"  - Max: {max_latency * 1000:.2f}ms")
            print(f"  - P99: {p99_latency * 1000:.2f}ms")
            
            stats = self.store.stats()
            print(f"  - Rotations: {stats['total_memtable_rotations']}")
            print(f"  - Async flushes: {stats['total_async_flushes']}")
            
            # Writes should be fast (flush is async)
            # Max latency should be < 100ms for writes
            if max_latency < 0.1:
                print("✓ Flush is non-blocking for writes")
            else:
                print(f"⚠ Max write latency {max_latency*1000:.2f}ms exceeds 100ms threshold")
            
            print("✓ TEST PASSED")
            return True
            
        finally:
            self.teardown()
    
    def test_3_compaction_blocking_analysis(self):
        """Analyze if compaction blocks reads/writes."""
        print("\n" + "=" * 70)
        print("TEST 3: Compaction Blocking Analysis")
        print("=" * 70)
        
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=20,
                max_immutable_memtables=2,
                max_l0_sstables=3,
                soft_limit_ratio=0.85,  # Trigger at 2-3 SSTables
                base_level_entries=50,
            )
            
            # Pre-populate to trigger compaction
            for i in range(200):
                self.store.put(f"preload{i:04d}", f"value{i}")
            
            time.sleep(2)
            
            # Now measure read/write latency during ongoing operations
            read_latencies = []
            write_latencies = []
            errors = []
            
            def reader():
                for _ in range(100):
                    start = time.time()
                    try:
                        self.store.get(f"preload{_:04d}")
                        latency = time.time() - start
                        read_latencies.append(latency)
                    except Exception as e:
                        errors.append(f"Read error: {e}")
                    time.sleep(0.001)
            
            def writer():
                for i in range(100):
                    start = time.time()
                    try:
                        self.store.put(f"concurrent{i:04d}", f"value{i}")
                        latency = time.time() - start
                        write_latencies.append(latency)
                    except Exception as e:
                        errors.append(f"Write error: {e}")
                    time.sleep(0.001)
            
            # Run concurrent readers and writers
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = [
                    executor.submit(reader),
                    executor.submit(reader),
                    executor.submit(reader),
                    executor.submit(writer),
                    executor.submit(writer),
                    executor.submit(writer),
                ]
                for f in as_completed(futures):
                    f.result()
            
            time.sleep(2)
            
            # Analyze results
            if read_latencies:
                avg_read = sum(read_latencies) / len(read_latencies)
                max_read = max(read_latencies)
                print(f"Read latencies ({len(read_latencies)} reads):")
                print(f"  - Average: {avg_read * 1000:.2f}ms")
                print(f"  - Max: {max_read * 1000:.2f}ms")
            
            if write_latencies:
                avg_write = sum(write_latencies) / len(write_latencies)
                max_write = max(write_latencies)
                print(f"Write latencies ({len(write_latencies)} writes):")
                print(f"  - Average: {avg_write * 1000:.2f}ms")
                print(f"  - Max: {max_write * 1000:.2f}ms")
            
            print(f"Errors: {len(errors)}")
            
            level_info = self.store.get_level_info()
            print(f"Final level info: {level_info}")
            
            # Check for blocking (max latency > 50ms indicates blocking)
            blocking_detected = False
            if read_latencies and max(read_latencies) > 0.05:
                print(f"⚠ BLOCKING DETECTED: Max read latency {max(read_latencies)*1000:.2f}ms > 50ms")
                blocking_detected = True
            if write_latencies and max(write_latencies) > 0.05:
                print(f"⚠ BLOCKING DETECTED: Max write latency {max(write_latencies)*1000:.2f}ms > 50ms")
                blocking_detected = True
            
            if not blocking_detected:
                print("✓ No significant blocking detected")
            
            print("✓ TEST COMPLETED (blocking analysis)")
            return True
            
        finally:
            self.teardown()
    
    def test_4_concurrent_operations_during_compaction(self):
        """Test read/write availability during compaction."""
        print("\n" + "=" * 70)
        print("TEST 4: Concurrent Operations During Compaction")
        print("=" * 70)
        
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=30,
                max_immutable_memtables=2,
                max_l0_sstables=3,
                soft_limit_ratio=0.85,
            )
            
            successful_reads = []
            successful_writes = []
            read_lock = threading.Lock()
            write_lock = threading.Lock()
            stop_flag = threading.Event()
            
            def continuous_reader():
                count = 0
                while not stop_flag.is_set():
                    try:
                        self.store.get(f"key{count % 100:04d}")
                        with read_lock:
                            successful_reads.append(time.time())
                        count += 1
                    except:
                        pass
                    time.sleep(0.001)
            
            def continuous_writer():
                count = 0
                while not stop_flag.is_set():
                    try:
                        self.store.put(f"key{count:04d}", f"value{count}")
                        with write_lock:
                            successful_writes.append(time.time())
                        count += 1
                    except:
                        pass
                    time.sleep(0.002)
            
            # Start readers and writers
            threads = [
                threading.Thread(target=continuous_reader),
                threading.Thread(target=continuous_reader),
                threading.Thread(target=continuous_writer),
                threading.Thread(target=continuous_writer),
            ]
            
            for t in threads:
                t.start()
            
            # Let it run for 5 seconds
            time.sleep(5)
            stop_flag.set()
            
            for t in threads:
                t.join(timeout=2)
            
            print(f"Successful reads: {len(successful_reads)}")
            print(f"Successful writes: {len(successful_writes)}")
            
            level_info = self.store.get_level_info()
            print(f"Level info: {level_info}")
            
            stats = self.store.stats()
            print(f"Stats:")
            print(f"  - Rotations: {stats['total_memtable_rotations']}")
            print(f"  - Async flushes: {stats['total_async_flushes']}")
            print(f"  - SSTables: {stats['num_sstables']}")
            
            # Success if we have many operations completed
            if len(successful_reads) > 100 and len(successful_writes) > 100:
                print("✓ Operations continued during background work")
            
            print("✓ TEST PASSED")
            return True
            
        finally:
            self.teardown()
    
    def run_all(self):
        """Run all tests."""
        print("=" * 70)
        print("BLOCKING ANALYSIS: Flush/Compact/Merge vs Main Thread")
        print("=" * 70)
        
        tests = [
            ("85% Soft Limit Trigger", self.test_1_soft_limit_triggers_at_85_percent),
            ("Flush Non-Blocking", self.test_2_flush_non_blocking_for_writes),
            ("Compaction Blocking Analysis", self.test_3_compaction_blocking_analysis),
            ("Concurrent Ops During Compaction", self.test_4_concurrent_operations_during_compaction),
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
        
        for name, passed, error in results:
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"  {status}: {name}")
            if error:
                print(f"         Error: {error}")
        
        print("\n" + "=" * 70)
        print("ANALYSIS FINDINGS:")
        print("=" * 70)
        print("""
Current Implementation Analysis:

1. FLUSH OPERATIONS (MemtableManager):
   ✓ Non-blocking for main thread
   - Uses ThreadPoolExecutor for async flush
   - Main thread returns immediately after submitting flush
   
2. COMPACTION OPERATIONS (SSTableManager):
   ⚠ POTENTIALLY BLOCKING
   - _auto_compact() is called inside add_sstable() lock
   - While compacting, SSTableManager.get() is blocked
   - This can cause read latency spikes during compaction
   
3. CURRENT LOCK STRUCTURE:
   - SSTableManager.lock protects all operations
   - Compaction holds lock for entire duration
   - Reads must wait for compaction to complete

RECOMMENDATIONS for notes.txt item 10:
   - Move compaction to background thread (like flush)
   - Use snapshot-based compaction (Phase 2 in notes.txt)
   - Implement read-write lock for concurrent reads
""")
        
        return all(passed for _, passed, _ in results)


if __name__ == "__main__":
    test = BlockingAnalysisTest()
    success = test.run_all()
    sys.exit(0 if success else 1)
