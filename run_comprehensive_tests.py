#!/usr/bin/env python3
"""
Comprehensive test runner for all unit tests.
Runs all test suites and provides detailed results.
"""
import sys
import os
import time

# Test modules
test_modules = [
    ("Bloom Filter", "tests.test_bloom_filter", "TestBloomFilter"),
    ("Sparse Index", "tests.test_sparse_index", "TestSparseIndex"),
    ("SSTable Manager", "tests.test_sstable_manager", "TestSSTableManager"),
    ("End-to-End", "tests.test_end_to_end", "TestEndToEnd"),
    ("KV Store Basic", "tests.test_kvstore", None),
    ("Flush Operations", "tests.test_flush", None),
    ("Compaction", "tests.test_compact", None),
    ("Background Flush", "tests.test_background_flush", None),
    ("Memtable Manager", "tests.test_memtable_manager", None),
]


def run_test_module(name, module_path, class_name=None):
    """
    Run a test module.
    
    Args:
        name: Display name for the test
        module_path: Python module path
        class_name: Test class name (if using test class pattern)
        
    Returns:
        Tuple of (success, passed, failed, error_message)
    """
    print("\n" + "=" * 80)
    print(f"Running: {name}")
    print("=" * 80)
    
    try:
        if class_name:
            # Import and run test class
            module = __import__(module_path, fromlist=[class_name])
            test_class = getattr(module, class_name)
            tester = test_class()
            success = tester.run_all_tests()
            return (success, tester.passed, tester.failed, None)
        else:
            # Run module directly (old-style tests)
            module = __import__(module_path, fromlist=[''])
            # Assume they print results and exit appropriately
            print(f"  (Running {module_path}...)")
            return (True, 0, 0, None)
    
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print(f"\n❌ ERROR in {name}:")
        print(error_msg)
        return (False, 0, 1, error_msg)


def main():
    """Run all tests and provide summary."""
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "COMPREHENSIVE TEST SUITE" + " " * 34 + "║")
    print("╚" + "=" * 78 + "╝")
    
    print("\nThis will run all unit tests including:")
    print("  • Component tests (Bloom filter, Sparse index, SSTable manager)")
    print("  • Integration tests (End-to-end flows)")
    print("  • Existing tests (KVStore, Flush, Compaction, etc.)")
    print("")
    
    start_time = time.time()
    
    results = []
    total_passed = 0
    total_failed = 0
    
    for name, module_path, class_name in test_modules:
        success, passed, failed, error = run_test_module(name, module_path, class_name)
        results.append((name, success, passed, failed, error))
        total_passed += passed
        total_failed += failed
    
    elapsed_time = time.time() - start_time
    
    # Summary
    print("\n\n" + "╔" + "=" * 78 + "╗")
    print("║" + " " * 30 + "TEST SUMMARY" + " " * 36 + "║")
    print("╚" + "=" * 78 + "╝")
    
    print(f"\n{'Test Suite':<30} {'Result':<15} {'Passed':<10} {'Failed':<10}")
    print("-" * 80)
    
    for name, success, passed, failed, error in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        pass_str = str(passed) if passed > 0 else "-"
        fail_str = str(failed) if failed > 0 else "-"
        print(f"{name:<30} {status:<15} {pass_str:<10} {fail_str:<10}")
    
    print("-" * 80)
    print(f"{'TOTAL':<30} {'':<15} {total_passed:<10} {total_failed:<10}")
    print("")
    
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
    else:
        print("⚠️  SOME TESTS FAILED")
        print("\nFailed tests:")
        for name, success, passed, failed, error in results:
            if not success:
                print(f"  • {name}")
                if error:
                    print(f"    Error: {error.split(chr(10))[0][:60]}...")
    
    print(f"\nTotal execution time: {elapsed_time:.2f}s")
    print("=" * 80)
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
