#!/usr/bin/env python3
"""
Test runner for all test suites.
"""
import subprocess
import sys
import os


def run_test(test_file):
    """Run a single test file."""
    test_name = os.path.basename(test_file)
    print(f"\n{'=' * 60}")
    print(f"Running {test_name}")
    print('=' * 60)
    
    result = subprocess.run(
        [sys.executable, test_file],
        capture_output=False,
        cwd=os.path.dirname(os.path.abspath(__file__))
    )
    
    return result.returncode == 0


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("LSM KV Store - Complete Test Suite")
    print("=" * 60)
    
    test_files = [
        "tests/test_kvstore.py",
        "tests/test_flush.py",
        "tests/test_compact.py",
        "tests/test_background_flush.py",
        "tests/test_memtable_manager.py"
    ]
    
    results = {}
    for test_file in test_files:
        success = run_test(test_file)
        results[test_file] = success
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_file, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status}: {os.path.basename(test_file)}")
    
    print("=" * 60)
    
    all_passed = all(results.values())
    if all_passed:
        print("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        print("\n❌ SOME TESTS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
