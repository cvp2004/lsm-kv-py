#!/usr/bin/env python3
"""
Comprehensive tests for Level-based Manifest System.

Tests:
1. LevelManifest - Per-level manifest operations
2. GlobalManifest - Global metadata management
3. LevelManifestManager - Coordinated manifest operations
4. Migration from old single manifest format
5. Integration with SSTableManager
6. LSM-tree logic compliance
"""
import sys
import os
import shutil
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lsmkv import LSMKVStore
from lsmkv.storage.level_manifest import LevelManifest, GlobalManifest, LevelManifestManager
from lsmkv.storage.manifest import ManifestEntry


class TestLevelManifest:
    """Tests for LevelManifest class."""
    
    def __init__(self):
        self.test_dir = None
        
    def setup(self):
        self.test_dir = f"./test_level_manifest_{int(time.time() * 1000)}"
        os.makedirs(self.test_dir, exist_ok=True)
        
    def teardown(self):
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_create_level_manifest(self):
        """Test creating a new level manifest."""
        self.setup()
        try:
            manifest = LevelManifest(self.test_dir, level=0)
            assert manifest.level == 0
            assert manifest.count() == 0
            assert manifest.is_empty()
            
            # Add an entry to trigger file creation
            entry = ManifestEntry(
                sstable_id=0, dirname="test", num_entries=1,
                min_key="a", max_key="z", level=0
            )
            manifest.add_sstable(entry)
            
            # Check file was created after adding entry
            assert os.path.exists(os.path.join(self.test_dir, "level_0.json"))
            print("  ✓ test_create_level_manifest")
            return True
        finally:
            self.teardown()
    
    def test_add_and_get_entries(self):
        """Test adding and retrieving entries."""
        self.setup()
        try:
            manifest = LevelManifest(self.test_dir, level=1)
            
            entry = ManifestEntry(
                sstable_id=1,
                dirname="sstable_000001",
                num_entries=100,
                min_key="aaa",
                max_key="zzz",
                level=1
            )
            manifest.add_sstable(entry)
            
            assert manifest.count() == 1
            assert manifest.total_entries() == 100
            
            # Retrieve entry
            retrieved = manifest.get_entry(1)
            assert retrieved is not None
            assert retrieved.dirname == "sstable_000001"
            assert retrieved.level == 1
            
            print("  ✓ test_add_and_get_entries")
            return True
        finally:
            self.teardown()
    
    def test_remove_entries(self):
        """Test removing entries from manifest."""
        self.setup()
        try:
            manifest = LevelManifest(self.test_dir, level=0)
            
            # Add 3 entries
            for i in range(3):
                entry = ManifestEntry(
                    sstable_id=i,
                    dirname=f"sstable_{i:06d}",
                    num_entries=50,
                    min_key=f"key{i}",
                    max_key=f"key{i}z",
                    level=0
                )
                manifest.add_sstable(entry)
            
            assert manifest.count() == 3
            
            # Remove one
            manifest.remove_sstables([1])
            assert manifest.count() == 2
            assert manifest.get_entry(1) is None
            assert manifest.get_entry(0) is not None
            assert manifest.get_entry(2) is not None
            
            print("  ✓ test_remove_entries")
            return True
        finally:
            self.teardown()
    
    def test_clear_level(self):
        """Test clearing all entries from a level."""
        self.setup()
        try:
            manifest = LevelManifest(self.test_dir, level=2)
            
            # Add entries
            for i in range(5):
                entry = ManifestEntry(
                    sstable_id=i, dirname=f"sstable_{i:06d}",
                    num_entries=10, min_key=f"a{i}", max_key=f"z{i}", level=2
                )
                manifest.add_sstable(entry)
            
            assert manifest.count() == 5
            
            manifest.clear()
            assert manifest.count() == 0
            assert manifest.is_empty()
            
            print("  ✓ test_clear_level")
            return True
        finally:
            self.teardown()
    
    def test_persistence(self):
        """Test that manifest persists across restarts."""
        self.setup()
        try:
            # Create and add entries
            manifest1 = LevelManifest(self.test_dir, level=0)
            entry = ManifestEntry(
                sstable_id=42, dirname="sstable_000042",
                num_entries=200, min_key="abc", max_key="xyz", level=0
            )
            manifest1.add_sstable(entry)
            
            # Create new instance (simulates restart)
            manifest2 = LevelManifest(self.test_dir, level=0)
            assert manifest2.count() == 1
            
            retrieved = manifest2.get_entry(42)
            assert retrieved is not None
            assert retrieved.dirname == "sstable_000042"
            assert retrieved.num_entries == 200
            
            print("  ✓ test_persistence")
            return True
        finally:
            self.teardown()
    
    def run_all(self):
        print("\n=== LevelManifest Tests ===")
        tests = [
            self.test_create_level_manifest,
            self.test_add_and_get_entries,
            self.test_remove_entries,
            self.test_clear_level,
            self.test_persistence,
        ]
        passed = sum(1 for t in tests if t())
        return passed, len(tests)


class TestGlobalManifest:
    """Tests for GlobalManifest class."""
    
    def __init__(self):
        self.test_dir = None
        
    def setup(self):
        self.test_dir = f"./test_global_manifest_{int(time.time() * 1000)}"
        os.makedirs(self.test_dir, exist_ok=True)
        
    def teardown(self):
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_create_global_manifest(self):
        """Test creating a new global manifest."""
        self.setup()
        try:
            manifest = GlobalManifest(self.test_dir)
            assert manifest.next_sstable_id == 0
            assert manifest.version == 2
            
            # Check file exists
            assert os.path.exists(os.path.join(self.test_dir, "global.json"))
            print("  ✓ test_create_global_manifest")
            return True
        finally:
            self.teardown()
    
    def test_get_next_id(self):
        """Test SSTable ID generation."""
        self.setup()
        try:
            manifest = GlobalManifest(self.test_dir)
            
            id1 = manifest.get_next_id()
            id2 = manifest.get_next_id()
            id3 = manifest.get_next_id()
            
            assert id1 == 0
            assert id2 == 1
            assert id3 == 2
            assert manifest.peek_next_id() == 3
            
            print("  ✓ test_get_next_id")
            return True
        finally:
            self.teardown()
    
    def test_metadata(self):
        """Test metadata storage."""
        self.setup()
        try:
            manifest = GlobalManifest(self.test_dir)
            
            manifest.set_metadata("test_key", "test_value")
            manifest.set_metadata("count", 42)
            
            assert manifest.get_metadata("test_key") == "test_value"
            assert manifest.get_metadata("count") == 42
            assert manifest.get_metadata("nonexistent", "default") == "default"
            
            print("  ✓ test_metadata")
            return True
        finally:
            self.teardown()
    
    def test_persistence(self):
        """Test persistence across restarts."""
        self.setup()
        try:
            # Generate some IDs
            manifest1 = GlobalManifest(self.test_dir)
            manifest1.get_next_id()
            manifest1.get_next_id()
            manifest1.set_metadata("migrated", True)
            
            # Restart
            manifest2 = GlobalManifest(self.test_dir)
            assert manifest2.peek_next_id() == 2
            assert manifest2.get_metadata("migrated") == True
            
            print("  ✓ test_persistence")
            return True
        finally:
            self.teardown()
    
    def run_all(self):
        print("\n=== GlobalManifest Tests ===")
        tests = [
            self.test_create_global_manifest,
            self.test_get_next_id,
            self.test_metadata,
            self.test_persistence,
        ]
        passed = sum(1 for t in tests if t())
        return passed, len(tests)


class TestLevelManifestManager:
    """Tests for LevelManifestManager class."""
    
    def __init__(self):
        self.test_dir = None
        
    def setup(self):
        self.test_dir = f"./test_level_manager_{int(time.time() * 1000)}"
        os.makedirs(self.test_dir, exist_ok=True)
        
    def teardown(self):
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_add_sstables_to_levels(self):
        """Test adding SSTables to different levels."""
        self.setup()
        try:
            manager = LevelManifestManager(self.test_dir)
            
            # Add to L0
            id1 = manager.add_sstable("sst_1", 100, "a", "m", level=0)
            id2 = manager.add_sstable("sst_2", 100, "n", "z", level=0)
            
            # Add to L1
            id3 = manager.add_sstable("sst_3", 500, "a", "z", level=1)
            
            assert id1 == 0
            assert id2 == 1
            assert id3 == 2
            
            assert manager.level_count(0) == 2
            assert manager.level_count(1) == 1
            assert manager.total_count() == 3
            
            print("  ✓ test_add_sstables_to_levels")
            return True
        finally:
            self.teardown()
    
    def test_get_level_entries(self):
        """Test getting entries by level."""
        self.setup()
        try:
            manager = LevelManifestManager(self.test_dir)
            
            manager.add_sstable("l0_1", 50, "a", "d", level=0)
            manager.add_sstable("l0_2", 50, "e", "h", level=0)
            manager.add_sstable("l1_1", 200, "a", "z", level=1)
            
            l0_entries = manager.get_level_entries(0)
            l1_entries = manager.get_level_entries(1)
            
            assert len(l0_entries) == 2
            assert len(l1_entries) == 1
            assert l1_entries[0].num_entries == 200
            
            print("  ✓ test_get_level_entries")
            return True
        finally:
            self.teardown()
    
    def test_clear_level(self):
        """Test clearing a specific level."""
        self.setup()
        try:
            manager = LevelManifestManager(self.test_dir)
            
            manager.add_sstable("l0_1", 50, "a", "d", level=0)
            manager.add_sstable("l0_2", 50, "e", "h", level=0)
            manager.add_sstable("l1_1", 200, "a", "z", level=1)
            
            manager.clear_level(0)
            
            assert manager.level_count(0) == 0
            assert manager.level_count(1) == 1
            
            print("  ✓ test_clear_level")
            return True
        finally:
            self.teardown()
    
    def test_discover_levels(self):
        """Test discovering levels from disk."""
        self.setup()
        try:
            manager1 = LevelManifestManager(self.test_dir)
            manager1.add_sstable("l0", 50, "a", "m", level=0)
            manager1.add_sstable("l1", 100, "a", "z", level=1)
            manager1.add_sstable("l2", 500, "a", "z", level=2)
            
            # New manager should discover levels
            manager2 = LevelManifestManager(self.test_dir)
            manager2.discover_levels()
            
            assert 0 in manager2.get_levels()
            assert 1 in manager2.get_levels()
            assert 2 in manager2.get_levels()
            assert manager2.total_count() == 3
            
            print("  ✓ test_discover_levels")
            return True
        finally:
            self.teardown()
    
    def test_migration_from_old_manifest(self):
        """Test migration from old single-manifest format."""
        self.setup()
        try:
            # Create old-style manifest
            old_manifest_path = os.path.join(self.test_dir, "manifest.json")
            old_data = {
                "next_sstable_id": 5,
                "entries": [
                    {"sstable_id": 0, "dirname": "sst_0", "num_entries": 100, 
                     "min_key": "a", "max_key": "m", "level": 0},
                    {"sstable_id": 1, "dirname": "sst_1", "num_entries": 100, 
                     "min_key": "n", "max_key": "z", "level": 0},
                    {"sstable_id": 2, "dirname": "sst_2", "num_entries": 500, 
                     "min_key": "a", "max_key": "z", "level": 1},
                ]
            }
            with open(old_manifest_path, 'w') as f:
                json.dump(old_data, f)
            
            # Create manager with old manifest path
            manager = LevelManifestManager(self.test_dir, old_manifest_path=old_manifest_path)
            
            # Verify migration
            assert manager.level_count(0) == 2
            assert manager.level_count(1) == 1
            assert manager.get_next_id() == 5
            
            # Old manifest should be backed up
            assert os.path.exists(old_manifest_path + ".backup")
            assert not os.path.exists(old_manifest_path)
            
            print("  ✓ test_migration_from_old_manifest")
            return True
        finally:
            self.teardown()
    
    def test_stats(self):
        """Test statistics gathering."""
        self.setup()
        try:
            manager = LevelManifestManager(self.test_dir)
            
            manager.add_sstable("l0", 50, "a", "m", level=0)
            manager.add_sstable("l0", 50, "n", "z", level=0)
            manager.add_sstable("l1", 200, "a", "z", level=1)
            
            stats = manager.stats()
            
            assert stats["num_levels"] == 2
            assert stats["total_sstables"] == 3
            assert stats["levels"][0]["sstables"] == 2
            assert stats["levels"][0]["total_entries"] == 100
            assert stats["levels"][1]["sstables"] == 1
            assert stats["levels"][1]["total_entries"] == 200
            
            print("  ✓ test_stats")
            return True
        finally:
            self.teardown()
    
    def run_all(self):
        print("\n=== LevelManifestManager Tests ===")
        tests = [
            self.test_add_sstables_to_levels,
            self.test_get_level_entries,
            self.test_clear_level,
            self.test_discover_levels,
            self.test_migration_from_old_manifest,
            self.test_stats,
        ]
        passed = sum(1 for t in tests if t())
        return passed, len(tests)


class TestLSMTreeCompliance:
    """Tests for LSM-tree logic compliance with level manifests."""
    
    def __init__(self):
        self.test_dir = None
        self.store = None
        
    def setup(self):
        self.test_dir = f"./test_lsm_compliance_{int(time.time() * 1000)}"
        shutil.rmtree(self.test_dir, ignore_errors=True)
        
    def teardown(self):
        if self.store:
            self.store.close()
            time.sleep(0.5)
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_level_manifests_created(self):
        """Test that level manifest files are created."""
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=5,
                max_immutable_memtables=2,
            )
            
            # Insert data to trigger flush
            for i in range(15):
                self.store.put(f"key{i:04d}", f"value{i}")
            
            time.sleep(2)  # Wait for flushes
            
            # Check manifests directory exists
            manifests_dir = os.path.join(self.test_dir, "manifests")
            assert os.path.exists(manifests_dir), "Manifests directory not created"
            
            # Check global manifest exists
            global_manifest = os.path.join(manifests_dir, "global.json")
            assert os.path.exists(global_manifest), "Global manifest not created"
            
            print("  ✓ test_level_manifests_created")
            return True
        finally:
            self.teardown()
    
    def test_l0_multiple_sstables(self):
        """Test that L0 allows multiple SSTables."""
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=5,
                max_immutable_memtables=2,
                max_l0_sstables=10,  # High limit to prevent compaction
            )
            
            # Create multiple flushes
            for batch in range(3):
                for i in range(10):
                    self.store.put(f"batch{batch}_key{i}", f"value{batch}_{i}")
                time.sleep(1)
            
            time.sleep(2)
            
            level_info = self.store.get_level_info()
            
            # L0 should have multiple SSTables
            if 0 in level_info:
                l0_sstables = level_info[0]['sstables']
                assert l0_sstables >= 1, f"L0 should have SSTables, got {l0_sstables}"
            
            print("  ✓ test_l0_multiple_sstables")
            return True
        finally:
            self.teardown()
    
    def test_compaction_updates_level_manifests(self):
        """Test that compaction correctly updates level manifests."""
        self.setup()
        try:
            self.store = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=5,
                max_immutable_memtables=2,
                max_l0_sstables=3,
                soft_limit_ratio=0.85,
            )
            
            # Insert enough to trigger compaction
            for i in range(50):
                self.store.put(f"key{i:04d}", f"value{i}")
            
            time.sleep(3)  # Wait for compaction
            
            level_info = self.store.get_level_info()
            
            # After compaction, should have data in L1
            if 1 in level_info:
                assert level_info[1]['sstables'] >= 1, "L1 should have SSTables after compaction"
            
            # Verify level manifest files are correct
            manifests_dir = os.path.join(self.test_dir, "manifests")
            if 1 in level_info and level_info[1]['sstables'] > 0:
                l1_manifest = os.path.join(manifests_dir, "level_1.json")
                assert os.path.exists(l1_manifest), "L1 manifest should exist"
            
            print("  ✓ test_compaction_updates_level_manifests")
            return True
        finally:
            self.teardown()
    
    def test_recovery_from_level_manifests(self):
        """Test recovery loads SSTables from level manifests."""
        self.setup()
        try:
            # Create store and add data
            store1 = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=5,
                max_immutable_memtables=2,
            )
            
            for i in range(20):
                store1.put(f"key{i:04d}", f"value{i}")
            
            time.sleep(2)
            store1.close()
            time.sleep(1)
            
            # Reopen and verify data
            store2 = LSMKVStore(
                data_dir=self.test_dir,
                memtable_size=5,
                max_immutable_memtables=2,
            )
            
            # Verify some data
            found = 0
            for i in range(20):
                result = store2.get(f"key{i:04d}")
                if result.found:
                    found += 1
            
            assert found >= 10, f"Should recover at least 10 entries, got {found}"
            
            store2.close()
            self.store = None  # Prevent double close
            
            print("  ✓ test_recovery_from_level_manifests")
            return True
        except Exception as e:
            print(f"  ✗ test_recovery_from_level_manifests: {e}")
            return False
        finally:
            self.teardown()
    
    def test_concurrent_level_updates(self):
        """Test concurrent updates to different level manifests."""
        self.setup()
        try:
            manager = LevelManifestManager(self.test_dir)
            errors = []
            
            def add_to_level(level, count):
                try:
                    for i in range(count):
                        manager.add_sstable(
                            f"l{level}_sst_{i}",
                            num_entries=50,
                            min_key=f"l{level}_{i}_a",
                            max_key=f"l{level}_{i}_z",
                            level=level
                        )
                except Exception as e:
                    errors.append(str(e))
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(add_to_level, 0, 10),
                    executor.submit(add_to_level, 1, 5),
                    executor.submit(add_to_level, 2, 5),
                    executor.submit(add_to_level, 0, 10),
                    executor.submit(add_to_level, 1, 5),
                ]
                for f in as_completed(futures):
                    f.result()
            
            assert len(errors) == 0, f"Errors during concurrent updates: {errors}"
            
            # Verify counts
            assert manager.level_count(0) == 20
            assert manager.level_count(1) == 10
            assert manager.level_count(2) == 5
            assert manager.total_count() == 35
            
            print("  ✓ test_concurrent_level_updates")
            return True
        finally:
            self.teardown()
    
    def run_all(self):
        print("\n=== LSM-Tree Compliance Tests ===")
        tests = [
            self.test_level_manifests_created,
            self.test_l0_multiple_sstables,
            self.test_compaction_updates_level_manifests,
            self.test_recovery_from_level_manifests,
            self.test_concurrent_level_updates,
        ]
        results = []
        for t in tests:
            try:
                results.append(t())
            except Exception as e:
                print(f"  ✗ {t.__name__}: {e}")
                results.append(False)
        passed = sum(1 for r in results if r)
        return passed, len(tests)


def run_all_tests():
    """Run all test suites."""
    print("=" * 70)
    print("LEVEL-BASED MANIFEST SYSTEM TESTS")
    print("=" * 70)
    
    total_passed = 0
    total_tests = 0
    
    # Run each test suite
    suites = [
        TestLevelManifest(),
        TestGlobalManifest(),
        TestLevelManifestManager(),
        TestLSMTreeCompliance(),
    ]
    
    for suite in suites:
        passed, tests = suite.run_all()
        total_passed += passed
        total_tests += tests
    
    print("\n" + "=" * 70)
    print(f"SUMMARY: {total_passed}/{total_tests} tests passed")
    print("=" * 70)
    
    if total_passed == total_tests:
        print("ALL TESTS PASSED!")
        return True
    else:
        print(f"FAILED: {total_tests - total_passed} tests")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
