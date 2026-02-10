"""
Level-based Manifest implementation for LSM-tree.

This module provides per-level manifest files for better organization
and isolation of SSTable metadata at each level.

Directory structure:
    data/
    ├── manifests/
    │   ├── level_0.json     # L0 manifest
    │   ├── level_1.json     # L1 manifest
    │   ├── level_2.json     # L2 manifest
    │   └── global.json      # Global metadata (next_sstable_id, etc.)
    └── sstables/
        └── ...
"""
import json
import os
from typing import List, Optional, Dict
from threading import RLock
from lsmkv.storage.manifest import ManifestEntry


class LevelManifest:
    """
    Manifest for a specific level in the LSM-tree.
    
    Each level has its own manifest file containing only the SSTables
    at that level. This enables:
    - Isolated updates per level
    - Faster manifest operations (smaller files)
    - Concurrent updates to different levels
    - Easier recovery and debugging
    """
    
    def __init__(self, manifest_dir: str, level: int):
        """
        Initialize a level manifest.
        
        Args:
            manifest_dir: Directory containing all manifest files
            level: Level number (0, 1, 2, ...)
        """
        self.manifest_dir = manifest_dir
        self.level = level
        self.filepath = os.path.join(manifest_dir, f"level_{level}.json")
        self.entries: List[ManifestEntry] = []
        self.lock = RLock()
        
        os.makedirs(manifest_dir, exist_ok=True)
        self._load()
    
    def _load(self):
        """Load manifest from disk."""
        if not os.path.exists(self.filepath):
            return
        
        with self.lock:
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    self.entries = [
                        ManifestEntry.from_dict(entry)
                        for entry in data.get("entries", [])
                    ]
                    # Ensure all entries have correct level
                    for entry in self.entries:
                        entry.level = self.level
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load level {self.level} manifest: {e}")
                self.entries = []
    
    def _save(self):
        """Save manifest to disk atomically."""
        os.makedirs(self.manifest_dir, exist_ok=True)
        
        data = {
            "level": self.level,
            "entries": [entry.to_dict() for entry in self.entries]
        }
        
        # Atomic write: temp file + rename
        temp_filepath = self.filepath + ".tmp"
        with open(temp_filepath, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        
        os.replace(temp_filepath, self.filepath)
    
    def add_sstable(self, entry: ManifestEntry):
        """
        Add an SSTable entry to this level's manifest.
        
        Args:
            entry: ManifestEntry to add
        """
        with self.lock:
            # Ensure level is correct
            entry.level = self.level
            self.entries.append(entry)
            self._save()
    
    def remove_sstables(self, sstable_ids: List[int]):
        """
        Remove SSTables from this level's manifest.
        
        Args:
            sstable_ids: List of SSTable IDs to remove
        """
        with self.lock:
            self.entries = [
                entry for entry in self.entries
                if entry.sstable_id not in sstable_ids
            ]
            self._save()
    
    def clear(self):
        """Remove all entries from this level's manifest."""
        with self.lock:
            self.entries = []
            self._save()
    
    def get_all_entries(self) -> List[ManifestEntry]:
        """Get all entries in this level."""
        with self.lock:
            return self.entries.copy()
    
    def get_entry(self, sstable_id: int) -> Optional[ManifestEntry]:
        """Get a specific entry by SSTable ID."""
        with self.lock:
            for entry in self.entries:
                if entry.sstable_id == sstable_id:
                    return entry
            return None
    
    def count(self) -> int:
        """Get number of SSTables in this level."""
        with self.lock:
            return len(self.entries)
    
    def is_empty(self) -> bool:
        """Check if this level has no SSTables."""
        return self.count() == 0
    
    def total_entries(self) -> int:
        """Get total number of entries across all SSTables in this level."""
        with self.lock:
            return sum(entry.num_entries for entry in self.entries)
    
    def __len__(self) -> int:
        return self.count()
    
    def __str__(self) -> str:
        return f"LevelManifest(level={self.level}, sstables={self.count()})"


class GlobalManifest:
    """
    Global manifest for cross-level metadata.
    
    Stores:
    - next_sstable_id: Global SSTable ID counter
    - version: Manifest format version
    - metadata: Additional global metadata
    """
    
    def __init__(self, manifest_dir: str):
        """
        Initialize global manifest.
        
        Args:
            manifest_dir: Directory containing all manifest files
        """
        self.manifest_dir = manifest_dir
        self.filepath = os.path.join(manifest_dir, "global.json")
        self.next_sstable_id = 0
        self.version = 2  # Version 2 = level-based manifests
        self.metadata: Dict = {}
        self.lock = RLock()
        
        os.makedirs(manifest_dir, exist_ok=True)
        self._load()
        
        # Ensure file exists even for new manifests
        if not os.path.exists(self.filepath):
            self._save()
    
    def _load(self):
        """Load global manifest from disk."""
        if not os.path.exists(self.filepath):
            return
        
        with self.lock:
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    self.next_sstable_id = data.get("next_sstable_id", 0)
                    self.version = data.get("version", 2)
                    self.metadata = data.get("metadata", {})
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load global manifest: {e}")
    
    def _save(self):
        """Save global manifest to disk."""
        os.makedirs(self.manifest_dir, exist_ok=True)
        
        data = {
            "next_sstable_id": self.next_sstable_id,
            "version": self.version,
            "metadata": self.metadata
        }
        
        temp_filepath = self.filepath + ".tmp"
        with open(temp_filepath, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        
        os.replace(temp_filepath, self.filepath)
    
    def get_next_id(self) -> int:
        """Get next SSTable ID and increment counter."""
        with self.lock:
            current_id = self.next_sstable_id
            self.next_sstable_id += 1
            self._save()
            return current_id
    
    def peek_next_id(self) -> int:
        """Get next SSTable ID without incrementing."""
        with self.lock:
            return self.next_sstable_id
    
    def set_next_id(self, next_id: int):
        """Set the next SSTable ID (used during migration)."""
        with self.lock:
            if next_id > self.next_sstable_id:
                self.next_sstable_id = next_id
                self._save()
    
    def set_metadata(self, key: str, value):
        """Set a metadata value."""
        with self.lock:
            self.metadata[key] = value
            self._save()
    
    def get_metadata(self, key: str, default=None):
        """Get a metadata value."""
        with self.lock:
            return self.metadata.get(key, default)


class LevelManifestManager:
    """
    Manager for all level manifests.
    
    Provides a unified interface to manage SSTables across all levels
    while using separate manifest files per level.
    
    Features:
    - Lazy loading of level manifests
    - Global SSTable ID management
    - Migration from old single-manifest format
    - Atomic cross-level operations
    """
    
    def __init__(self, data_dir: str, old_manifest_path: Optional[str] = None):
        """
        Initialize the level manifest manager.
        
        Args:
            data_dir: Base data directory
            old_manifest_path: Path to old single manifest (for migration)
        """
        self.data_dir = data_dir
        self.manifest_dir = os.path.join(data_dir, "manifests")
        self.old_manifest_path = old_manifest_path
        
        # Global manifest for cross-level metadata
        self.global_manifest = GlobalManifest(self.manifest_dir)
        
        # Level manifests (loaded on demand)
        self._level_manifests: Dict[int, LevelManifest] = {}
        self.lock = RLock()
        
        # Migrate from old format if needed
        self._migrate_if_needed()
    
    def _migrate_if_needed(self):
        """Migrate from old single-manifest format if old manifest exists."""
        if not self.old_manifest_path or not os.path.exists(self.old_manifest_path):
            return
        
        # Check if already migrated
        if self.global_manifest.get_metadata("migrated_from_v1"):
            return
        
        print("[LevelManifestManager] Migrating from single manifest to level-based manifests...")
        
        try:
            with open(self.old_manifest_path, 'r') as f:
                old_data = json.load(f)
            
            # Get next SSTable ID
            old_next_id = old_data.get("next_sstable_id", 0)
            self.global_manifest.set_next_id(old_next_id)
            
            # Migrate entries to level manifests
            old_entries = old_data.get("entries", [])
            for entry_data in old_entries:
                entry = ManifestEntry.from_dict(entry_data)
                level = entry.level
                level_manifest = self._get_or_create_level_manifest(level)
                level_manifest.add_sstable(entry)
            
            # Mark as migrated
            self.global_manifest.set_metadata("migrated_from_v1", True)
            self.global_manifest.set_metadata("migration_time", 
                __import__('time').strftime("%Y-%m-%d %H:%M:%S"))
            
            # Rename old manifest (backup)
            backup_path = self.old_manifest_path + ".backup"
            os.rename(self.old_manifest_path, backup_path)
            
            print(f"[LevelManifestManager] Migrated {len(old_entries)} entries, old manifest backed up")
            
        except Exception as e:
            print(f"Warning: Migration failed: {e}")
    
    def _get_or_create_level_manifest(self, level: int) -> LevelManifest:
        """Get or create a manifest for a specific level."""
        with self.lock:
            if level not in self._level_manifests:
                self._level_manifests[level] = LevelManifest(self.manifest_dir, level)
            return self._level_manifests[level]
    
    def get_level_manifest(self, level: int) -> LevelManifest:
        """Get the manifest for a specific level."""
        return self._get_or_create_level_manifest(level)
    
    def add_sstable(self, dirname: str, num_entries: int, 
                    min_key: str, max_key: str, level: int = 0,
                    sstable_id: Optional[int] = None) -> int:
        """
        Add an SSTable to the appropriate level manifest.
        
        Args:
            dirname: Directory name for the SSTable
            num_entries: Number of entries
            min_key: Smallest key
            max_key: Largest key
            level: Level to add to
            sstable_id: Optional SSTable ID (if None, auto-assigns)
            
        Returns:
            Assigned SSTable ID
        """
        with self.lock:
            if sstable_id is None:
                sstable_id = self.global_manifest.get_next_id()
            else:
                # Ensure global ID counter is updated
                self.global_manifest.set_next_id(sstable_id + 1)
            
            entry = ManifestEntry(
                sstable_id=sstable_id,
                dirname=dirname,
                num_entries=num_entries,
                min_key=min_key,
                max_key=max_key,
                level=level
            )
            
            level_manifest = self._get_or_create_level_manifest(level)
            level_manifest.add_sstable(entry)
            
            return sstable_id
    
    def remove_sstables(self, sstable_ids: List[int], level: Optional[int] = None):
        """
        Remove SSTables from manifest(s).
        
        Args:
            sstable_ids: List of SSTable IDs to remove
            level: If specified, only remove from this level
        """
        with self.lock:
            if level is not None:
                # Remove from specific level
                if level in self._level_manifests:
                    self._level_manifests[level].remove_sstables(sstable_ids)
            else:
                # Remove from all levels
                for level_manifest in self._level_manifests.values():
                    level_manifest.remove_sstables(sstable_ids)
    
    def clear_level(self, level: int):
        """Clear all SSTables from a specific level."""
        with self.lock:
            if level in self._level_manifests:
                self._level_manifests[level].clear()
    
    def get_all_entries(self) -> List[ManifestEntry]:
        """Get all entries from all levels."""
        with self.lock:
            all_entries = []
            for level in sorted(self._level_manifests.keys()):
                all_entries.extend(self._level_manifests[level].get_all_entries())
            return all_entries
    
    def get_level_entries(self, level: int) -> List[ManifestEntry]:
        """Get all entries for a specific level."""
        with self.lock:
            if level in self._level_manifests:
                return self._level_manifests[level].get_all_entries()
            return []
    
    def get_entry(self, sstable_id: int) -> Optional[ManifestEntry]:
        """Find an entry by SSTable ID across all levels."""
        with self.lock:
            for level_manifest in self._level_manifests.values():
                entry = level_manifest.get_entry(sstable_id)
                if entry:
                    return entry
            return None
    
    def get_next_id(self) -> int:
        """Get the next SSTable ID without incrementing."""
        return self.global_manifest.peek_next_id()
    
    def get_levels(self) -> List[int]:
        """Get list of all levels that have manifests."""
        with self.lock:
            return sorted(self._level_manifests.keys())
    
    def level_count(self, level: int) -> int:
        """Get number of SSTables at a specific level."""
        with self.lock:
            if level in self._level_manifests:
                return self._level_manifests[level].count()
            return 0
    
    def total_count(self) -> int:
        """Get total number of SSTables across all levels."""
        with self.lock:
            return sum(lm.count() for lm in self._level_manifests.values())
    
    def reload_level(self, level: int):
        """Reload a specific level's manifest from disk."""
        with self.lock:
            if level in self._level_manifests:
                del self._level_manifests[level]
            self._get_or_create_level_manifest(level)
    
    def reload_all(self):
        """Reload all manifests from disk."""
        with self.lock:
            levels = list(self._level_manifests.keys())
            self._level_manifests.clear()
            for level in levels:
                self._get_or_create_level_manifest(level)
    
    def discover_levels(self):
        """Discover and load all existing level manifests from disk."""
        with self.lock:
            if not os.path.exists(self.manifest_dir):
                return
            
            for filename in os.listdir(self.manifest_dir):
                if filename.startswith("level_") and filename.endswith(".json"):
                    try:
                        level = int(filename[6:-5])  # "level_X.json" -> X
                        self._get_or_create_level_manifest(level)
                    except ValueError:
                        continue
    
    def stats(self) -> Dict:
        """Get statistics about all level manifests."""
        with self.lock:
            level_stats = {}
            for level in sorted(self._level_manifests.keys()):
                lm = self._level_manifests[level]
                level_stats[level] = {
                    "sstables": lm.count(),
                    "total_entries": lm.total_entries()
                }
            
            return {
                "num_levels": len(self._level_manifests),
                "total_sstables": self.total_count(),
                "next_sstable_id": self.global_manifest.peek_next_id(),
                "levels": level_stats
            }
    
    def __str__(self) -> str:
        return f"LevelManifestManager(levels={len(self._level_manifests)}, sstables={self.total_count()})"
