"""
Manifest file implementation for storing SSTable metadata.
"""
import json
import os
from typing import List, Optional
from threading import Lock


class ManifestEntry:
    """Entry in the manifest file."""
    
    def __init__(self, sstable_id: int, dirname: str, num_entries: int, 
                 min_key: str, max_key: str, level: int = 0):
        """
        Initialize a manifest entry.
        
        Args:
            sstable_id: Unique ID for the SSTable
            dirname: Directory name for the SSTable (e.g., sstable_000001)
            num_entries: Number of entries in the SSTable
            min_key: Smallest key in the SSTable
            max_key: Largest key in the SSTable
            level: Level in the LSM tree (0 for L0)
        """
        self.sstable_id = sstable_id
        self.dirname = dirname
        self.num_entries = num_entries
        self.min_key = min_key
        self.max_key = max_key
        self.level = level
        
        # Legacy support: filename is same as dirname for backward compatibility
        self.filename = dirname
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "sstable_id": self.sstable_id,
            "dirname": self.dirname,
            "num_entries": self.num_entries,
            "min_key": self.min_key,
            "max_key": self.max_key,
            "level": self.level
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'ManifestEntry':
        """Create from dictionary."""
        # Support both old 'filename' and new 'dirname' fields
        dirname = data.get("dirname") or data.get("filename", "")
        
        return ManifestEntry(
            sstable_id=data["sstable_id"],
            dirname=dirname,
            num_entries=data["num_entries"],
            min_key=data["min_key"],
            max_key=data["max_key"],
            level=data.get("level", 0)
        )


class Manifest:
    """Manifest file for tracking SSTable metadata."""
    
    def __init__(self, filepath: str):
        """
        Initialize the manifest.
        
        Args:
            filepath: Path to the manifest file
        """
        self.filepath = filepath
        self.entries: List[ManifestEntry] = []
        self.next_sstable_id = 0
        self.lock = Lock()
        self._load()
    
    def _load(self):
        """Load manifest from disk."""
        if not os.path.exists(self.filepath):
            return
        
        with self.lock:
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    self.next_sstable_id = data.get("next_sstable_id", 0)
                    self.entries = [
                        ManifestEntry.from_dict(entry) 
                        for entry in data.get("entries", [])
                    ]
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load manifest: {e}")
                self.entries = []
                self.next_sstable_id = 0
    
    def _save(self):
        """Save manifest to disk."""
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        
        data = {
            "next_sstable_id": self.next_sstable_id,
            "entries": [entry.to_dict() for entry in self.entries]
        }
        
        # Write to temp file first, then rename for atomicity
        temp_filepath = self.filepath + ".tmp"
        with open(temp_filepath, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        
        os.replace(temp_filepath, self.filepath)
    
    def add_sstable(self, dirname: str, num_entries: int, 
                    min_key: str, max_key: str, level: int = 0, sstable_id: Optional[int] = None) -> int:
        """
        Add a new SSTable to the manifest.
        
        Args:
            dirname: Directory name for the SSTable
            num_entries: Number of entries
            min_key: Smallest key
            max_key: Largest key
            level: Level in LSM tree
            sstable_id: Optional SSTable ID (if None, uses next_sstable_id)
            
        Returns:
            SSTable ID
        """
        with self.lock:
            if sstable_id is None:
                sstable_id = self.next_sstable_id
                self.next_sstable_id += 1
            else:
                # Ensure next_sstable_id is updated
                if sstable_id >= self.next_sstable_id:
                    self.next_sstable_id = sstable_id + 1
            
            entry = ManifestEntry(
                sstable_id=sstable_id,
                dirname=dirname,
                num_entries=num_entries,
                min_key=min_key,
                max_key=max_key,
                level=level
            )
            self.entries.append(entry)
            self._save()
            
            return sstable_id
    
    def remove_sstables(self, sstable_ids: List[int]):
        """
        Remove SSTables from the manifest.
        
        Args:
            sstable_ids: List of SSTable IDs to remove
        """
        with self.lock:
            self.entries = [
                entry for entry in self.entries 
                if entry.sstable_id not in sstable_ids
            ]
            self._save()
    
    def get_all_entries(self) -> List[ManifestEntry]:
        """Get all manifest entries."""
        with self.lock:
            return self.entries.copy()
    
    def get_next_id(self) -> int:
        """Get the next SSTable ID without incrementing."""
        with self.lock:
            return self.next_sstable_id
