"""
LSM KV Store - A simple LSM tree-based key-value store implementation.
"""

from lsmkv.core.kvstore import LSMKVStore
from lsmkv.core.dto import Entry, WALRecord, GetResult, OperationType
from lsmkv.core.sstable_manager import SSTableManager
from lsmkv.storage.bloom_filter import BloomFilter
from lsmkv.storage.sparse_index import SparseIndex
from lsmkv.storage.sstable import SSTable, SSTableMetadata, LazySSTable
from lsmkv.storage.level_manifest import LevelManifest, LevelManifestManager, GlobalManifest

__version__ = "1.2.0"  # Lazy loading and non-blocking compaction
__all__ = [
    "LSMKVStore", 
    "Entry", 
    "WALRecord", 
    "GetResult", 
    "OperationType",
    "SSTableManager",
    "BloomFilter",
    "SparseIndex",
    "SSTable",
    "SSTableMetadata",
    "LazySSTable",
    "LevelManifest",
    "LevelManifestManager",
    "GlobalManifest"
]
