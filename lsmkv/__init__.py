"""
LSM KV Store - A simple LSM tree-based key-value store implementation.
"""

from lsmkv.core.kvstore import LSMKVStore
from lsmkv.core.dto import Entry, WALRecord, GetResult, OperationType
from lsmkv.core.sstable_manager import SSTableManager
from lsmkv.storage.bloom_filter import BloomFilter
from lsmkv.storage.sparse_index import SparseIndex

__version__ = "1.0.0"
__all__ = [
    "LSMKVStore", 
    "Entry", 
    "WALRecord", 
    "GetResult", 
    "OperationType",
    "SSTableManager",
    "BloomFilter",
    "SparseIndex"
]
