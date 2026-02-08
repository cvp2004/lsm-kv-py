"""
Bloom filter wrapper using pybloomfiltermmap3 for efficient mmap-based operations.

Requires: pip install pybloomfiltermmap3
"""
import os
from typing import Optional

try:
    from pybloomfilter import BloomFilter as PyBloomFilter
except ImportError:
    raise ImportError(
        "pybloomfiltermmap3 is required but not installed.\n"
        "Install it with: pip install pybloomfiltermmap3\n"
        "Or install all dependencies: pip install -r requirements.txt"
    )


class BloomFilter:
    """
    Bloom filter wrapper using pybloomfiltermmap3.
    Provides mmap-backed Bloom filter with automatic persistence.
    
    Features:
    - Native mmap support for file-backed filters
    - Optimized C implementation for performance
    - Automatic persistence to disk
    - Minimal memory footprint
    """
    
    def __init__(self, expected_elements: int = 1000, false_positive_rate: float = 0.01, 
                 filepath: Optional[str] = None):
        """
        Initialize a Bloom filter.
        
        Args:
            expected_elements: Expected number of elements to insert
            false_positive_rate: Desired false positive rate (0.0 to 1.0)
            filepath: Optional file path for mmap-backed filter
        """
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        self.filepath = filepath
        self._bloom = None
        
        if filepath and os.path.exists(filepath):
            # Load existing mmap-backed filter
            self._bloom = PyBloomFilter.open(filepath)
        elif filepath:
            # Create new mmap-backed filter
            self._bloom = PyBloomFilter(expected_elements, false_positive_rate, filepath)
        else:
            # Create in-memory filter
            self._bloom = PyBloomFilter(expected_elements, false_positive_rate)
    
    def add(self, key: str):
        """
        Add a key to the Bloom filter.
        
        Args:
            key: The key to add
        """
        self._bloom.add(key)
    
    def might_contain(self, key: str) -> bool:
        """
        Check if a key might be in the set.
        
        Args:
            key: The key to check
            
        Returns:
            True if key might be in the set (or false positive)
            False if key is definitely not in the set
        """
        return key in self._bloom
    
    def save_to_file(self, filepath: str):
        """
        Save the Bloom filter to a file.
        
        Args:
            filepath: Path to save the Bloom filter
        """
        if self.filepath and self.filepath == filepath:
            # Already saved (mmap-backed), just sync
            self._bloom.sync()
        else:
            # Copy to new file
            self._bloom.copy_template(filepath)
    
    @staticmethod
    def load_from_file(filepath: str) -> 'BloomFilter':
        """
        Load a Bloom filter from a file.
        
        Args:
            filepath: Path to the Bloom filter file
            
        Returns:
            Loaded Bloom filter
        """
        # Create with filepath - will load existing file
        return BloomFilter(1000, 0.01, filepath)
    
    def close(self):
        """Close the Bloom filter and sync to disk if file-backed."""
        if self._bloom and self.filepath:
            self._bloom.sync()
    
    def __contains__(self, key: str) -> bool:
        """Support 'in' operator."""
        return self.might_contain(key)
    
    def __str__(self) -> str:
        """String representation of the Bloom filter."""
        return (f"BloomFilter(capacity={self.expected_elements}, "
                f"error_rate={self.false_positive_rate:.4f}, "
                f"file={'yes' if self.filepath else 'no'})")
