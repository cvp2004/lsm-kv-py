#!/usr/bin/env python3
"""
Command-Line Interface for the LSM KV Store.
"""
import sys
import os
import cmd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsmkv import LSMKVStore


class KVStoreCLI(cmd.Cmd):
    """Interactive CLI for the KV Store."""
    
    intro = """
╔══════════════════════════════════════════╗
║    LSM-based Key-Value Store CLI        ║
╚══════════════════════════════════════════╝

Type 'help' for available commands.
    """
    prompt = "kvstore> "
    
    def __init__(self, data_dir: str = "./data"):
        super().__init__()
        self.store = LSMKVStore(data_dir=data_dir)
    
    def do_put(self, arg):
        """
        Insert or update a key-value pair.
        Usage: put <key> <value>
        Example: put name Alice
        """
        args = arg.split(maxsplit=1)
        if len(args) != 2:
            print("Error: put requires exactly 2 arguments: key and value")
            print("Usage: put <key> <value>")
            return
        
        key, value = args
        success = self.store.put(key, value)
        if success:
            print(f"OK: Set '{key}' = '{value}'")
        else:
            print(f"Error: Failed to set '{key}'")
    
    def do_set(self, arg):
        """
        Insert or update a key-value pair (alias for put).
        Usage: set <key> <value>
        Example: set name Alice
        """
        self.do_put(arg)
    
    def do_get(self, arg):
        """
        Retrieve a value by key.
        Usage: get <key>
        Example: get name
        """
        if not arg:
            print("Error: get requires a key argument")
            print("Usage: get <key>")
            return
        
        key = arg.strip()
        result = self.store.get(key)
        
        if result.found:
            print(f"{result.value}")
        else:
            print(f"Key '{key}' not found")
    
    def do_delete(self, arg):
        """
        Delete a key-value pair.
        Usage: delete <key>
        Example: delete name
        """
        if not arg:
            print("Error: delete requires a key argument")
            print("Usage: delete <key>")
            return
        
        key = arg.strip()
        success = self.store.delete(key)
        if success:
            print(f"OK: Deleted '{key}'")
        else:
            print(f"Error: Failed to delete '{key}'")
    
    def do_flush(self, arg):
        """
        Flush the memtable to an SSTable on disk.
        Usage: flush
        """
        try:
            metadata = self.store.flush()
            print(f"OK: Flushed memtable to SSTable")
            print(f"  File: {metadata.filename}")
            print(f"  Entries: {metadata.num_entries}")
            print(f"  Key Range: [{metadata.min_key}, {metadata.max_key}]")
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error: Failed to flush memtable: {e}")
    
    def do_compact(self, arg):
        """
        Compact all SSTables into a single SSTable.
        Usage: compact
        """
        try:
            metadata = self.store.compact()
            print(f"OK: Compacted all SSTables")
            print(f"  File: {metadata.filename}")
            print(f"  Entries: {metadata.num_entries}")
            print(f"  Key Range: [{metadata.min_key}, {metadata.max_key}]")
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error: Failed to compact SSTables: {e}")
    
    def do_stats(self, arg):
        """
        Display statistics about the store.
        Usage: stats
        """
        stats = self.store.stats()
        print("\n=== Store Statistics ===")
        
        # Active Memtable
        print(f"Active Memtable: {stats['active_memtable_size']} / {stats['memtable_max_size']}")
        print(f"Active Full: {stats['active_memtable_full']}")
        
        # Immutable Queue
        print(f"\nImmutable Memtables: {stats['immutable_memtables']} / {stats['max_immutable_memtables']}")
        if stats['immutable_memtables'] > 0:
            mem_kb = stats['immutable_memory_bytes'] / 1024
            mem_limit_kb = stats['immutable_memory_limit_bytes'] / 1024
            print(f"Immutable Memory: {mem_kb:.2f} KB / {mem_limit_kb:.2f} KB")
        
        # SSTables
        print(f"\nSSTables: {stats['num_sstables']}")
        if stats['num_sstables'] > 0:
            size_kb = stats['total_sstable_size_bytes'] / 1024
            print(f"SSTable Size: {size_kb:.2f} KB")
        
        # Performance
        print(f"\nRotations: {stats['total_memtable_rotations']}")
        print(f"Async Flushes: {stats['total_async_flushes']}")
        print()
    
    def do_exit(self, arg):
        """
        Exit the CLI.
        Usage: exit
        """
        print("Goodbye!")
        self.store.close()
        return True
    
    def do_quit(self, arg):
        """
        Exit the CLI (alias for exit).
        Usage: quit
        """
        return self.do_exit(arg)
    
    def do_EOF(self, arg):
        """Handle Ctrl+D."""
        print()
        return self.do_exit(arg)
    
    def emptyline(self):
        """Do nothing on empty line."""
        pass
    
    def default(self, line):
        """Handle unknown commands."""
        print(f"Unknown command: {line}")
        print("Type 'help' for available commands.")


def main():
    """Main entry point for the CLI."""
    data_dir = "./data"
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("LSM KV Store CLI")
            print("Usage: python cli.py [data_directory]")
            print(f"Default data directory: {data_dir}")
            return
        else:
            data_dir = sys.argv[1]
    
    try:
        cli = KVStoreCLI(data_dir=data_dir)
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
