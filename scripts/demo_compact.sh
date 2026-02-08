#!/bin/bash
# Demo script for SSTable compaction functionality

echo "========================================="
echo "   LSM KV Store - Compaction Demo"
echo "========================================="
echo ""

# Clean up any previous demo data
rm -rf ./demo_compact_data

echo "Step 1: Create multiple SSTables with overlapping data"
echo "-------------------------------------------------------"
python3 scripts/cli.py ./demo_compact_data <<'EOF'
put user:1 Alice_v1
put user:2 Bob_v1
flush
put user:1 Alice_v2
put user:3 Charlie
flush
put user:2 Bob_v2
put user:4 Dave
flush
stats
exit
EOF

echo ""
echo "Step 2: Check SSTable files before compaction"
echo "----------------------------------------------"
ls -lh ./demo_compact_data/ | grep sstable

echo ""
echo "Step 3: Compact all SSTables into one"
echo "--------------------------------------"
python3 scripts/cli.py ./demo_compact_data <<'EOF'
compact
stats
exit
EOF

echo ""
echo "Step 4: Check SSTable files after compaction"
echo "---------------------------------------------"
ls -lh ./demo_compact_data/ | grep sstable

echo ""
echo "Step 5: Verify data integrity and latest versions"
echo "--------------------------------------------------"
python3 scripts/cli.py ./demo_compact_data <<'EOF'
get user:1
get user:2
get user:3
get user:4
exit
EOF

echo ""
echo "Step 6: Demo with deletions"
echo "----------------------------"
python3 scripts/cli.py ./demo_compact_data <<'EOF'
put key1 value1
put key2 value2
put key3 value3
flush
delete key2
flush
stats
compact
stats
get key1
get key2
get key3
exit
EOF

echo ""
echo "========================================="
echo "   Demo Complete!"
echo "========================================="
echo ""
echo "Summary:"
echo "- Compaction merges multiple SSTables into one"
echo "- Latest version of each key is preserved"
echo "- Deleted entries (tombstones) are removed"
echo "- Reduces disk space and improves read performance"
