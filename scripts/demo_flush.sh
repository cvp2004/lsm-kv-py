#!/bin/bash
# Demo script for SSTable flush functionality

echo "========================================="
echo "   LSM KV Store - Flush Demo"
echo "========================================="
echo ""

# Clean up any previous demo data
rm -rf ./demo_flush_data

echo "Step 1: Add data to memtable"
echo "-----------------------------"
python3 scripts/cli.py ./demo_flush_data <<'EOF'
put user:1:name Alice
put user:1:email alice@example.com
put user:2:name Bob
put user:2:email bob@example.com
put user:3:name Charlie
stats
exit
EOF

echo ""
echo "Step 2: Flush memtable to SSTable"
echo "----------------------------------"
python3 scripts/cli.py ./demo_flush_data <<'EOF'
flush
stats
exit
EOF

echo ""
echo "Step 3: Add more data to memtable"
echo "----------------------------------"
python3 scripts/cli.py ./demo_flush_data <<'EOF'
put user:4:name Dave
put user:5:name Eve
stats
exit
EOF

echo ""
echo "Step 4: Verify data from both memtable and SSTable"
echo "---------------------------------------------------"
python3 scripts/cli.py ./demo_flush_data <<'EOF'
get user:1:name
get user:4:name
stats
exit
EOF

echo ""
echo "Step 5: Restart and verify persistence"
echo "---------------------------------------"
echo "Simulating restart by creating new store instance..."
python3 scripts/cli.py ./demo_flush_data <<'EOF'
get user:1:name
get user:2:email
get user:4:name
stats
exit
EOF

echo ""
echo "Step 6: Flush again and check SSTable files"
echo "--------------------------------------------"
python3 scripts/cli.py ./demo_flush_data <<'EOF'
flush
stats
exit
EOF

echo ""
echo "SSTable files on disk:"
ls -lh ./demo_flush_data/

echo ""
echo "========================================="
echo "   Demo Complete!"
echo "========================================="
