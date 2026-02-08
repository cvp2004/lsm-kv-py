#!/bin/bash
# Demo script for LSM KV Store

echo "========================================="
echo "   LSM KV Store Demo"
echo "========================================="
echo ""

# Clean up any previous demo data
rm -rf ./demo_data

echo "Starting the KV Store..."
echo ""

# Run demo commands
python3 scripts/cli.py ./demo_data <<'EOF'
put user:alice:name Alice Smith
put user:alice:email alice@example.com
put user:alice:age 28
put user:bob:name Bob Johnson
put user:bob:email bob@example.com
get user:alice:name
get user:alice:email
get user:bob:name
stats
delete user:alice:age
get user:alice:age
stats
exit
EOF

echo ""
echo "========================================="
echo "   Demonstrating WAL Recovery"
echo "========================================="
echo ""
echo "Restarting store to show data persistence..."
echo ""

python3 scripts/cli.py ./demo_data <<'EOF'
get user:alice:name
get user:alice:email
get user:bob:name
get user:bob:email
stats
exit
EOF

echo ""
echo "========================================="
echo "   Demo Complete!"
echo "========================================="
