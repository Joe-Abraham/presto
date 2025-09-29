#!/bin/bash

# Verification script for multiple namespace support
# This script helps verify that the namespace configuration is set up correctly

echo "🔍 Verifying Multiple Namespace Configuration..."
echo

# Check main config file
CONFIG_FILE="presto-native-execution/etc_sidecar/config.properties"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "❌ Config file not found: $CONFIG_FILE"
    exit 1
fi

echo "📋 Main Configuration:"
echo "   File: $CONFIG_FILE"

# Check for namespace manager configurations
if grep -q "function-namespace-manager\.native\.name=native" "$CONFIG_FILE"; then
    echo "   ✅ Native namespace manager configured"
else
    echo "   ❌ Native namespace manager NOT configured"
fi

if grep -q "function-namespace-manager\.hive\.name=native" "$CONFIG_FILE"; then
    echo "   ✅ Hive namespace manager configured"
else
    echo "   ❌ Hive namespace manager NOT configured"
fi

# Check catalog filtering
if grep -q "function-namespace-manager\.native\.catalog=native" "$CONFIG_FILE"; then
    echo "   ✅ Native catalog filter configured"
else
    echo "   ❌ Native catalog filter NOT configured"
fi

if grep -q "function-namespace-manager\.hive\.catalog=hive" "$CONFIG_FILE"; then
    echo "   ✅ Hive catalog filter configured"
else
    echo "   ❌ Hive catalog filter NOT configured"
fi

# Check default namespace
if grep -q "presto\.default-namespace=native\.default" "$CONFIG_FILE"; then
    echo "   ✅ Default namespace set to native.default"
else
    echo "   ❌ Default namespace NOT set correctly"
fi

echo

# Check hive catalog configuration
HIVE_CATALOG="presto-native-execution/etc_sidecar/catalog/hive.properties"
if [[ -f "$HIVE_CATALOG" ]]; then
    echo "📋 Hive Catalog Configuration:"
    echo "   File: $HIVE_CATALOG"
    echo "   ✅ Hive catalog configured (required for hive functions)"
else
    echo "❌ Hive catalog NOT configured: $HIVE_CATALOG"
    echo "   Hive functions will not be registered"
fi

echo

# Check for expected behavior
echo "🎯 Expected Behavior:"
echo "   • native.default namespace: Built-in functions (abs, substring, etc.)"
echo "   • hive.default namespace: Hive functions (initcap, etc.)"
echo "   • Unqualified calls: SELECT abs(-5)  → native.default"
echo "   • Qualified calls: SELECT hive.default.initcap('hello') → hive.default"

echo

# Check old directory-based config (should be removed)
OLD_DIR="presto-native-execution/etc_sidecar/function-namespace/"
if [[ -d "$OLD_DIR" ]]; then
    echo "⚠️  Old directory-based config still exists: $OLD_DIR"
    echo "   This should be removed to avoid conflicts"
else
    echo "✅ Old directory-based config properly removed"
fi

echo
echo "🔧 Configuration verification complete!"