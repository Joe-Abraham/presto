#!/bin/bash

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script to validate multiple catalog configuration for native sidecar function registry

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAMESPACE_DIR="$SCRIPT_DIR/function-namespace"

echo "Validating multiple catalog configuration for native sidecar..."

# Check if function-namespace directory exists
if [[ ! -d "$FUNCTION_NAMESPACE_DIR" ]]; then
    echo "ERROR: function-namespace directory not found at $FUNCTION_NAMESPACE_DIR"
    exit 1
fi

echo "✓ Found function-namespace directory"

# Expected configuration files
EXPECTED_CONFIGS=(
    "native.properties"
    "custom_cpp.properties"
    "user_functions.properties"
)

# Validate each configuration file
for config in "${EXPECTED_CONFIGS[@]}"; do
    config_path="$FUNCTION_NAMESPACE_DIR/$config"
    if [[ ! -f "$config_path" ]]; then
        echo "ERROR: Configuration file not found: $config_path"
        exit 1
    fi
    
    # Check if the configuration contains required properties
    if ! grep -q "function-namespace-manager.name=native" "$config_path"; then
        echo "ERROR: Missing 'function-namespace-manager.name=native' in $config"
        exit 1
    fi
    
    echo "✓ Validated $config"
done

# Check main configuration
if [[ ! -f "$SCRIPT_DIR/config.properties" ]]; then
    echo "ERROR: Main config.properties not found"
    exit 1
fi

echo "✓ Found main config.properties"

# Check for plugin bundles configuration
if ! grep -q "plugin.bundles" "$SCRIPT_DIR/config.properties"; then
    echo "WARNING: No 'plugin.bundles' found in config.properties - this may be needed for multiple catalog support"
fi

echo ""
echo "Multiple catalog configuration validation completed successfully!"
echo ""
echo "Configuration summary:"
echo "- Native catalog: $FUNCTION_NAMESPACE_DIR/native.properties"
echo "- Custom C++ catalog: $FUNCTION_NAMESPACE_DIR/custom_cpp.properties"  
echo "- User functions catalog: $FUNCTION_NAMESPACE_DIR/user_functions.properties"
echo ""
echo "To test multiple catalog support:"
echo "1. Start the native sidecar with this configuration"
echo "2. Register functions in different catalogs"
echo "3. Query functions using catalog.schema.function syntax"
echo ""
echo "Example function calls:"
echo "- SELECT native.default.my_builtin_function()"
echo "- SELECT custom_cpp.analytics.my_cpp_function()"  
echo "- SELECT user_functions.custom.my_user_function()"