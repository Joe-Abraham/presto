#!/bin/bash

# Test script for Native Sidecar Catalog Filtering
# This script helps test different catalog filtering configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETC_SIDECAR_DIR="${SCRIPT_DIR}"

echo "Native Sidecar Catalog Filtering Test Helper"
echo "============================================="
echo

# Function to show available configurations
show_configurations() {
    echo "Available function namespace configurations:"
    echo
    for config_file in "${ETC_SIDECAR_DIR}/function-namespace/"*.properties; do
        if [[ -f "${config_file}" ]]; then
            file=$(basename "${config_file}")
            echo "  ${file}"
            catalog_line=$(grep "^sidecar.catalog-name=" "${config_file}" 2>/dev/null || echo "")
            if [[ -n "${catalog_line}" ]]; then
                catalog=$(echo "${catalog_line}" | cut -d'=' -f2)
                if [[ -n "${catalog}" ]]; then
                    echo "    Catalog: ${catalog}"
                else
                    echo "    Catalog: (all catalogs - no filtering)"
                fi
            else
                echo "    Catalog: (all catalogs - no filtering)"
            fi
            echo
        fi
    done
}

# Function to validate configuration
validate_config() {
    local config_file="$1"
    echo "Validating configuration: ${config_file}"
    
    if [[ ! -f "${config_file}" ]]; then
        echo "ERROR: Configuration file not found: ${config_file}"
        return 1
    fi
    
    # Check required properties
    if ! grep -q "function-namespace-manager.name=native" "${config_file}"; then
        echo "ERROR: Missing required property: function-namespace-manager.name=native"
        return 1
    fi
    
    if ! grep -q "supported-function-languages=CPP" "${config_file}"; then
        echo "ERROR: Missing required property: supported-function-languages=CPP"
        return 1  
    fi
    
    if ! grep -q "function-implementation-type=CPP" "${config_file}"; then
        echo "ERROR: Missing required property: function-implementation-type=CPP"
        return 1
    fi
    
    echo "✓ Configuration is valid"
    
    # Show catalog filtering setting
    if grep -q "sidecar.catalog-name=" "${config_file}"; then
        catalog=$(grep "sidecar.catalog-name=" "${config_file}" | cut -d'=' -f2)
        if [[ -n "${catalog}" ]]; then
            echo "✓ Catalog filtering enabled for: ${catalog}"
        else
            echo "✓ Catalog filtering disabled (all functions)"
        fi
    else
        echo "✓ Catalog filtering disabled (all functions)"
    fi
    
    echo
}

# Main script logic
case "${1:-help}" in
    "list")
        show_configurations
        ;;
    "validate")
        if [[ -n "$2" ]]; then
            validate_config "${ETC_SIDECAR_DIR}/function-namespace/$2"
        else
            echo "Usage: $0 validate <config-file>"
            echo "Example: $0 validate native-hive.properties"
        fi
        ;;
    "validate-all")
        echo "Validating all configurations..."
        echo
        for config in "${ETC_SIDECAR_DIR}/function-namespace/"*.properties; do
            if [[ -f "${config}" ]]; then
                validate_config "${config}"
            fi
        done
        ;;
    "help"|*)
        echo "Usage: $0 [command]"
        echo
        echo "Commands:"
        echo "  list         - Show available function namespace configurations"  
        echo "  validate <file> - Validate a specific configuration file"
        echo "  validate-all - Validate all configuration files"
        echo "  help         - Show this help message"
        echo
        echo "Examples:"
        echo "  $0 list"
        echo "  $0 validate native-hive.properties"
        echo "  $0 validate-all"
        echo
        ;;
esac