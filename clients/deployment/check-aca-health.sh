#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Checking Container Apps Health Status..."
echo "======================================"

# Get all container apps across all resource groups
container_apps=$(az containerapp list --query "[].{name:name, resourceGroup:resourceGroup, fqdn:properties.configuration.ingress.fqdn, runningState:properties.runningStatus}" --output tsv)

if [ -z "$container_apps" ]; then
    echo -e "${YELLOW}No Container Apps found${NC}"
    exit 0
fi

while IFS=$'\t' read -r name resource_group fqdn running_state; do
    echo -n "Container App: $name ($resource_group) - "
    
    # Check running state first
    if [[ "$running_state" == *"Running"* ]]; then
        echo -e "${GREEN}RUNNING${NC}"
        
        # If running, check health endpoint if FQDN exists
        if [ "$fqdn" != "null" ] && [ -n "$fqdn" ]; then
            echo -n "  Health check: https://$fqdn/q/health - "
            
            # Make HTTP request and get both status code and response body
            temp_file=$(mktemp)
            http_code=$(curl -s -w "%{http_code}" --connect-timeout 10 --max-time 30 "https://$fqdn/q/health" -o "$temp_file" 2>/dev/null)
            response_body=$(cat "$temp_file" 2>/dev/null)
            rm -f "$temp_file"
            
            if [ "$http_code" = "200" ]; then
                # Check if response body contains "RUNNING"
                if echo "$response_body" | grep -q "RUNNING"; then
                    echo -e "${GREEN}✓ RUNNING${NC}"
                else
                    echo -e "${RED}✗ Not RUNNING (payload: ${response_body:0:100})${NC}"
                fi
            elif [ "$http_code" = "000" ]; then
                echo -e "${RED}✗ Connection failed${NC}"
            else
                echo -e "${RED}✗ HTTP $http_code${NC}"
            fi
        else
            echo "  No external endpoint configured"
        fi
    else
        echo -e "${RED}NOT RUNNING ($running_state)${NC}"
    fi
    echo
done <<< "$container_apps"

echo "Health check completed."