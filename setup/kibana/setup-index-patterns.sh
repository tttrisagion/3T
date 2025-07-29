#!/bin/sh

# Kibana Index Pattern Setup Script
# Creates index patterns for Jaeger tracing data

set -e

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
MAX_RETRIES=60
RETRY_INTERVAL=5

echo "Setting up Kibana index patterns for Jaeger..."

# Wait for Kibana to be ready
echo "Waiting for Kibana to be available at $KIBANA_URL..."
i=1
while [ $i -le $MAX_RETRIES ]; do
    if curl -s "$KIBANA_URL/api/status" > /dev/null 2>&1; then
        echo "Kibana is available!"
        break
    fi
    
    if [ $i -eq $MAX_RETRIES ]; then
        echo "ERROR: Kibana not available after $((MAX_RETRIES * RETRY_INTERVAL)) seconds"
        exit 1
    fi
    
    echo "Attempt $i/$MAX_RETRIES: Kibana not ready, waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
    i=$((i + 1))
done

# Wait a bit more for Kibana to fully initialize
echo "Waiting for Kibana to fully initialize..."
sleep 10

# Function to create index pattern
create_index_pattern() {
    local pattern_id=$1
    local pattern_title=$2
    local time_field=$3
    
    echo "Creating index pattern: $pattern_title"
    
    # Check if index pattern already exists
    if curl -s "$KIBANA_URL/api/saved_objects/index-pattern/$pattern_id" | grep -q "\"id\":\"$pattern_id\""; then
        echo "Index pattern $pattern_id already exists, skipping..."
        return 0
    fi
    
    # Create the index pattern
    local payload="{\"attributes\":{\"title\":\"$pattern_title\""
    if [ -n "$time_field" ]; then
        payload="$payload,\"timeFieldName\":\"$time_field\""
    fi
    payload="$payload}}"
    
    local response=$(curl -s -X POST "$KIBANA_URL/api/saved_objects/index-pattern/$pattern_id" \
        -H "Content-Type: application/json" \
        -H "kbn-xsrf: true" \
        -d "$payload")
    
    if echo "$response" | grep -q "\"id\":\"$pattern_id\""; then
        echo "✓ Successfully created index pattern: $pattern_title"
    else
        echo "✗ Failed to create index pattern: $pattern_title"
        echo "Response: $response"
        return 1
    fi
}

# Create Jaeger index patterns
echo "Creating Jaeger index patterns..."

create_index_pattern "jaeger-span" "jaeger-span-*" "startTimeMillis"
create_index_pattern "jaeger-service" "jaeger-service-*" ""

echo "✓ Kibana index pattern setup completed successfully!"

# Set default index pattern to jaeger-span if none exists
echo "Setting default index pattern..."
default_response=$(curl -s "$KIBANA_URL/api/saved_objects/_find?type=config&fields=defaultIndex" || true)
if ! echo "$default_response" | grep -q "defaultIndex"; then
    echo "Setting jaeger-span-* as default index pattern..."
    curl -s -X POST "$KIBANA_URL/api/saved_objects/config/7.17.15" \
        -H "Content-Type: application/json" \
        -H "kbn-xsrf: true" \
        -d '{"attributes":{"defaultIndex":"jaeger-span"}}' > /dev/null || true
    echo "✓ Default index pattern set"
fi

echo "🎉 Kibana setup complete!"