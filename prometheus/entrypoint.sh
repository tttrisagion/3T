#!/bin/sh
set -e

CONFIG_FILE="/app/config.yml"
PROM_TEMPLATE="/etc/prometheus/prometheus.yml"
PROM_RUNTIME="/etc/prometheus/prometheus_runtime.yml"

# Check if www replication is enabled
if [ -f "$CONFIG_FILE" ] && grep -q "www:" "$CONFIG_FILE" && grep -A 5 "www:" "$CONFIG_FILE" | grep -q "enabled: true"; then
    echo "Prometheus: WWW replication enabled. Injecting remote_write..."
    
    # Extract wallet and node_id
    WALLET=$(grep -A 5 "www:" "$CONFIG_FILE" | grep "wallet:" | awk '{print $2}' | tr -d '\r')
    NODE_ID=$(hostname)
    
    # Initialize AUTH_TOKEN from env or fallback
    AUTH_TOKEN=${METRICS_AUTH_TOKEN:-"default_token"}
    
    # Try to extract METRICS_AUTH_TOKEN from secrets.yml if not set in environment
    if [ "$AUTH_TOKEN" = "default_token" ] && [ -f "/app/secrets.yml" ]; then
        TOKEN_FROM_FILE=$(grep -A 1 "metrics:" "/app/secrets.yml" | grep "auth_token:" | awk '{print $2}' | tr -d '"\r')
        if [ -n "$TOKEN_FROM_FILE" ]; then
            echo "Prometheus: Extracted token from /app/secrets.yml"
            AUTH_TOKEN="$TOKEN_FROM_FILE"
        fi
    fi

    # Create a fresh runtime config with our global and remote_write blocks
    cat > "$PROM_RUNTIME" <<EOF
global:
  scrape_interval: 15s
  external_labels:
    wallet: "$WALLET"
    node_id: "$NODE_ID"

remote_write:
  - url: "https://3t-metrics-hub.fly.dev/api/v1/write"
    bearer_token: "$AUTH_TOKEN"
    write_relabel_configs:
      - source_labels: [__name__]
        regex: '(providence_.*|account_.*|positions_.*|up)'
        action: keep
EOF

    # Append only the scrape_configs from the original template
    # This skips the original 'global:' block entirely
    sed -n '/^scrape_configs:/,$p' "$PROM_TEMPLATE" >> "$PROM_RUNTIME"
else
    echo "Prometheus: WWW replication disabled or config not found. Using default config."
    cp "$PROM_TEMPLATE" "$PROM_RUNTIME"
fi

# Start Prometheus with the runtime config
exec /bin/prometheus --config.file="$PROM_RUNTIME" --storage.tsdb.path=/prometheus --web.enable-remote-write-receiver "$@"
