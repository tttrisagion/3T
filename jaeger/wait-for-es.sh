#!/bin/sh
set -e

# Usage: wait-for-es.sh host:port [-- command args...]

es_host="$1"
shift
cmd="$@"

# Wait for Elasticsearch to be ready
# Retry for up to 2 minutes
retries=60
count=0
while [ $count -lt $retries ]; do
    # Use a simpler approach to get the status
    health_json=$(curl -s "http://$es_host/_cluster/health" 2>/dev/null || echo "")
    if [ -n "$health_json" ]; then
        # Check if the JSON contains "yellow" or "green" status
        if echo "$health_json" | grep -q '"status":"green"' || echo "$health_json" | grep -q '"status":"yellow"'; then
            status=$(echo "$health_json" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
            >&2 echo "Elasticsearch is up with status: $status"
            exec $cmd
        fi
        status=$(echo "$health_json" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
        >&2 echo "Elasticsearch is not ready yet (status: $status). Waiting..."
    else
        >&2 echo "Elasticsearch is not ready yet (no response). Waiting..."
    fi
    sleep 2
    count=$((count+1))
done

>&2 echo "Timed out waiting for Elasticsearch to be ready."
exit 1
