#!/bin/sh
# hub/entrypoint.sh

mkdir -p /app/www

# 1. Start VictoriaMetrics internally (port 8428)
/usr/local/bin/victoria-metrics \
    -storageDataPath=/storage \
    -retentionPeriod=1 \
    -httpListenAddr=:8428 \
    -search.maxUniqueTimeseries=1000000 &

# 2. Start the Snapshotter
python metrics_snapshotter.py &

# 3. Start Caddy as the public gateway (port 8080)
# This handles the Proxy (POST) and Static Files (GET)
exec /usr/local/bin/caddy run --config /app/Caddyfile --adapter caddyfile
