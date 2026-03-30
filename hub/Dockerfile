# hub/Dockerfile
FROM victoriametrics/victoria-metrics:v1.94.0 AS vm
FROM caddy:2.7.5-alpine AS caddy

FROM python:3.11-slim

# Install binaries
COPY --from=vm /victoria-metrics-prod /usr/local/bin/victoria-metrics
COPY --from=caddy /usr/bin/caddy /usr/local/bin/caddy

# Install dependencies for snapshotter
RUN pip install requests

WORKDIR /app

# Copy logic
COPY metrics_snapshotter.py .
COPY entrypoint.sh .
COPY Caddyfile .

RUN chmod +x entrypoint.sh

# Expose ports
EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
