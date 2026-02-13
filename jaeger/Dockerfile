FROM jaegertracing/all-in-one:1.53

# Install curl for healthcheck and create Badger data dirs with correct ownership
USER root
RUN apk add --no-cache curl && \
    mkdir -p /badger/data /badger/key && \
    chown -R 10001:10001 /badger
USER 10001

# Badger embedded storage (replaces Elasticsearch)
ENV SPAN_STORAGE_TYPE=badger
ENV BADGER_EPHEMERAL=false
ENV BADGER_DIRECTORY_VALUE=/badger/data
ENV BADGER_DIRECTORY_KEY=/badger/key

# Auto-prune traces older than 3 hours to limit memory usage
ENV BADGER_SPAN_STORE_TTL=3h
