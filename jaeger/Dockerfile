# Use a builder stage to set permissions
FROM alpine:latest as builder
COPY wait-for-es.sh /tmp/wait-for-es.sh
RUN chmod +x /tmp/wait-for-es.sh

# Final stage
FROM jaegertracing/all-in-one:1.53

# Install curl for the wait script
USER root
RUN apk add --no-cache curl
USER 10001

# Copy the executable script from the builder stage
COPY --from=builder /tmp/wait-for-es.sh /usr/local/bin/wait-for-es.sh

# The base image's entrypoint is /go/bin/all-in-one-linux
# We'll use our script as the new entrypoint to wrap the original one.
ENTRYPOINT ["/usr/local/bin/wait-for-es.sh", "elasticsearch:9200", "/go/bin/all-in-one-linux"]

# Environment variables for Jaeger configuration (cleaner than command line args)
ENV SPAN_STORAGE_TYPE=elasticsearch
ENV ES_SERVER_URLS=http://elasticsearch:9200

# No CMD needed as the binary runs with env vars
