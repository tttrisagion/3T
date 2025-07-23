import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

# Set the sampling rate based on a global configuration
# 0.0 = no sampling (disable tracing)
# 1.0 = sample all traces
SAMPLING_RATE = 1.0  # Set to 0.0 to disable tracing


def get_tracer(service_name):
    # Configure the tracer provider with the desired sampler
    resource = Resource.create(attributes={"service.name": service_name})
    provider = TracerProvider(
        sampler=TraceIdRatioBased(SAMPLING_RATE), resource=resource
    )

    # Configure a span processor to export spans to the OTLP collector
    exporter = OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    # Set the tracer provider
    trace.set_tracer_provider(provider)

    # Get a tracer
    return trace.get_tracer(service_name)
