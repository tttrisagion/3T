from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

# Set the sampling rate based on a global configuration
# 0.0 = no sampling (disable tracing)
# 1.0 = sample all traces
SAMPLING_RATE = 1.0  # Set to 0.0 to disable tracing

def get_tracer(service_name):
    # Configure the tracer provider with the desired sampler
    provider = TracerProvider(sampler=TraceIdRatioBased(SAMPLING_RATE))

    # Configure a span processor to export spans to the console
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)

    # Set the tracer provider
    trace.set_tracer_provider(provider)

    # Get a tracer
    return trace.get_tracer(service_name)