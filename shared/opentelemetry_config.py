import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ParentBased,
    Sampler,
    SamplingResult,
    TraceIdRatioBased,
)
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import Attributes

from shared.config import config

# --- Custom Sampler Definition ---


class DispatchingSampler(Sampler):
    """
    Dispatches to one of two samplers based on the span name.
    """

    def __init__(
        self,
        target_tasks: set[str],
        low_rate_sampler: Sampler,
        default_sampler: Sampler,
    ):
        self._target_tasks = target_tasks
        self._low_rate_sampler = low_rate_sampler
        self._default_sampler = default_sampler

    def should_sample(
        self,
        parent_context,
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links=None,
        trace_state=None,
    ) -> SamplingResult:
        if name in self._target_tasks:
            return self._low_rate_sampler.should_sample(
                parent_context, trace_id, name, kind, attributes, links, trace_state
            )
        return self._default_sampler.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )

    def get_description(self) -> str:
        return "DispatchingSampler"


# --- Global Tracer Setup ---

_TRACER_PROVIDER = None


def setup_telemetry(service_name: str):
    """
    Configures and sets the global TracerProvider for the application.
    This should be called once per service.
    """
    global _TRACER_PROVIDER
    if _TRACER_PROVIDER is not None:
        return

    resource = Resource.create(attributes={"service.name": service_name})

    # Get sampling rate from config for the noisy tasks
    sampling_rate = config.get("observability.sampling_rate", 1.0)

    # Define the noisy tasks that should be sampled at a lower rate
    noisy_tasks = {
        "worker.tasks.get_market_weight",
        "worker.tasks.update_pnl",
        "worker.tasks.get_exit_status",
        "get_market_weight_task",
        "update_pnl_task",
        "get_exit_status_task",
        # Add run/ prefixed tasks for Celery instrumentation
        "run/worker.tasks.get_market_weight",
        "run/worker.tasks.update_pnl",
        "run/worker.tasks.get_exit_status",
    }

    # Configure the dispatching sampler
    dispatching_sampler = DispatchingSampler(
        target_tasks=noisy_tasks,
        low_rate_sampler=TraceIdRatioBased(sampling_rate),
        default_sampler=TraceIdRatioBased(1.0),  # Sample all other traces
    )

    # Use ParentBased to ensure our sampler is the root and its decision is final
    sampler = ParentBased(root=dispatching_sampler)

    provider = TracerProvider(sampler=sampler, resource=resource)

    if os.environ.get("DISABLE_OTEL_EXPORTER") != "true":
        exporter = OTLPSpanExporter(
            endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        )
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)
    _TRACER_PROVIDER = provider
    return provider


def get_tracer(service_name: str):
    """
    Initializes telemetry if not already done and returns a tracer instance.
    """
    setup_telemetry(service_name)
    return trace.get_tracer(service_name)
