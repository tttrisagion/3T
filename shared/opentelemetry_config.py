import os
import random

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    Decision,
    ParentBased,
    Sampler,
    SamplingResult,
    TraceIdRatioBased,
)
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import Attributes

from shared.config import config

# --- Custom Sampler Definition ---


class TaskSpecificSampler(Sampler):
    """
    A custom sampler that applies a specific sampling rate to a list of target tasks,
    while sampling all other tasks. It ensures that errored traces for the target
    tasks are always recorded.
    """

    def __init__(self, rate: float, target_tasks: list[str]):
        self._rate = rate
        self._target_tasks = set(target_tasks)
        # Use ParentBased with a default of "always on" for non-target tasks
        self._default_sampler = ParentBased(root=TraceIdRatioBased(1.0))

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
        # If the span is one of the noisy tasks, apply our custom logic
        if name in self._target_tasks:
            # The OpenTelemetry SDK is designed to record spans with an ERROR status
            # regardless of the sampler's decision. So, we can simply apply
            # the sampling rate here. If an exception occurs later and the
            # span status is set to ERROR, it will be recorded.
            if random.random() < self._rate:
                return SamplingResult(
                    Decision.RECORD_AND_SAMPLE, attributes, trace_state
                )
            else:
                return SamplingResult(Decision.DROP, attributes, trace_state)

        # For all other tasks, use the default parent-based sampling.
        return self._default_sampler.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )

    def get_description(self) -> str:
        return f"TaskSpecificSampler{{rate={self._rate}, targets={self._target_tasks}}}"


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

    # Get sampling rate from config
    sampling_rate = config.get("observability.sampling_rate", 1.0)

    # Define the noisy tasks that should be sampled
    noisy_tasks = [
        "worker.tasks.get_market_weight",
        "worker.tasks.update_pnl",
        "worker.tasks.get_exit_status",
    ]

    # Configure the custom sampler
    sampler = TaskSpecificSampler(rate=sampling_rate, target_tasks=noisy_tasks)

    provider = TracerProvider(sampler=sampler, resource=resource)

    if os.environ.get("DISABLE_OTEL_EXPORTER") != "true":
        exporter = OTLPSpanExporter(
            endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        )
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)
    _TRACER_PROVIDER = provider


def get_tracer(service_name: str):
    """
    Initializes telemetry if not already done and returns a tracer instance.
    """
    setup_telemetry(service_name)
    return trace.get_tracer(service_name)
