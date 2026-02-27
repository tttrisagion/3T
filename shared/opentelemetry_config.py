import logging
import os
import random

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
        # Providence iteration and scheduler (highest volume)
        "worker.providence.providence_trading_iteration",
        "providence_trading_iteration",
        "worker.providence.providence_iteration_scheduler",
        "providence_iteration_scheduler",
        # Sub-tasks dispatched from iterations
        "worker.tasks.get_market_weight",
        "worker.tasks.update_pnl",
        "worker.tasks.get_exit_status",
        "worker.tasks.update_run_position",
        "worker.tasks.save_run_state",
        "worker.tasks.calculate_permutation_entropy",
        "get_market_weight_task",
        "update_pnl_task",
        "get_exit_status_task",
        "update_run_position_task",
        "save_run_state_task",
        "calculate_permutation_entropy_task",
        # Price polling spans (continuous)
        "fetch_latest_prices",
        "publish_prices",
        "publish_price_update",
        "poll_cycle",
        # Celery run/ prefixed variants
        "run/worker.providence.providence_trading_iteration",
        "run/worker.providence.providence_iteration_scheduler",
        "run/worker.tasks.get_market_weight",
        "run/worker.tasks.update_pnl",
        "run/worker.tasks.get_exit_status",
        "run/worker.tasks.update_run_position",
        "run/worker.tasks.save_run_state",
        "run/worker.tasks.calculate_permutation_entropy",
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


# --- Log Sampling ---


class SamplingLogFilter(logging.Filter):
    """
    Filters log records based on sampling rate for noisy tasks.
    Applies the same noisy task detection as telemetry sampling.
    Works for both application logs and Celery internal task logs.
    """

    def __init__(self, noisy_patterns: set[str], sampling_rate: float):
        """
        Args:
            noisy_patterns: Set of task name patterns to sample
            sampling_rate: Probability of logging (0.0 to 1.0)
        """
        super().__init__()
        self.noisy_patterns = noisy_patterns
        self.sampling_rate = sampling_rate

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Returns True if the record should be logged, False otherwise.
        """
        # Check if this is a noisy task by examining the log message
        message = record.getMessage()

        # Check if any noisy pattern appears in the message
        # This catches both application logs and Celery's "Task X received/succeeded" messages
        for pattern in self.noisy_patterns:
            if pattern in message:
                # Sample based on configured rate
                return random.random() < self.sampling_rate

        # Not a noisy task, always log
        return True


def setup_log_sampling(logger_names: list[str] = None):
    """
    Configures log sampling for noisy tasks.
    Should be called after logger configuration.

    Args:
        logger_names: List of logger names to apply sampling to (None for root logger)
    """
    sampling_rate = config.get("observability.sampling_rate", 1.0)

    # Use same noisy task patterns as telemetry
    noisy_patterns = {
        "providence_trading_iteration",
        "get_market_weight",
        "update_pnl",
        "get_exit_status",
    }

    # Create the sampling filter
    sampling_filter = SamplingLogFilter(noisy_patterns, sampling_rate)

    # Apply to specified loggers (or root if None)
    if logger_names is None:
        logger_names = [None]

    for logger_name in logger_names:
        logger = logging.getLogger(logger_name)
        logger.addFilter(sampling_filter)

    return sampling_filter
