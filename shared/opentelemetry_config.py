import logging
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


class DispatchingSampler(Sampler):
    """
    Dispatches to one of two samplers based on the span name.
    Strictly throttles noisy tasks even if a parent is sampled.
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
        # Priority spans: Always sample these regardless of parent
        priority_spans = {
            # Reconciliation
            "reconcile_positions",
            "worker.reconciliation_engine.reconcile_positions",
            "run/worker.reconciliation_engine.reconcile_positions",
            "reconcile_symbol",
            "calculate_reconciliation_action",
            "send_order_to_gateway",
            # Order Gateway
            "execute_order",
            "POST /execute_order",
            "order-gateway",
            # Balance
            "update_balance",
            "worker.tasks.update_balance",
            "run/worker.tasks.update_balance",
            "update_balance_task",
        }

        if name in priority_spans:
            return SamplingResult(Decision.RECORD_AND_SAMPLE, attributes, trace_state)

        # Noisy spans: Apply low rate even if parent is sampled (throttling)
        if name in self._target_tasks:
            return self._low_rate_sampler.should_sample(
                parent_context, trace_id, name, kind, attributes, links, trace_state
            )

        # For all others, defer to the default sampler (100% for root spans)
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
        "worker.tasks.get_exit_status",
        "worker.tasks.save_run_state",
        "worker.tasks.calculate_permutation_entropy",
        "get_market_weight_task",
        "get_exit_status_task",
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
        "run/worker.tasks.get_exit_status",
        "run/worker.tasks.save_run_state",
        "run/worker.tasks.calculate_permutation_entropy",
    }

    # Configure the dispatching sampler
    dispatching_sampler = DispatchingSampler(
        target_tasks=noisy_tasks,
        low_rate_sampler=TraceIdRatioBased(sampling_rate),
        default_sampler=TraceIdRatioBased(1.0),  # Sample all other traces
    )

    # Use ParentBased but ensure our dispatching logic is applied in all cases
    # to support forcing sampling for priority spans and forcing drops for noisy ones.
    sampler = ParentBased(
        root=dispatching_sampler,
        remote_parent_sampled=dispatching_sampler,
        remote_parent_not_sampled=dispatching_sampler,
        local_parent_sampled=dispatching_sampler,
        local_parent_not_sampled=dispatching_sampler,
    )

    provider = TracerProvider(sampler=sampler, resource=resource)

    if os.environ.get("DISABLE_OTEL_EXPORTER") != "true":
        exporter = OTLPSpanExporter(
            endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        )
        # Tighten timeouts for eventlet compatibility.
        # BatchSpanProcessor spawns a background thread that becomes a greenlet
        # under eventlet monkey patching. Short export timeouts prevent a stalled
        # OTEL collector from blocking the greenlet scheduler, and a capped queue
        # prevents unbounded memory growth if exports fall behind.
        processor = BatchSpanProcessor(
            exporter,
            export_timeout_millis=5000,  # 5s vs default 30s
            schedule_delay_millis=5000,  # flush every 5s vs default 5s (explicit)
            max_queue_size=2048,  # cap memory; default is 2048 (explicit)
            max_export_batch_size=512,  # default is 512 (explicit)
        )
        provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)
    _TRACER_PROVIDER = provider
    return provider


def shutdown_telemetry(timeout_millis: int = 3000):
    """
    Flush and shut down the tracer provider.

    Call this during Celery worker shutdown (e.g., worker_shutting_down signal)
    BEFORE eventlet's hub is torn down, to avoid atexit deadlocks.
    """
    global _TRACER_PROVIDER
    if _TRACER_PROVIDER is not None:
        try:
            _TRACER_PROVIDER.force_flush(timeout_millis=timeout_millis)
        except Exception:
            pass  # Best-effort; don't block shutdown
        try:
            _TRACER_PROVIDER.shutdown()
        except Exception:
            pass
        _TRACER_PROVIDER = None


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
