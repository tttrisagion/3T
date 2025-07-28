"""
Network monitoring utilities for detecting and alerting on network issues.
"""

import logging
from datetime import datetime

from opentelemetry import metrics, trace

tracer = trace.get_tracer(__name__)

# Set up metrics
meter = metrics.get_meter(__name__)
network_errors_counter = meter.create_counter(
    name="network_errors_total", description="Total number of network errors", unit="1"
)
network_latency_histogram = meter.create_histogram(
    name="network_request_duration_seconds",
    description="Network request duration in seconds",
    unit="s",
)
circuit_breaker_state_gauge = meter.create_gauge(
    name="circuit_breaker_state",
    description="Circuit breaker state (0=closed, 1=open)",
    unit="1",
)


class NetworkMonitor:
    """
    Monitor network health and provide alerting capabilities.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._error_counts = {}
        self._last_alert_time = {}
        self.alert_cooldown = 300  # 5 minutes between alerts

    def record_network_error(
        self, service: str, error_type: str, error_message: str = ""
    ):
        """
        Record a network error for monitoring and alerting.

        Args:
            service: Name of the service experiencing the error
            error_type: Type of error (timeout, connection_refused, etc.)
            error_message: Detailed error message
        """
        with tracer.start_as_current_span("record_network_error") as span:
            span.set_attribute("service", service)
            span.set_attribute("error.type", error_type)

            # Update metrics
            network_errors_counter.add(
                1, {"service": service, "error_type": error_type}
            )

            # Track error counts
            key = f"{service}:{error_type}"
            self._error_counts[key] = self._error_counts.get(key, 0) + 1

            # Log the error
            self.logger.error(
                f"Network error in {service}: {error_type} - {error_message}",
                extra={
                    "service": service,
                    "error_type": error_type,
                    "error_count": self._error_counts[key],
                },
            )

            # Check if we should send an alert
            self._check_alert_threshold(service, error_type)

    def record_network_latency(self, service: str, duration_seconds: float):
        """
        Record network request latency.

        Args:
            service: Name of the service
            duration_seconds: Request duration in seconds
        """
        network_latency_histogram.record(duration_seconds, {"service": service})

    def record_circuit_breaker_state(self, service: str, is_open: bool):
        """
        Record circuit breaker state change.

        Args:
            service: Name of the service
            is_open: True if circuit breaker is open, False if closed
        """
        with tracer.start_as_current_span("circuit_breaker_state_change") as span:
            span.set_attribute("service", service)
            span.set_attribute("circuit_breaker.open", is_open)

            circuit_breaker_state_gauge.set(1 if is_open else 0, {"service": service})

            if is_open:
                self.logger.warning(
                    f"Circuit breaker opened for {service}",
                    extra={"service": service, "circuit_breaker_state": "open"},
                )
                self._send_alert(
                    service,
                    "circuit_breaker_open",
                    f"Circuit breaker opened for {service} due to repeated failures",
                )
            else:
                self.logger.info(
                    f"Circuit breaker closed for {service}",
                    extra={"service": service, "circuit_breaker_state": "closed"},
                )

    def _check_alert_threshold(self, service: str, error_type: str):
        """Check if error count exceeds alert threshold."""
        key = f"{service}:{error_type}"
        error_count = self._error_counts.get(key, 0)

        # Alert thresholds
        thresholds = {
            "connection_timeout": 3,
            "connection_refused": 5,
            "network_unreachable": 2,
            "dns_resolution": 2,
            "ssl_error": 1,
        }

        threshold = thresholds.get(error_type, 5)  # Default threshold

        if error_count >= threshold:
            self._send_alert(
                service,
                error_type,
                f"High error count for {service}: {error_count} {error_type} errors",
            )

    def _send_alert(self, service: str, alert_type: str, message: str):
        """
        Send an alert (currently just logs, but can be extended to send to
        external alerting systems like Slack, PagerDuty, etc.).
        """
        alert_key = f"{service}:{alert_type}"
        now = datetime.now()

        # Check cooldown period
        if alert_key in self._last_alert_time:
            time_since_last = (now - self._last_alert_time[alert_key]).total_seconds()
            if time_since_last < self.alert_cooldown:
                return  # Skip alert due to cooldown

        self._last_alert_time[alert_key] = now

        # For now, just log the alert
        # In production, you could send to Slack, PagerDuty, email, etc.
        self.logger.critical(
            f"ALERT: {message}",
            extra={
                "alert_type": "network_error",
                "service": service,
                "severity": "high",
                "timestamp": now.isoformat(),
            },
        )

        # Example: Send to Slack (uncomment and configure as needed)
        # self._send_slack_alert(service, alert_type, message)

    def _send_slack_alert(self, service: str, alert_type: str, message: str):
        """
        Example Slack alerting (requires slack_sdk package).
        Uncomment and configure with your Slack webhook URL.
        """
        # import requests
        # import json
        #
        # webhook_url = "YOUR_SLACK_WEBHOOK_URL"
        # payload = {
        #     "text": f"🚨 Network Alert: {message}",
        #     "attachments": [
        #         {
        #             "color": "danger",
        #             "fields": [
        #                 {"title": "Service", "value": service, "short": True},
        #                 {"title": "Alert Type", "value": alert_type, "short": True},
        #                 {"title": "Time", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": True}
        #             ]
        #         }
        #     ]
        # }
        #
        # try:
        #     requests.post(webhook_url, json=payload, timeout=10)
        # except Exception as e:
        #     self.logger.error(f"Failed to send Slack alert: {e}")
        pass

    def get_error_summary(self) -> dict[str, int]:
        """Get summary of all recorded errors."""
        return self._error_counts.copy()

    def reset_error_counts(self):
        """Reset error counts (useful for testing or periodic cleanup)."""
        self._error_counts.clear()


# Global monitor instance
network_monitor = NetworkMonitor()
