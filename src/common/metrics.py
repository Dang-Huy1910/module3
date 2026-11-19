import time
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# 1. Pipeline Event Processing Counter
EVENTS_TOTAL = Counter(
    "bk_pipeline_events_total",
    "Total events processed by pipeline service",
    ["service", "result"],
)

# 2. Pipeline Processing Duration Histogram
PROCESSING_SECONDS = Histogram(
    "bk_pipeline_processing_seconds",
    "Time spent in event processing and storage persistence",
    ["service"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

# 3. Last Successful Processing Timestamp (Unix Seconds Gauge)
LAST_SUCCESS_UNIXTIME = Gauge(
    "bk_pipeline_last_success_unixtime",
    "Unix timestamp of the last successfully processed event",
    ["service"],
)

def start_metrics_server(port: int = 9108) -> None:
    """Starts Prometheus scraping HTTP server on separate thread."""
    try:
        start_http_server(port)
    except Exception as err:
        print(f"Warning: Prometheus metrics server could not bind to port {port}: {err}")
