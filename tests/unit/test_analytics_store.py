from pathlib import Path
import tempfile
import pytest

from src.analytics.analytics_store import AnalyticsStore


@pytest.fixture
def temp_store():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = Path(tmpdir) / "test_analytics.db"
        store = AnalyticsStore(db_path=str(db_file))
        yield store


def test_analytics_idempotency_and_aggregation(temp_store):
    order_event = {
        "event_id": "evt-o-101",
        "event_type": "OrderCreated",
        "event_time": "2026-11-02T10:00:15Z",
        "order_id": "ORD-101",
        "customer_id": "CUS-1",
        "total_amount": 200000,
    }
    view_event = {
        "event_id": "evt-v-101",
        "event_type": "ProductViewed",
        "event_time": "2026-11-02T10:00:25Z",
        "customer_id": "CUS-2",
        "product_id": "P-1",
    }

    # 1. Apply order
    assert temp_store.process_event_transactional(order_event, "2026-11-02T10:00:00Z", "order-events", 0, 1) is True

    # 2. Duplicate order -> Must return False, metrics unchanged
    assert temp_store.process_event_transactional(order_event, "2026-11-02T10:00:00Z", "order-events", 0, 1) is False

    # 3. Apply product view
    assert temp_store.process_event_transactional(view_event, "2026-11-02T10:00:00Z", "customer-activity-events", 1, 10) is True

    metrics = temp_store.get_dashboard_metrics()
    assert len(metrics) == 1
    m = metrics[0]
    assert m["order_count"] == 1
    assert m["created_order_value_vnd"] == 200000
    assert m["product_view_count"] == 1
    assert m["active_customers"] == 2
