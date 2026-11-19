import json
import pytest

from src.common.serialization import deserialize_event, serialize_event
from src.common.validation import ValidationError, validate_order_created


def test_serialization_roundtrip_vietnamese():
    """Ensures UTF-8 encoding retains Vietnamese accents correctly."""
    original = {
        "event_id": "test-vn-01",
        "name": "Bàn phím cơ không dây Tiếng Việt",
        "price": 250000,
    }
    encoded = serialize_event(original)
    assert isinstance(encoded, bytes)

    decoded = deserialize_event(encoded)
    assert decoded["name"] == "Bàn phím cơ không dây Tiếng Việt"
    assert decoded == original


def test_deserialization_invalid_inputs():
    """Rejects malformed JSON and invalid UTF-8 payloads."""
    with pytest.raises(ValueError, match="Invalid JSON format"):
        deserialize_event(b"{ malformed json ... }")

    with pytest.raises(TypeError):
        deserialize_event("not-bytes")  # type: ignore


def test_validation_valid_order():
    """Valid order passes both JSON Schema and business invariants."""
    event = {
        "event_id": "evt-001",
        "event_type": "OrderCreated",
        "event_version": 1,
        "event_time": "2026-11-14T10:00:00Z",
        "source": "test-suite",
        "order_id": "ORD-001",
        "customer_id": "CUS-001",
        "items": [
            {"product_id": "P-01", "quantity": 2, "unit_price": 100000},
            {"product_id": "P-02", "quantity": 1, "unit_price": 50000},
        ],
        "total_amount": 250000,
        "currency": "VND",
    }
    # Should not raise exception
    validate_order_created(event)


def test_validation_rejects_inconsistent_total():
    """Rejects event when total_amount does not match line item calculations."""
    event = {
        "event_id": "evt-002",
        "event_type": "OrderCreated",
        "event_version": 1,
        "event_time": "2026-11-14T10:00:00Z",
        "source": "test-suite",
        "order_id": "ORD-002",
        "customer_id": "CUS-001",
        "items": [
            {"product_id": "P-01", "quantity": 1, "unit_price": 100000},
        ],
        "total_amount": 999999,  # Mismatch!
        "currency": "VND",
    }
    with pytest.raises(ValidationError) as exc_info:
        validate_order_created(event)
    assert "Business invariant failed" in str(exc_info.value)
    assert exc_info.value.error_type == "INVARIANT_VIOLATION"


def test_validation_rejects_boolean_in_numeric_fields():
    """Rejects booleans passed into integer fields (e.g. total_amount=True)."""
    event = {
        "event_id": "evt-003",
        "event_type": "OrderCreated",
        "event_version": 1,
        "event_time": "2026-11-14T10:00:00Z",
        "source": "test-suite",
        "order_id": "ORD-003",
        "customer_id": "CUS-001",
        "items": [
            {"product_id": "P-01", "quantity": True, "unit_price": 100000},  # Bool in quantity
        ],
        "total_amount": 100000,
        "currency": "VND",
    }
    with pytest.raises(ValidationError):
        validate_order_created(event)
