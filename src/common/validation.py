from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict
import jsonschema


class ValidationError(Exception):
    """Custom exception raised when an event violates schema or business logic."""

    def __init__(self, message: str, error_type: str = "VALIDATION_ERROR"):
        super().__init__(message)
        self.error_type = error_type


_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}


def load_schema(schema_name: str) -> Dict[str, Any]:
    """Loads and caches a JSON schema from the contracts directory."""
    if schema_name not in _SCHEMA_CACHE:
        current_dir = Path(__file__).resolve().parent
        repo_root = current_dir.parent.parent
        schema_path = repo_root / "contracts" / schema_name

        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found at {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            _SCHEMA_CACHE[schema_name] = json.load(f)

    return _SCHEMA_CACHE[schema_name]


def validate_order_created(event: Dict[str, Any]) -> None:
    """Validates an OrderCreated event against both JSON Schema and business invariants.

    Business Invariants:
    1. Boolean values must not be accepted for integer fields.
    2. total_amount must exactly equal sum(item['quantity'] * item['unit_price']).
    3. event_time must be a valid ISO-8601 UTC timestamp.
    """
    # TODO: Học viên hoàn thiện 2 tầng validation:
    # 1. Dùng jsonschema.validate() với schema 'order_created.schema.json'
    # 2. Kiểm tra Business Invariant: total_amount == sum(quantity * unit_price)
    # 3. Kiểm tra không chấp nhận kiểu boolean trong các trường số
    # 4. Kiểm tra timestamp UTC
    raise NotImplementedError("Học viên cần triển khai hàm validate_order_created")


def validate_product_viewed(event: Dict[str, Any]) -> None:
    """Validates a ProductViewed event against JSON Schema and UTC timestamp rule."""
    # TODO: Học viên hoàn thiện validation cho ProductViewed event
    raise NotImplementedError("Học viên cần triển khai hàm validate_product_viewed")
