import json
from typing import Any, Dict


def serialize_event(event_data: Dict[str, Any]) -> bytes:
    """Serialize dictionary event to UTF-8 encoded JSON bytes.

    Ensures Vietnamese / Unicode characters are preserved correctly without ascii escaping.
    """
    # TODO: Học viên hoàn thiện logic hàm này theo hướng dẫn trong Lab Handout
    # Gợi ý: dùng json.dumps với ensure_ascii=False và encode sang 'utf-8'
    raise NotImplementedError("Học viên cần triển khai hàm serialize_event")


def deserialize_event(raw_bytes: bytes) -> Dict[str, Any]:
    """Deserialize raw bytes into a Python dictionary.

    Raises:
        ValueError: If input is not valid UTF-8 or not valid JSON object.
    """
    # TODO: Học viên hoàn thiện logic giải mã bytes sang dictionary
    # Kiểm tra kiểu dữ liệu, giải mã UTF-8 và parse JSON
    raise NotImplementedError("Học viên cần triển khai hàm deserialize_event")
