import random
import pytest

from src.simulators.order_generator import generate_single_order


def test_generator_deterministic_seed():
    """Same seed and parameters must generate identical sequence."""
    rng1 = random.Random(42)
    rng2 = random.Random(42)

    order1 = generate_single_order("run-test", 1, rng1)
    order2 = generate_single_order("run-test", 1, rng2)

    assert order1["order_id"] == order2["order_id"]
    assert order1["items"] == order2["items"]
    assert order1["total_amount"] == order2["total_amount"]


def test_generator_run_id_scoping():
    """Different run_id creates distinct order IDs."""
    rng = random.Random(42)
    order_a = generate_single_order("run-A", 1, rng)
    order_b = generate_single_order("run-B", 1, rng)

    assert order_a["order_id"] == "ORD-run-A-0001"
    assert order_b["order_id"] == "ORD-run-B-0001"
    assert order_a["order_id"] != order_b["order_id"]
