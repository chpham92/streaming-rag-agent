import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "producer"))

from generate_tickets import make_ticket  # noqa: E402
from tickets_seed import PRODUCT_AREAS  # noqa: E402


def test_make_ticket_has_required_fields():
    ticket = make_ticket()
    for field in ("ticket_id", "customer_id", "product_area", "subject", "body", "created_at"):
        assert field in ticket
        assert ticket[field]  # non-empty


def test_make_ticket_product_area_is_valid():
    for _ in range(20):
        assert make_ticket()["product_area"] in PRODUCT_AREAS


def test_make_ticket_ids_are_unique():
    ids = {make_ticket()["ticket_id"] for _ in range(50)}
    assert len(ids) == 50
