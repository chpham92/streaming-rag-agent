import sys
from collections import deque
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).parent.parent / "agent"))

from models import TriageDecision  # noqa: E402
from triage_agent import (  # noqa: E402
    REPEAT_CONTACT_THRESHOLD,
    REPEAT_CONTACT_WINDOW_S,
    check_repeat_contact,
)


def valid_decision_kwargs(**overrides):
    base = dict(
        category="billing",
        priority="medium",
        summary="Customer was double-charged for their plan.",
        suggested_action="Issue a refund for the duplicate charge.",
        confidence=0.9,
    )
    base.update(overrides)
    return base


def test_triage_decision_accepts_valid_input():
    decision = TriageDecision(**valid_decision_kwargs())
    assert decision.category == "billing"
    assert decision.similar_ticket_ids == []


def test_triage_decision_rejects_confidence_out_of_range():
    with pytest.raises(ValidationError):
        TriageDecision(**valid_decision_kwargs(confidence=1.5))


def test_triage_decision_rejects_oversized_summary():
    # This is the exact failure mode that crashed the live agent the first
    # time it ran against real Claude output: an unhandled ValidationError
    # on one ticket took the whole consumer process down (see
    # triage_agent.py's try/except in main() and the commit history).
    with pytest.raises(ValidationError):
        TriageDecision(**valid_decision_kwargs(summary="x" * 281))


def test_triage_decision_rejects_oversized_suggested_action():
    with pytest.raises(ValidationError):
        TriageDecision(**valid_decision_kwargs(suggested_action="x" * 601))


def test_repeat_contact_false_below_threshold():
    history = deque()
    now = 1000.0
    for i in range(REPEAT_CONTACT_THRESHOLD - 1):
        assert check_repeat_contact(history, now + i) is False


def test_repeat_contact_true_at_threshold():
    # is_repeat is evaluated against history *before* the current contact is
    # appended, so triggering True needs REPEAT_CONTACT_THRESHOLD prior
    # contacts already recorded, not THRESHOLD - 1.
    history = deque()
    now = 1000.0
    for i in range(REPEAT_CONTACT_THRESHOLD):
        check_repeat_contact(history, now + i)
    assert check_repeat_contact(history, now + REPEAT_CONTACT_THRESHOLD) is True


def test_repeat_contact_resets_after_window_expires():
    history = deque()
    now = 1000.0
    for i in range(REPEAT_CONTACT_THRESHOLD):
        check_repeat_contact(history, now + i)
    # jump past the window — old contacts should have aged out
    later = now + REPEAT_CONTACT_WINDOW_S + 1
    assert check_repeat_contact(history, later) is False
