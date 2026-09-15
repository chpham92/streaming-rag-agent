from pydantic import BaseModel, Field


class TriageDecision(BaseModel):
    """The agent's terminal tool call — same tool-choice-forced pattern as
    fitness-agent: the model has no path to a free-text reply, so every
    triage decision is schema-valid by construction rather than parsed
    out of prose.
    """

    category: str = Field(..., description="One of: billing, auth, performance, integrations, mobile-app")
    priority: str = Field(..., description="One of: low, medium, high, urgent")
    summary: str = Field(..., max_length=280, description="One-sentence summary of the issue for a human reviewer")
    suggested_action: str = Field(..., max_length=600)
    confidence: float = Field(..., ge=0.0, le=1.0)
    similar_ticket_ids: list[str] = Field(default_factory=list, description="IDs of past tickets that informed this decision")
