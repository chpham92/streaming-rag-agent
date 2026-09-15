"""Consumes tickets.indexed, retrieves similar past tickets from Pinecone
(the same index stream_indexer.py just wrote into), and forces a Claude
tool call to produce a structured triage decision — same tool-choice-forced
pattern as fitness-agent's emit_plan, for the same reason: a guaranteed-
valid decision beats parsing one out of prose.

"Memory handling" here is deliberately simple: an in-process, per-customer
recent-ticket counter used only to flag repeat contacts within this
process's lifetime. It resets on restart and isn't shared across replicas
— call it out as a real limitation rather than disguising it, the same way
fitness-agent's Deployment comment documents its own SQLite single-writer
ceiling instead of hiding it. A production version would move this to
something like Redis so it survives restarts and works across replicas.
"""
import json
import os
import time
from collections import defaultdict, deque

from anthropic import Anthropic
from confluent_kafka import Consumer
from openai import OpenAI
from pinecone import Pinecone
from prometheus_client import Counter, Histogram, start_http_server

from models import TriageDecision

METRICS_PORT = int(os.environ.get("METRICS_PORT", "9200"))

# Labels kept low-cardinality on purpose (category/priority are closed sets
# from TriageDecision's own field descriptions) — Prometheus is a bad fit
# for anything with unbounded label values like ticket_id or customer_id.
TICKETS_PROCESSED = Counter(
    "triage_tickets_processed_total", "Tickets successfully triaged", ["category", "priority"]
)
TICKETS_FAILED = Counter(
    "triage_tickets_failed_total", "Tickets that errored during triage", ["error_type"]
)
TRIAGE_DURATION = Histogram(
    "triage_duration_seconds", "Time spent per ticket in retrieval + Claude triage call"
)
REPEAT_CONTACTS = Counter(
    "triage_repeat_contacts_total", "Tickets flagged as a repeat contact within the window"
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "tickets.indexed")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX", "support-tickets")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
CLAUDE_MODEL = os.environ.get("TRIAGE_AGENT_MODEL", "claude-opus-5")
TOP_K = 3
REPEAT_CONTACT_WINDOW_S = 600  # 10 minutes
REPEAT_CONTACT_THRESHOLD = 2   # this ticket + N prior within the window

TRIAGE_TOOL = {
    "name": "emit_triage_decision",
    "description": "Record the triage decision for this support ticket.",
    "input_schema": TriageDecision.model_json_schema(),
}


def build_system_prompt() -> str:
    return (
        "You are a support-ticket triage agent for an enterprise SaaS product. "
        "Given a new ticket and semantically similar past tickets retrieved from "
        "the knowledge base, classify it and recommend an action. Weigh the "
        "similar tickets as context, not ground truth — the current ticket may "
        "differ in ways that matter. If nothing similar was retrieved, decide "
        "from the ticket content alone and reflect that in a lower confidence. "
        "You must call emit_triage_decision exactly once with your decision."
    )


def fetch_ticket(pinecone_index, ticket_id: str) -> dict | None:
    result = pinecone_index.fetch(ids=[f"{ticket_id}::0"])
    vectors = result.vectors if hasattr(result, "vectors") else result.get("vectors", {})
    match = vectors.get(f"{ticket_id}::0")
    if not match:
        return None
    return dict(match.metadata if hasattr(match, "metadata") else match["metadata"])


def find_similar(pinecone_index, openai_client, ticket: dict, ticket_id: str) -> list[dict]:
    query_text = f"{ticket['subject']}\n\n{ticket['chunk_text']}"
    embedding = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=[query_text]).data[0].embedding
    results = pinecone_index.query(vector=embedding, top_k=TOP_K + 1, include_metadata=True)
    matches = results.matches if hasattr(results, "matches") else results["matches"]

    similar = []
    for match in matches:
        meta = match.metadata if hasattr(match, "metadata") else match["metadata"]
        if meta.get("ticket_id") == ticket_id:
            continue  # a ticket is always its own nearest neighbor — exclude it
        similar.append(meta)
        if len(similar) >= TOP_K:
            break
    return similar


def check_repeat_contact(history: deque, now: float) -> bool:
    """Mutates history in place (drops stale entries, records this contact)
    and returns whether this counts as a repeat within the window. Pulled
    out of main()'s loop body specifically so it's unit-testable without a
    live Kafka consumer.
    """
    while history and now - history[0] > REPEAT_CONTACT_WINDOW_S:
        history.popleft()
    is_repeat = len(history) >= REPEAT_CONTACT_THRESHOLD
    history.append(now)
    return is_repeat


def triage(anthropic_client, ticket: dict, similar: list[dict], repeat_contact: bool) -> TriageDecision:
    similar_block = "\n\n".join(
        f"- [{s['ticket_id']}] ({s['product_area']}) {s['subject']}: {s['chunk_text']}"
        for s in similar
    ) or "(no similar past tickets found)"

    user_message = (
        f"NEW TICKET\n"
        f"product_area: {ticket['product_area']}\n"
        f"subject: {ticket['subject']}\n"
        f"body: {ticket['chunk_text']}\n"
        f"repeat_contact_from_this_customer: {repeat_contact}\n\n"
        f"SIMILAR PAST TICKETS\n{similar_block}"
    )

    response = anthropic_client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        system=build_system_prompt(),
        tools=[TRIAGE_TOOL],
        tool_choice={"type": "tool", "name": "emit_triage_decision"},
        messages=[{"role": "user", "content": user_message}],
    )

    tool_use = next(b for b in response.content if b.type == "tool_use")
    decision = TriageDecision.model_validate(tool_use.input)
    decision.similar_ticket_ids = [s["ticket_id"] for s in similar]
    return decision


def main():
    start_http_server(METRICS_PORT)
    print(f"metrics exposed on :{METRICS_PORT}/metrics")

    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    pinecone_index = pc.Index(PINECONE_INDEX)
    openai_client = OpenAI()
    anthropic_client = Anthropic()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "triage-agent",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([SOURCE_TOPIC])

    recent_by_customer: dict[str, deque] = defaultdict(deque)

    print(f"triage-agent listening on {SOURCE_TOPIC}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            ticket_id = event["ticket_id"]

            ticket = fetch_ticket(pinecone_index, ticket_id)
            if ticket is None:
                print(f"[{ticket_id}] no vector found yet, skipping (may be a later chunk)")
                continue

            history = recent_by_customer[ticket["customer_id"]]
            repeat_contact = check_repeat_contact(history, time.time())
            if repeat_contact:
                REPEAT_CONTACTS.inc()

            try:
                with TRIAGE_DURATION.time():
                    similar = find_similar(pinecone_index, openai_client, ticket, ticket_id)
                    decision = triage(anthropic_client, ticket, similar, repeat_contact)
            except Exception as exc:
                TICKETS_FAILED.labels(error_type=type(exc).__name__).inc()
                # A malformed model output or a transient API error on one
                # ticket must not take down the whole consumer — this crashed
                # the process outright the first time it happened (a
                # suggested_action over the old 400-char limit raised an
                # unhandled Pydantic ValidationError). Same "one bad item
                # shouldn't sink the batch" principle as fitness-agent's
                # tool-error-recovery tests, just applied to a long-running
                # consumer loop instead of a single request/response.
                print(json.dumps({
                    "ticket_id": ticket_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }))
                continue

            TICKETS_PROCESSED.labels(category=decision.category, priority=decision.priority).inc()
            print(json.dumps({
                "ticket_id": ticket_id,
                "customer_id": ticket["customer_id"],
                "repeat_contact": repeat_contact,
                **decision.model_dump(),
            }))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
