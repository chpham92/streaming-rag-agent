# streaming-rag-agent

A real-time support-ticket triage pipeline: new tickets stream through Kafka,
get chunked/embedded/indexed into Pinecone by a Spark Structured Streaming
job, and a Claude tool-calling agent triages each one against the growing
knowledge base of past tickets it retrieves via RAG.

Built to close a specific gap: hands-on production experience with event
streaming (Kafka) and a data pipeline (Spark), on top of the agentic
LLM/RAG work in my other projects ([fitness-agent](https://github.com/chpham92/fitness-agent),
[research-agent](https://github.com/chpham92/research-agent)).

## Architecture

```
producer (synthetic tickets)
    -> Kafka topic: tickets.raw
        -> Spark Structured Streaming (stream_indexer.py)
            -> chunk -> OpenAI embeddings -> Pinecone upsert
            -> Kafka topic: tickets.indexed
                -> triage-agent (triage_agent.py)  --/metrics--> Prometheus -> Alertmanager -> alert-receiver
                    -> Pinecone query (RAG: similar past tickets)      ^                              (logs the alert)
                    -> Claude tool call -> structured TriageDecision   |
                                                          kafka-exporter (consumer lag)
                                                                        |
                                                                    Grafana (dashboards)
```

Everything runs locally via Docker Compose — real Apache Kafka (KRaft mode,
no ZooKeeper), real PySpark, and a real Prometheus/Alertmanager/Grafana stack,
no cloud cost to demo any of it. See `DEPLOY_GKE.md`-style notes below if you
want to push this to a real cluster later (not built yet for this project —
ask before assuming it exists).

## Running it

```bash
export OPENAI_API_KEY=...
export PINECONE_API_KEY=...
export ANTHROPIC_API_KEY=...
docker compose up -d --build
```

Then watch a service:

```bash
docker logs -f triage-agent      # real triage decisions, one per ticket
docker logs -f spark-indexer     # batch upsert confirmations
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic tickets.indexed --from-beginning
```

**Stop it when you're not actively using it** — the producer emits a new
ticket every 10s by default, and each one costs a real (small) OpenAI
embedding call plus a real Claude call: `docker compose down`.

## Observability

- **Grafana** — http://localhost:3000 (anonymous admin access, local-only
  demo box): the "Streaming Triage Pipeline" dashboard — agent up/down,
  Kafka consumer lag, tickets processed by priority/category, triage
  latency p50/p95, failures by error type.
- **Prometheus** — http://localhost:9090 — `triage-agent` exposes
  `/metrics` directly (`triage_tickets_processed_total`,
  `triage_tickets_failed_total`, `triage_duration_seconds`,
  `triage_repeat_contacts_total`); `kafka-exporter` exposes
  `kafka_consumergroup_lag` per consumer group/topic/partition.
- **Alertmanager** — http://localhost:9093 — three rules in
  `monitoring/prometheus/alerts.yml`: `TriageAgentDown` (scrape target
  unreachable 30s+), `TriageTicketFailures` (any failure in the last 2m —
  intentionally sensitive for a demo; a real deployment would tune this to
  something like 3+ in 10m), `TriageConsumerLagHigh` (agent falling behind
  the stream). Routed to `alert-receiver`, a minimal stand-in for a real
  destination (PagerDuty/Slack) that logs the full firing → resolved
  lifecycle — verified live: stopped `triage-agent`, watched
  `TriageAgentDown` go `pending` → `firing`, confirmed the receiver logged
  it, restarted the container, confirmed the `resolved` notification also
  arrived.

## Design notes / real bugs hit building this

- **`spark.jars.ivy` and a manual `/etc/passwd` entry, both in
  `pipeline/Dockerfile`**: the Spark image runs as a non-root uid with no
  passwd entry. Ivy's cache resolution and the JVM's native Unix login
  module both fail on that in different, unrelated ways — neither
  `HADOOP_USER_NAME` nor a `JAVA_TOOL_OPTIONS` system property fixes the
  second one, since it's a native/JNI call that reads the OS user database
  directly. See the comments in that Dockerfile for the full chain.
- **`bitnamilegacy/spark`, not `bitnami/spark`**: Bitnami moved its
  free-tier images to a new org mid-2025; the old tags 404 now.
- **The triage agent crashed the whole consumer process the first time it
  hit a real (not synthetic) validation failure** — Claude's
  `suggested_action` exceeded the Pydantic field's `max_length` and an
  unhandled `ValidationError` took the container down. Fixed with a
  per-message try/except (`agent/triage_agent.py`) plus a
  `restart: unless-stopped` policy as a second line of defense — verified
  live afterward: two more oversized outputs got logged and skipped while
  the consumer kept processing everything else.
- **SQLite-single-writer-style constraint, but for Kafka consumer groups**:
  `triage-agent`'s per-customer repeat-contact memory (`recent_by_customer`
  in `triage_agent.py`) is in-process and resets on restart — fine for one
  replica, would need to move to something shared (Redis) before running
  more than one.
- **Prometheus's default 1-minute rule-group evaluation interval, not a
  bug but a real "why hasn't this fired yet" moment**: `for: 30s` on
  `TriageAgentDown` doesn't mean it fires 30s after the target goes down —
  it means the condition has to hold across evaluations spanning 30s, and
  with the default 60s evaluation interval that's actually ~60-90s in
  practice (breach detected at evaluation N, `pending` recorded, still
  `pending` at N+1 if that's under 30s since `activeAt`, `firing` at N+2).
  Watched `lastEvaluation` sit on the same timestamp for a full minute
  before realizing it just hadn't run again yet, not that anything was
  stuck.

## Tests

```bash
pip install -r requirements-test.txt
pytest -q
```

15 tests covering chunking edge cases, ticket generation, `TriageDecision`
validation (including the exact failure mode that crashed the live agent),
and the repeat-contact window logic — all against scripted/pure-function
inputs, no live credentials required. CI runs this on every push.
