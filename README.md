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
                -> triage-agent (triage_agent.py)
                    -> Pinecone query (RAG: similar past tickets)
                    -> Claude tool call -> structured TriageDecision
```

Everything runs locally via Docker Compose — real Apache Kafka (KRaft mode,
no ZooKeeper) and real PySpark, no cloud cost to demo it. See `DEPLOY_GKE.md`-style
notes below if you want to push this to a real cluster later (not built yet
for this project — ask before assuming it exists).

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

## Tests

```bash
pip install -r requirements-test.txt
pytest -q
```

15 tests covering chunking edge cases, ticket generation, `TriageDecision`
validation (including the exact failure mode that crashed the live agent),
and the repeat-contact window logic — all against scripted/pure-function
inputs, no live credentials required. CI runs this on every push.
