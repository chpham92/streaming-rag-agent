"""Structured Streaming job: tickets.raw (Kafka) -> chunk -> embed -> upsert
into Pinecone -> tickets.indexed (Kafka).

Embedding/Pinecone/Kafka-producer calls happen inside foreachBatch rather
than a Spark UDF: they're I/O-bound network calls to external services, not
distributed compute, and foreachBatch gives a plain pandas DataFrame per
micro-batch to work with instead of fighting Spark's UDF serialization for
network clients. At this data volume (a synthetic ticket stream) that
trade-off is clearly right; a higher-throughput version would batch and
parallelize the embedding calls across partitions instead.
"""
import json
import os

from confluent_kafka import Producer
from openai import OpenAI
from pinecone import Pinecone, ServerlessSpec
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

from chunking import chunk_text

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "tickets.raw")
SINK_TOPIC = os.environ.get("SINK_TOPIC", "tickets.indexed")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX", "support-tickets")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = 1536  # text-embedding-3-small's native dimension

TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_area", StringType()),
    StructField("subject", StringType()),
    StructField("body", StringType()),
    StructField("created_at", StringType()),
])


def get_pinecone_index():
    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    existing = {idx["name"] for idx in pc.list_indexes()}
    if PINECONE_INDEX not in existing:
        pc.create_index(
            name=PINECONE_INDEX,
            dimension=EMBEDDING_DIM,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
    return pc.Index(PINECONE_INDEX)


def process_batch(batch_df, batch_id: int):
    rows = batch_df.collect()
    if not rows:
        return

    openai_client = OpenAI()
    pinecone_index = get_pinecone_index()
    kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    vectors = []
    indexed_events = []
    for row in rows:
        text = f"{row.subject}\n\n{row.body}"
        chunks = chunk_text(text)
        if not chunks:
            continue

        embeddings = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=chunks).data
        for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            vectors.append({
                "id": f"{row.ticket_id}::{i}",
                "values": emb.embedding,
                "metadata": {
                    "ticket_id": row.ticket_id,
                    "customer_id": row.customer_id,
                    "product_area": row.product_area,
                    "subject": row.subject,
                    "chunk_text": chunk,
                    "created_at": row.created_at,
                },
            })

        indexed_events.append({
            "ticket_id": row.ticket_id,
            "product_area": row.product_area,
            "chunk_count": len(chunks),
            "indexed_at_batch": batch_id,
        })

    if vectors:
        pinecone_index.upsert(vectors=vectors)
        print(f"[batch {batch_id}] upserted {len(vectors)} vectors from {len(rows)} tickets")

    for event in indexed_events:
        kafka_producer.produce(SINK_TOPIC, key=event["ticket_id"], value=json.dumps(event))
    kafka_producer.flush(timeout=10)


def main():
    spark = SparkSession.builder.appName("ticket-stream-indexer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    tickets = raw.select(
        from_json(col("value").cast("string"), TICKET_SCHEMA).alias("t")
    ).select("t.*")

    query = (
        tickets.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/ticket-indexer")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
