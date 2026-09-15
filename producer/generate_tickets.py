"""Publishes synthetic support-ticket events to Kafka at a steady rate.

Stands in for whatever real ingestion source a production version of this
pipeline would have (a helpdesk webhook, a CDC stream off a tickets table).
The event shape (tickets.raw) is the actual contract the rest of the
pipeline depends on, so it's kept realistic on purpose.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from tickets_seed import BROWSERS, PLANS, PRODUCT_AREAS, TEMPLATES

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.environ.get("TOPIC", "tickets.raw")
EVENTS_PER_MINUTE = float(os.environ.get("EVENTS_PER_MINUTE", "6"))


def make_ticket() -> dict:
    area = random.choice(PRODUCT_AREAS)
    subject_tpl, body_tpl = random.choice(TEMPLATES[area])
    ctx = {
        "minutes": random.choice([5, 10, 15, 20, 30, 45, 90]),
        "browser": random.choice(BROWSERS),
        "plan": random.choice(PLANS),
        "seats": random.randint(10, 50),
        "seats_actual": random.randint(5, 9),
    }
    return {
        "ticket_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1000, 9999)}",
        "product_area": area,
        "subject": subject_tpl.format(**ctx),
        "body": body_tpl.format(**ctx),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"produced ticket to {msg.topic()}[{msg.partition()}]")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    interval_s = 60.0 / EVENTS_PER_MINUTE
    print(f"producing to {BOOTSTRAP}/{TOPIC} every {interval_s:.1f}s")
    while True:
        ticket = make_ticket()
        producer.produce(
            TOPIC,
            key=ticket["ticket_id"],
            value=json.dumps(ticket),
            callback=delivery_report,
        )
        producer.poll(0)
        producer.flush(timeout=5)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
