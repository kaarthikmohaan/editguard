"""T-I-RESUME-01 and the pipeline integration test (docs/testing.md).

The 1,000-event fixture is served by a fake EventStreams over real HTTP (Server-Sent Events).
The real producer sends it to a real Kafka and Schema Registry (throwaway Docker containers),
is stopped halfway and restarted from its bookmark, and then the live job's own bronze and
scoring code reads Kafka in Spark. Checks: no gaps, every event exactly once in bronze, and
the flags equal those computed offline from the same fixture.
"""

import gzip
import json
import shutil
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.spark,
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),  # testcontainers' own notices
]

ROOT = Path(__file__).parents[1]
SCORED_AT = datetime(2026, 9, 26, 13, 0, tzinfo=UTC)
STREAM = "mediawiki.page_change.v1"
REGISTRY_IMAGE = "confluentinc/cp-schema-registry:8.3.0"
KAFKA_IMAGE = "confluentinc/cp-kafka:8.0.0"


def fixture_events() -> list[dict[str, Any]]:
    """The fixture, with each upstream partition's offsets renumbered 0, 1, 2, ... The
    fixture keeps only the 8 target wikis, so the real offsets have holes the gap detector
    would (rightly) report; an unfiltered upstream has none."""
    lines = gzip.open(ROOT / "tests/fixtures/page_change_1k.jsonl.gz", "rt", encoding="utf-8")
    events, next_offset = [], {}
    for line in lines:
        event = json.loads(line)
        key = (event["meta"]["topic"], event["meta"]["partition"])
        event["meta"]["offset"] = next_offset.get(key, 0)
        next_offset[key] = event["meta"]["offset"] + 1
        events.append(event)
    return events


class FakeEventStreams:
    """Serves events as EventStreams does: SSE, one id per event, resuming from Last-Event-ID.
    An id is a list of (topic, partition, offset) assignments, and EventStreams starts each
    assigned partition at that offset (measured on the real service, 2026-10-03), so each
    event's id points at the next offset. Each response ends after the available events;
    the producer reconnects for more."""

    def __init__(self, events: list[dict[str, Any]], pause_after: int) -> None:
        self.events = events
        # Until the gate opens, only the first `pause_after` events exist upstream: the
        # producer cannot run past the halfway point however fast the machine is.
        self.pause_after = pause_after
        self.gate = threading.Event()
        self.ids = [
            json.dumps(
                [
                    {
                        "topic": e["meta"]["topic"],
                        "partition": e["meta"]["partition"],
                        "offset": e["meta"]["offset"] + 1,
                    }
                ]
            )
            for e in events
        ]
        self.connections = 0
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802 - http.server API
                fake.connections += 1
                last = self.headers.get("Last-Event-ID")
                starts = {
                    (a["topic"], a["partition"]): a["offset"] for a in json.loads(last or "[]")
                }
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                end = len(fake.events) if fake.gate.is_set() else fake.pause_after
                for event_id, event in zip(fake.ids[:end], fake.events[:end], strict=True):
                    meta = event["meta"]
                    if meta["offset"] < starts.get((meta["topic"], meta["partition"]), 0):
                        continue  # before the resume point of its partition
                    body = f"event: message\nid: {event_id}\ndata: {json.dumps(event)}\n\n"
                    self.wfile.write(body.encode())
                self.wfile.flush()  # then close: the producer reconnects and gets nothing new

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v2/stream"


@pytest.fixture(scope="module")
def kafka_and_registry() -> Iterator[tuple[str, str]]:
    if shutil.which("docker") is None:
        pytest.skip("integration tests need Docker")
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network
    from testcontainers.core.waiting_utils import wait_for_logs
    from testcontainers.kafka import KafkaContainer

    network = Network().create()
    kafka = KafkaContainer(KAFKA_IMAGE).with_kraft().with_network(network)
    kafka.with_network_aliases("kafka")
    kafka.start()
    registry = (
        DockerContainer(REGISTRY_IMAGE)
        .with_network(network)
        .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
        .with_exposed_ports(8081)
    )
    registry.start()
    wait_for_logs(registry, "Server started", timeout=120)
    try:
        yield (
            kafka.get_bootstrap_server(),
            f"http://{registry.get_container_host_ip()}:{registry.get_exposed_port(8081)}",
        )
    finally:
        registry.stop()
        kafka.stop()
        network.remove()


def create_topics(bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [
        NewTopic("edits.raw.v1", num_partitions=6, replication_factor=1),
        NewTopic("edits.dlq", num_partitions=1, replication_factor=1),
        NewTopic(
            "_producer_state",
            num_partitions=1,
            replication_factor=1,
            config={"cleanup.policy": "compact"},
        ),
    ]
    for future in admin.create_topics(topics).values():
        future.result(30)


def run_producer_until(received: int, timeout_s: float = 120) -> dict[str, int]:
    """Run the real producer in a thread and stop it (like Ctrl+C) once it has received
    `received` events in this run. Returns its counters."""
    from editguard.producer.__main__ import Producer
    from editguard.producer.streams import STREAMS

    producer = Producer(STREAMS["edits"])
    thread = threading.Thread(target=producer.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + timeout_s
    while producer.counts["received"] < received and time.monotonic() < deadline:
        time.sleep(0.05)
    producer.stopping = True
    thread.join(60)
    assert not thread.is_alive(), "producer did not stop"
    return producer.counts


def kafka_event_ids(bootstrap: str, registry: str) -> list[str]:
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": "check", "enable.auto.commit": False}
    )
    decode = AvroDeserializer(SchemaRegistryClient({"url": registry}))
    ids = []
    for p in range(6):
        low, high = consumer.get_watermark_offsets(TopicPartition("edits.raw.v1", p), timeout=10)
        if high == low:
            continue
        consumer.assign([TopicPartition("edits.raw.v1", p, low)])
        while True:
            msg = consumer.poll(5)
            if msg is None or msg.error():
                continue
            ids.append(
                decode(msg.value(), SerializationContext("edits.raw.v1", MessageField.VALUE))[
                    "event_id"
                ]
            )
            if msg.offset() >= high - 1:
                break
    consumer.close()
    return ids


def test_producer_resumes_without_gaps_and_spark_scores_like_offline(
    kafka_and_registry: tuple[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from editguard.common.config import get_settings
    from editguard.producer import sse
    from editguard.producer.parse import to_edit_event
    from editguard.streaming.flags import flag_records

    bootstrap, registry = kafka_and_registry
    events = fixture_events()
    half = len(events) // 2
    fake = FakeEventStreams(events, pause_after=half)
    monkeypatch.setattr(sse, "STREAM_BASE_URL", fake.url)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap)
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", registry)
    monkeypatch.setenv("CONTACT_EMAIL", "ci@example.com")
    get_settings.cache_clear()
    create_topics(bootstrap)

    # T-I-RESUME-01: stop halfway, restart from the bookmark, finish.
    first = run_producer_until(half)
    fake.gate.set()  # the rest of the stream "arrives" while the producer is stopped
    second = run_producer_until(len(events) - half)
    assert first["received"] == half  # stopped exactly halfway, so resume is exercised
    assert second["received"] == len(events) - half  # resumed after the bookmark, no re-reads
    assert first["gap_events"] == second["gap_events"] == 0
    assert first["dlq"] == second["dlq"] == 0
    assert fake.connections >= 2  # the second run reconnected with Last-Event-ID
    ids = kafka_event_ids(bootstrap, registry)
    assert set(ids) == {e["meta"]["id"] for e in events}  # every event, none lost
    assert len(ids) - len(set(ids)) <= 50  # at most the in-flight window repeated

    # The live job's code on Spark: bronze exactly once, and flags equal to offline scoring.
    bronze_ids, live_flags = run_spark(bootstrap, registry, tmp_path)
    assert sorted(bronze_ids) == sorted({e["meta"]["id"] for e in events})
    offline = flag_records([to_edit_event(e) for e in events], SCORED_AT)
    key = lambda f: (f["wiki_id"], f["rev_id"])  # noqa: E731
    assert {key(f): f["score"] for f in live_flags} == {key(f): f["score"] for f in offline}
    assert offline, "the fixture must contain flagged edits"


def run_spark(bootstrap: str, registry: str, tmp_path: Path) -> tuple[list[str], list[dict]]:
    from pyspark.sql import SparkSession

    from editguard.common.config import get_settings
    from editguard.streaming.bronze import BRONZE_DDL, registered_schemas
    from editguard.streaming.flags import flag_records
    from editguard.streaming.live_job import PACKAGES, read_edits, scoring_rows

    settings = get_settings()
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.jars.packages", PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(tmp_path / "warehouse"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        writers = registered_schemas(settings.schema_registry_url, "edits.raw.v1-value")
        spark.sql(BRONZE_DDL.format(table="local.bronze.edits"))
        (
            read_edits(spark, settings, "earliest", 50_000, writers)
            .writeStream.format("iceberg")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", str(tmp_path / "cp_bronze"))
            .toTable("local.bronze.edits")
            .awaitTermination()
        )
        flags: list[dict] = []

        def score(batch: Any, _batch_id: int) -> None:
            rows = scoring_rows(batch)
            flags.extend(flag_records(rows, SCORED_AT))

        (
            read_edits(spark, settings, "earliest", 50_000, writers)
            .writeStream.foreachBatch(score)
            .trigger(availableNow=True)
            .option("checkpointLocation", str(tmp_path / "cp_scoring"))
            .start()
            .awaitTermination()
        )
        bronze_ids = [
            r["event_id"] for r in spark.table("local.bronze.edits").select("event_id").collect()
        ]
        return bronze_ids, flags
    finally:
        spark.stop()
