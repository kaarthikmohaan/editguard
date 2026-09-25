# 0001. Apache Kafka over Redpanda

- Status: accepted
- Date: 2026-09-25

## Context
The pipeline needs a durable, replayable log between the stream and Spark. It runs on one Mac. Kafka appears in about 17% of data engineer postings; Redpanda is rarely named.

## Options
| Option | Pros | Cons |
| --- | --- | --- |
| Apache Kafka 4.3.1 (KRaft) + Confluent Schema Registry | The skill employers name; no ZooKeeper since 4.0 | More containers and memory |
| Redpanda | One binary with registry and console; lighter | Less recognised; same client code anyway |

## Decision
Kafka 4.3.1 with 3 KRaft brokers and Confluent Schema Registry.

## Consequences
- Heap capped at 512 MB per broker to fit the laptop.
- Three brokers on one host show ISR and `min.insync.replicas` behaviour, not real high availability. Stated in the README.
- Code uses plain Kafka clients, so a move to MSK or Confluent Cloud is a config change.
