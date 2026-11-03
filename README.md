# Tributary — Java Event Broker Simulator

A local simulator for sending and receiving queued messages. Create topics, send sample events and inspect how consumers share the work.

Maintained by **Weihou Zeng (PeterZG)**.

## Features

- Typed topics and FIFO partitions.
- Manual and random producer allocation.
- Consumer groups with range or round-robin rebalancing.
- Automatic reassignment when partitions or consumers change.
- Thread-safe partition access and immutable played-event history.
- An interactive command-line interface.

## How consumption works

A consumer group assigns each partition to at most one of its consumers. Groups maintain their own assignments but share the same destructive partition queue: once an event is consumed, another group cannot read it through an independent offset.

Played-event history supports inspection and playback. This is not a durable per-group log.

## Technology and architecture

Java 17, Gradle, JUnit, Checkstyle and JaCoCo.

```text
CLI -> Cluster -> Topic -> Partition -> FIFO events
             -> Producer
             -> ConsumerGroup -> Consumer
                              -> RebalancingStrategy
```

```text
app/src/main/java/tributary/
  api/                  Public event, topic and consumer models
  core/                 Producers and rebalancing strategies
  cli/                  CLI and in-memory registry
app/src/test/           Behaviour and concurrency tests
app/events/             Example event JSON files
config/                 Checkstyle configuration
```

## Build and run

Install JDK 17+:

```sh
./gradlew run --console=plain
```

On Windows, use `gradlew.bat`.

Enter `help` to see the available commands, or `exit` to close the simulator.

### Send your first message

The Gradle run task starts the CLI in `app/`, where the sample `events/` folder is available. Enter these commands one line at a time:

```text
create topic updates String
create partition updates inbox
create consumer group readers updates range
create consumer readers reader
create producer writer String manual
produce event writer updates events/event01.json inbox
consume event reader inbox
exit
```

The producer queues `Event01` in `inbox`; the consumer then prints its ID and message. In the reference below, values inside `<...>` are placeholders for your own names.

## CLI reference

```text
create topic <id> <type>
create partition <topic> <id>
create consumer group <id> <topic> <rebalancing>
create consumer <group> <id>
create producer <id> <type> <allocation>
produce event <producer> <topic> <event> <partition>
consume event <consumer> <partition>
show topic <topic>
show consumer group <group>
show consumer <consumer>
show producer <producer>
set consumer group rebalancing <group> <rebalancing>
set producer allocation <producer> <manual|random>
playback <consumer> <partition> <offset>
delete partition <topic> <partition>
delete consumer <consumer>
delete consumer group <group>
delete producer <producer>
delete topic <topic>
```

Use the files in `app/events/` for event payload examples. The CLI help also lists batch, parallel and deletion commands.

Payload types are `String` or `Integer`; producer and topic types must match.
JSON files are read as UTF-8. Integer payloads must be whole JSON numbers within
the signed 32-bit range; fractional, oversized and quoted numeric values are rejected before publishing.
Identifiers for partitions, consumer groups and consumers are unique across the
CLI cluster because commands refer to them by ID. Removing a partition discards
its queued events and rebalances the remaining assignments; removing a topic
also removes its consumer groups. Topic and event payload types are immutable.
Removing a consumer, group or topic clears the detached consumers' old assignments.
Parallel operations report worker failures but do not roll back successful tasks.

## Tests

```sh
./gradlew test lint assemble
./gradlew jacocoTestReport
```

Tests cover FIFO handling, empty queues, immutable history, consumer assignment, rebalancing and concurrent partition access. Coverage reports are written under `app/build/reports/jacoco/`.

The GitHub workflow runs tests, style checks, assembly and coverage with Java 17. The retained `.gitlab-ci.yml` is the original course-runner configuration. Neither pipeline deploys a service. Confirm remote job conclusions separately from local test results.

## Limitations

All state lives in memory. There is no network transport, replication, durable storage or distributed recovery. Partition operations are synchronized; configure the wider cluster topology serially.

[中文说明](README_zh.md) · [Code guide](CODE_GUIDE.md)
