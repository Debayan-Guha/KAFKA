# Kafka Syllabus

## 1. Topics (Core Logical Unit)

1.1 Definition & Purpose

1.2 Naming Conventions

1.3 Topic vs Queue vs Stream

1.4 Internal Topics (__consumer_offsets, __transaction_state)

1.5 Auto-Creation of Topics (enable.auto.create)

1.6 Topic Deletion (delete.topic.enable)

## 2. Partitions

2.1 What is a Partition

2.2 Partition Key & Hashing

2.3 Partition Assignment (Round-Robin, Sticky, Uniform Sticky)

2.4 Partition Counts (How to Choose)

2.5 Partition Rebalancing

2.6 Partition Leadership

2.7 Preferred Replica & Leader Rebalance

2.8 Offsets Inside a Partition

2.9 Partition Granularity for Parallelism

2.10 Increasing Partitions (Limitations)

2.11 Decreasing Partitions (Not Allowed)

## 3. Brokers

3.1 Broker Role & Responsibilities

3.2 Broker IDs

3.3 Bootstrap Servers

3.4 Broker Configuration (server.properties)

3.5 Rack Awareness (rack.id)

3.6 Broker Failure Detection

3.7 Controlled Shutdown

3.8 Broker Metadata (in KRaft vs ZK Mode)

3.9 Broker Recovery

## 4. Replication

4.1 Replication Factor (RF)

4.2 Leader & Follower Replicas

4.3 ISR (In-Sync Replicas)

4.4 OSR (Out-of-Sync Replicas)

4.5 Unclean Leader Election (unclean.leader.election.enable)

4.6 Min In-Sync Replicas (min.insync.replicas)

4.7 Replication Throttling

4.8 Fetch from Follower (consumer.rack)

4.9 Replication Across Racks (Fault Tolerance)

## 5. Producers (Detailed)

5.1 Producer Architecture

5.2 ProducerRecord (topic, partition, key, value, timestamp, headers)

5.3 Serializers (String, Avro, JSON, Protobuf, Custom)

5.4 Partitioner (Default, Custom, Key-Based)

5.5 Interceptors

5.6 Acks Levels

5.6.1 acks=0 (Fire & Forget)

5.6.2 acks=1 (Leader Only)

5.6.3 acks=all / -1 (Leader + ISR)

5.7 Retries

5.7.1 retries Config

5.7.2 retry.backoff.ms

5.7.3 deliver.timeout.ms

5.7.4 request.timeout.ms

5.8 Batching

5.8.1 batch.size

5.8.2 linger.ms

5.8.3 buffer.memory

5.8.4 max.block.ms

5.9 Compression

5.9.1 gzip, snappy, lz4, zstd

5.9.2 Broker-Side Decompression/Compression

5.9.3 Trade-Offs (CPU vs Network)

5.10 Idempotent Producer (enable.idempotence)

5.10.1 Exactly-Once Semantics (Producer Side)

5.10.2 Sequence Numbers & Producer IDs

5.11 Transactions (Producer-Only)

5.11.1 initTransactions()

5.11.2 beginTransaction()

5.11.3 commitTransaction() / abortTransaction()

5.12 Producer Monitoring

5.12.1 request-rate, error-rate

5.12.2 record-retry-rate, record-send-total

5.12.3 batch-size-avg, compression-rate

## 6. Consumers (Detailed)

6.1 Consumer Architecture

6.2 Consumer Group Concept

6.3 Group Coordinator & Group Leader

6.4 Offset

6.4.1 Current Offset

6.4.2 Committed Offset

6.4.3 Earliest / Latest / Specific Offset

6.4.4 Offset Storage (__consumer_offsets)

6.5 Offset Commit Strategies

6.5.1 Auto Commit (enable.auto.commit, auto.commit.interval.ms)

6.5.2 Manual Sync Commit

6.5.3 Manual Async Commit

6.5.4 Commit with Callbacks

6.6 Deserializers

6.7 Poll Loop

6.7.1 poll(Duration)

6.7.2 max.poll.records

6.7.3 max.poll.interval.ms

6.7.4 fetch.min.bytes

6.7.5 fetch.max.wait.ms

6.8 Consumer Rebalancing

6.8.1 Eager Rebalancing (Stop-the-World)

6.8.2 Cooperative Rebalancing (Incremental)

6.8.3 Partition Rebalance Listeners

6.8.4 Static Group Membership (group.instance.id)

6.9 Consumer Liveness

6.9.1 session.timeout.ms

6.9.2 heartbeat.interval.ms

6.10 Consumer Position Control

6.10.1 seek()

6.10.2 seekToBeginning() / seekToEnd()

6.10.3 position()

6.10.4 pause() / resume()

6.11 Consumer Groups vs No-Group (Simple Consumer)

6.12 Consumer Lag

6.12.1 Definition & Causes

6.12.2 Tools: kafka-consumer-groups, Burrow, LagExporter

6.12.3 Reducing Lag

6.13 Multi-Threaded Consumers

6.13.1 One Consumer Per Thread

6.13.2 Single Consumer + Multiple Worker Threads

## 7. Delivery Semantics & Guarantees

7.1 At-Most-Once

7.2 At-Least-Once

7.3 Exactly-Once

7.3.1 Idempotent Producer (enable.idempotence)

7.3.2 Transactions (Producer + Consumer)

7.3.3 read_committed Isolation Level

7.3.4 Exactly-Once for Kafka Streams / Kafka Connect

7.4 When Exactly-Once Fails (Recovery Semantics)

## 8. Kafka Internals (Advanced)

8.1 Log Segments

8.1.1 segment.bytes, segment.ms

8.1.2 .log, .index, .timeindex Files

8.1.3 Rolling & Retention

8.2 Retention Policies

8.2.1 Time-Based (retention.ms)

8.2.2 Size-Based (retention.bytes)

8.2.3 Compacted (cleanup.policy=compact)

8.2.4 Delete + Compact

8.3 Log Cleaner (for Compaction)

8.4 Leader Election

8.4.1 Preferred Leader Election

8.4.2 Unclean Election (Pros/Cons)

8.4.3 Controller's Role

8.5 The Controller (Broker)

8.5.1 Controller Election (ZooKeeper / KRaft)

8.5.2 Controller Responsibilities (Partition Re-Assignment, Leader Changes)

8.6 ZooKeeper Roles (Pre-2.8)

8.6.1 Metadata Storage

8.6.2 Broker Registration

8.6.3 Controller Election

8.6.4 Topic Configuration

8.7 KRaft (Kafka Raft) – Post-2.8 / 3.x

8.7.1 Metadata Quorum

8.7.2 Controller Nodes & Voters

8.7.3 No ZooKeeper

8.7.4 Migration from ZK Mode

8.8 Request Handling

8.8.1 Acceptor Threads

8.8.2 Processor (Network) Threads

8.8.3 Kafka Request Handler Threads

8.8.4 Response Threading

## 9. Administration & Operations

9.1 Topic Management (kafka-topics.sh)

9.1.1 create, list, describe, alter, delete

9.1.2 Increase Partitions

9.1.3 Update Config (min.insync.replicas, retention, etc.)

9.2 Partition Reassignment (kafka-reassign-partitions.sh)

9.3 Preferred Replica Election (kafka-leader-election.sh)

9.4 Dump Log Segments (kafka-dump-log.sh)

9.5 Consumer Group CLI (kafka-consumer-groups.sh)

9.5.1 list, describe, reset offsets, delete

9.6 Quotas

9.6.1 Produce Quota

9.6.2 Fetch Quota

9.6.3 Request Quota (Controller Mutex)

9.6.4 Client-ID / User Based

9.7 Authentication

9.7.1 SSL / mTLS

9.7.2 SASL (PLAIN, SCRAM, GSSAPI (Kerberos), OAUTHBEARER)

9.8 Authorization

9.8.1 ACLs (Simple, Wildcard)

9.8.2 Super Users

9.8.3 Resource Types (Topic, Group, Cluster, TransactionalId)

9.9 Monitoring (Metrics & JMX)

9.9.1 Under-Replicated Partitions

9.9.2 Offline Partitions

9.9.3 ISR Shrink/Expand Rate

9.9.4 Request Handler Avg Idle %

9.9.5 Network Processor Avg Idle %

9.9.6 Consumer Lag by Group/Topic

9.10 Logging & Debugging

9.10.1 Broker Log Levels

9.10.2 Slow Consumer Detection

9.10.3 Fetcher Lag

## 10. Kafka Connect

10.1 Connect Architecture (Workers, Tasks, Connectors)

10.1.1 Workers

10.1.2 Tasks

10.1.3 Connectors

10.2 Source Connectors (Read from External)

10.3 Sink Connectors (Write to External)

10.4 Converters (Avro, JSON, Parquet, Protobuf)

10.4.1 Avro Converter

10.4.2 JSON Converter

10.4.3 Parquet Converter

10.4.4 Protobuf Converter

10.5 Transforms (Single Message Transforms)

10.6 Dead Letter Queue (DLQ)

10.7 Distributed vs Standalone Mode

10.8 Rebalancing in Connect Workers

10.9 Offset Management in Connect

## 11. Kafka Streams

11.1 Streams Architecture (Embedded Library)

11.2 KStream vs KTable vs GlobalKTable

11.3 State Stores

11.3.1 RocksDB

11.3.2 In-Memory

11.3.3 Custom

11.4 Stream-Table Duality

11.5 Operations

11.5.1 Stateless (map, filter, flatMap, groupBy, branch, split, merge)

11.5.2 Stateful (aggregate, reduce, count, windowed joins, session windows)

11.6 Windowing

11.6.1 Tumbling Windows

11.6.2 Hopping Windows

11.6.3 Session Windows

11.6.4 Sliding Windows (v2.8+)

11.6.5 Grace Period

11.7 Exactly-Once in Streams

11.8 Interactive Queries (Queryable State Stores)

11.9 Streams Rebalancing & Scaling

## 12. KSQLDB / KSQL

12.1 KSQL Architecture

12.2 Streams vs Tables in SQL

12.3 Pull vs Push Queries

12.4 Materialized Views

12.5 UDFs / UDAFs

12.5.1 UDFs (User Defined Functions)

12.5.2 UDAFs (User Defined Aggregate Functions)

12.5.3 UDF vs UDAF Comparison

12.6 Exactly-Once in KSQL

## 13. Performance Tuning

13.1 Producer Tuning

13.1.1 Batching Optimization

13.1.2 Reliability vs Performance

13.1.3 Retry Tuning

13.1.4 Memory Tuning

13.1.5 Compression Tuning

13.2 Consumer Tuning

13.2.1 Fetch Tuning

13.2.2 Poll Tuning

13.2.3 Consumer Group Scaling

13.2.4 Offset Commit Optimization

13.3 Broker Tuning

13.3.1 OS Page Cache

13.3.2 Filesystem (XFS)

13.3.3 Heap Size (6-8GB Max)

13.3.4 Log Flush Policies (Don't)

13.4 Network & Disk Bandwidth

13.5 Partition Count vs Throughput

13.6 Compression Trade-Offs

13.6.1 GZIP

13.6.2 Snappy

13.6.3 LZ4

13.6.4 ZSTD

13.6.5 Compression Algorithm Comparison

13.7 Replication Throttling for Migration

## 14. Comparison & Conceptual

14.1 Kafka vs RabbitMQ

14.2 Kafka vs Pulsar

14.3 Kafka vs Kinesis

14.4 Kafka vs Traditional Message Queues (ActiveMQ, IBM MQ)

14.5 Pull vs Push Model (Why Pull Wins)

14.6 Ordering Guarantees (Partition Level Only)

14.7 Persistence vs Deletion

14.8 Distributed Commit Log

## 15. Migration & Upgrade

15.1 Rolling Upgrade (Broker by Broker)

15.2 Upgrade from ZooKeeper to KRaft

15.3 Protocol Version Compatibility

15.4 Inter-Broker Protocol (IBP)

15.5 Log Format Version

15.6 Downgrade Safety

## 16. Common Interview Scenarios (Problem-Solving)

16.1 Duplicate Messages (Causes & Prevention)

16.2 Missing Messages (Acks, Retries, Min.insync)

16.3 Consumer Group Stuck (max.poll.interval.ms Too Small)

16.4 Rebalancing Too Often (session.timeout.ms, heartbeat.interval.ms)

16.5 High Latency (linger.ms, batch.size, fetch.min.bytes)

16.6 Disk Full (Retention, Low Capacity)

16.7 Leader Not Replicating (ISR Shrinks, Network Issues)

16.8 Exactly-Once Across Multiple Topics/Partitions

16.9 Resetting Offsets for Reprocessing

16.10 Designing a Chat System / Event-Driven Pipeline

---

# Kafka Hands-On Labs

## Topics Labs

Lab 1: Create Production Topics for an E-Commerce Platform

Lab 2: Implement Enterprise Topic Naming Standards

Lab 3: Verify Publish-Subscribe Behavior

Lab 4: Verify Queue-Like Processing Using Consumer Groups

Lab 5: Enable Topic Auto-Creation and Validate Behavior

Lab 6: Disable Topic Auto-Creation

Lab 7: Audit Automatically Created Topics

Lab 8: Inspect Kafka Internal Topics

Lab 9: Observe Offset Storage

Lab 10: Topic Configuration Review

Lab 11: Topic Deletion Validation

Lab 12: Recreate Deleted Topic

Lab 13: Production Topic Inventory Exercise

Lab 14: Topic Governance Review

Lab 15: Production Readiness Validation

## Partitions Labs

Lab 1: Create a Multi-Partition Topic

Lab 2: Explore Partition Storage

Lab 3: Key-Based Partition Routing

Lab 4: Keyless Message Distribution

Lab 5: Partition Key Design Exercise

Lab 6: Round-Robin Partition Assignment

Lab 7: Sticky Partition Assignment

Lab 8: Uniform Sticky Partition Assignment

Lab 9: Partition Count Planning Exercise

Lab 10: Consumer Parallelism Using Partitions

Lab 11: Consumer Rebalancing - New Consumer Join

Lab 12: Consumer Rebalancing - Consumer Failure

Lab 13: Partition Leadership Inspection

Lab 14: Leader Failover Simulation

Lab 15: Preferred Replica Election

Lab 16: Offset Exploration

Lab 17: Consumer Offset Tracking

Lab 18: Partition Scaling for Throughput

Lab 19: Increase Partition Count

Lab 20: Impact of Partition Increase on Consumer Groups

Lab 21: Partition Migration Strategy

Lab 22: Topic Recreation for Partition Reduction

## Brokers Labs

Lab 1: Deploy a Single Kafka Broker

Lab 2: Build a Multi-Broker Kafka Cluster

Lab 3: Broker ID Configuration and Validation

Lab 4: Bootstrap Server Discovery

Lab 5: Broker Configuration Audit

Lab 6: Modify Broker Configuration and Restart

Lab 7: Configure Rack Awareness

Lab 8: Cross-Rack Replication Validation

Lab 9: Broker Failure Detection

Lab 10: Producer and Consumer Behavior During Broker Failure

Lab 11: Controlled Broker Shutdown

Lab 12: Uncontrolled Broker Shutdown

Lab 13: Broker Metadata Inspection (KRaft)

Lab 14: ZooKeeper-Based Metadata Inspection

Lab 15: KRaft vs ZooKeeper Comparison Exercise

Lab 16: Broker Recovery After Restart

Lab 17: Broker Recovery After Crash

Lab 18: Replica Synchronization After Recovery

Lab 19: Broker Health Monitoring

Lab 20: Production Broker Readiness Validation

## Replication Labs

Lab 1: Create Topics with Different Replication Factors

Lab 2: Replication Factor Failure Tolerance Validation

Lab 3: Inspect Leader and Follower Replicas

Lab 4: Validate Producer Writes Through Leaders

Lab 5: Validate Consumer Reads from Leaders

Lab 6: Leader Failure and Automatic Failover

Lab 7: ISR Inspection and Monitoring

Lab 8: Trigger ISR Shrink Event

Lab 9: ISR Recovery Validation

Lab 10: Simulate Out-of-Sync Replica (OSR)

Lab 11: OSR Recovery and Synchronization

Lab 12: Unclean Leader Election Demonstration

Lab 13: Clean vs Unclean Leader Election Comparison

Lab 14: Configure Min In-Sync Replicas

Lab 15: Write Failure Due to Insufficient ISR

Lab 16: Compare Different min.insync.replicas Values

Lab 17: Replication Throttling During Recovery

Lab 18: Replication Throttling During Partition Reassignment

Lab 19: Monitor Replication Metrics

Lab 20: Configure Fetch from Follower

Lab 21: Cross-Rack Consumer Optimization

Lab 22: Rack-Aware Replication Configuration

Lab 23: Simulate Rack Failure

Lab 24: Availability Zone Failure Simulation

Lab 25: Production Replication Readiness Assessment

## Producers Labs

Lab 1: Build a Basic Kafka Producer

Lab 2: Analyze Producer Architecture

Lab 3: ProducerRecord Field Validation

Lab 4: Message Headers Implementation

Lab 5: String Serializer Implementation

Lab 6: JSON Serializer Implementation

Lab 7: Avro Serialization with Schema Registry

Lab 8: Protobuf Serialization

Lab 9: Custom Serializer Development

Lab 10: Default Partitioner Validation

Lab 11: Key-Based Partitioning

Lab 12: Sticky Partitioner Demonstration

Lab 13: Custom Partitioner Development

Lab 14: Producer Interceptor Implementation

Lab 15: Audit and Monitoring Interceptors

Lab 16: acks=0 Performance Test

Lab 17: acks=1 Reliability Test

Lab 18: acks=all Durability Test

Lab 19: Compare Acknowledgment Modes

Lab 20: Retry Mechanism Validation

Lab 21: retry.backoff.ms Experiment

Lab 22: deliver.timeout.ms Validation

Lab 23: request.timeout.ms Validation

Lab 24: Producer Batching Performance Test

Lab 25: batch.size Optimization

Lab 26: linger.ms Optimization

Lab 27: buffer.memory Stress Test

Lab 28: max.block.ms Validation

Lab 29: GZIP Compression Benchmark

Lab 30: Snappy Compression Benchmark

Lab 31: LZ4 Compression Benchmark

Lab 32: ZSTD Compression Benchmark

Lab 33: Compression Comparison Report

Lab 34: Enable Idempotent Producer

Lab 35: Idempotence Failure Recovery Test

Lab 36: Producer ID and Sequence Number Analysis

Lab 37: Transactional Producer Setup

Lab 38: Transaction Commit Workflow

Lab 39: Transaction Abort Workflow

Lab 40: Multi-Partition Transaction Test

Lab 41: Multi-Topic Transaction Test

Lab 42: Transaction Failure Recovery

Lab 43: Producer Metrics Monitoring

Lab 44: Request Rate and Error Rate Analysis

Lab 45: Retry Metrics Analysis

Lab 46: Batch Size and Compression Monitoring

Lab 47: End-to-End Producer Performance Tuning

Lab 48: Production Producer Readiness Assessment

## Consumers Labs

Lab 1: Create and Run a Basic Consumer

Lab 2: Consumer Group Load Balancing

Lab 3: Multiple Consumer Groups

Lab 4: Observe Group Coordinator Activity

Lab 5: Offset Tracking Validation

Lab 6: Inspect __consumer_offsets Topic

Lab 7: Auto Commit Testing

Lab 8: Manual Sync Commit Testing

Lab 9: Manual Async Commit Testing

Lab 10: Commit Callback Validation

Lab 11: Consumer Restart Recovery

Lab 12: Earliest Offset Consumption

Lab 13: Latest Offset Consumption

Lab 14: Seek to Specific Offset

Lab 15: Seek to Beginning and End

Lab 16: Position Monitoring

Lab 17: Pause and Resume Consumption

Lab 18: Poll Loop Tuning

Lab 19: max.poll.interval.ms Failure Scenario

Lab 20: fetch.min.bytes Optimization

Lab 21: fetch.max.wait.ms Optimization

Lab 22: Eager Rebalancing Demonstration

Lab 23: Cooperative Rebalancing Demonstration

Lab 24: Rebalance Listener Testing

Lab 25: Static Membership Testing

Lab 26: Session Timeout Failure Simulation

Lab 27: Heartbeat Configuration Testing

Lab 28: Consumer Lag Generation

Lab 29: Consumer Lag Reduction

Lab 30: Consumer Group Monitoring

Lab 31: Multi-Threaded Consumer Design

Lab 32: Worker Thread Processing Model

Lab 33: Consumer Failure Recovery

Lab 34: Production Consumer Validation

## Kafka Administration & Operations - Hands-On Labs

Lab 1: Topic Lifecycle Management

Lab 2: Production Topic Administration Audit

Lab 3: Increase Topic Partitions

Lab 4: Partition Expansion Impact Analysis

Lab 5: Update Topic Retention Policies

Lab 6: Configure min.insync.replicas

Lab 7: Topic Configuration Governance

Lab 8: Partition Reassignment Between Brokers

Lab 9: Broker Expansion and Rebalancing

Lab 10: Reassignment Throttling Validation

Lab 11: Preferred Leader Election

Lab 12: Leader Imbalance Recovery

Lab 13: Dump Kafka Log Segments

Lab 14: Analyze Index and Time Index Files

Lab 15: Log Corruption Investigation

Lab 16: Consumer Group Administration

Lab 17: Consumer Lag Analysis

Lab 18: Offset Reset Operations

Lab 19: Consumer Group Cleanup

Lab 20: Producer Quota Enforcement

Lab 21: Consumer Fetch Quota Enforcement

Lab 22: Client-Based Quota Management

Lab 23: SSL Encryption Setup

Lab 24: Mutual TLS (mTLS) Authentication

Lab 25: SASL Authentication Setup

Lab 26: ACL-Based Authorization

Lab 27: Wildcard ACL Management

Lab 28: Super User Administration

Lab 29: Transactional ID Authorization

Lab 30: JMX Metrics Monitoring

Lab 31: Prometheus and Grafana Integration

Lab 32: Under-Replicated Partition Investigation

Lab 33: Offline Partition Recovery

Lab 34: ISR Shrink and Expansion Monitoring

Lab 35: Request Handler Capacity Analysis

Lab 36: Network Processor Capacity Analysis

Lab 37: Consumer Lag Dashboard Creation

Lab 38: Dynamic Broker Log Level Changes

Lab 39: Slow Consumer Investigation

Lab 40: Fetcher Lag Troubleshooting

Lab 41: Production Operations Runbook Exercise

## Performance Tuning Labs

LAB 1: Producer Batching Performance Tuning

LAB 2: Producer Reliability vs Performance

LAB 3: Producer Retry Configuration Tuning

LAB 4: Producer Memory Buffer Tuning

LAB 5: Producer Compression Benchmark

LAB 6: Consumer Fetch Tuning

LAB 7: Consumer Poll Loop Optimization

LAB 8: Consumer Group Scaling

LAB 9: Offset Commit Performance Analysis

LAB 10: Consumer Lag Investigation and Tuning

LAB 11: Broker Page Cache Analysis

LAB 12: Broker Heap Size Tuning

LAB 13: Log Flush Performance Analysis

LAB 14: Disk and Network Bandwidth Monitoring

LAB 15: Partition Count vs Throughput Benchmark

LAB 16: Compression Algorithm Comparison

LAB 17: Replication Throttling During Migration

LAB 18: End-to-End Kafka Performance Benchmark
