# Kafka Syllabus

## 1. Topics (Core Logical Unit)

[1.1 Definition & Purpose](kafka-syllabus.md#11-definition--purpose)

[1.2 Naming Conventions](kafka-syllabus.md#12-naming-conventions)

[1.3 Topic vs Queue vs Stream](kafka-syllabus.md#13-topic-vs-queue-vs-stream)

[1.4 Internal Topics (__consumer_offsets, __transaction_state)](kafka-syllabus.md#14-internal-topics-__consumer_offsets-__transaction_state)

[1.5 Auto-Creation of Topics (enable.auto.create)](kafka-syllabus.md#15-auto-creation-of-topics-enableautocreate)

[1.6 Topic Deletion (delete.topic.enable)](kafka-syllabus.md#16-topic-deletion-deletetopicenable)

## 2. Partitions

[2.1 What is a Partition](kafka-syllabus.md#21-what-is-a-partition)

[2.2 Partition Key & Hashing](kafka-syllabus.md#22-partition-key--hashing)

[2.3 Partition Assignment (Round-Robin, Sticky, Uniform Sticky)](kafka-syllabus.md#23-partition-assignment-round-robin-sticky-uniform-sticky)

[2.4 Partition Counts (How to Choose)](kafka-syllabus.md#24-partition-counts-how-to-choose)

[2.5 Partition Rebalancing](kafka-syllabus.md#25-partition-rebalancing)

[2.6 Partition Leadership](kafka-syllabus.md#26-partition-leadership)

[2.7 Preferred Replica & Leader Rebalance](kafka-syllabus.md#27-preferred-replica--leader-rebalance)

[2.8 Offsets Inside a Partition](kafka-syllabus.md#28-offsets-inside-a-partition)

[2.9 Partition Granularity for Parallelism](kafka-syllabus.md#29-partition-granularity-for-parallelism)

[2.10 Increasing Partitions (Limitations)](kafka-syllabus.md#210-increasing-partitions-limitations)

[2.11 Decreasing Partitions (Not Allowed)](kafka-syllabus.md#211-decreasing-partitions-not-allowed)

## 3. Brokers

[3.1 Broker Role & Responsibilities](kafka-syllabus.md#31-broker-role--responsibilities)

[3.2 Broker IDs](kafka-syllabus.md#32-broker-ids)

[3.3 Bootstrap Servers](kafka-syllabus.md#33-bootstrap-servers)

[3.4 Broker Configuration (server.properties)](kafka-syllabus.md#34-broker-configuration-serverproperties)

[3.5 Rack Awareness (rack.id)](kafka-syllabus.md#35-rack-awareness-rackid)

[3.6 Broker Failure Detection](kafka-syllabus.md#36-broker-failure-detection)

[3.7 Controlled Shutdown](kafka-syllabus.md#37-controlled-shutdown)

[3.8 Broker Metadata (in KRaft vs ZK Mode)](kafka-syllabus.md#38-broker-metadata-in-kraft-vs-zk-mode)

[3.9 Broker Recovery](kafka-syllabus.md#39-broker-recovery)

## 4. Replication

[4.1 Replication Factor (RF)](kafka-syllabus.md#41-replication-factor-rf)

[4.2 Leader & Follower Replicas](kafka-syllabus.md#42-leader--follower-replicas)

[4.3 ISR (In-Sync Replicas)](kafka-syllabus.md#43-isr-in-sync-replicas)

[4.4 OSR (Out-of-Sync Replicas)](kafka-syllabus.md#44-osr-out-of-sync-replicas)

[4.5 Unclean Leader Election (unclean.leader.election.enable)](kafka-syllabus.md#45-unclean-leader-election-uncleanleaderelectionenable)

[4.6 Min In-Sync Replicas (min.insync.replicas)](kafka-syllabus.md#46-min-in-sync-replicas-mininsyncreplicas)

[4.7 Replication Throttling](kafka-syllabus.md#47-replication-throttling)

[4.8 Fetch from Follower (consumer.rack)](kafka-syllabus.md#48-fetch-from-follower-consumerrack)

[4.9 Replication Across Racks (Fault Tolerance)](kafka-syllabus.md#49-replication-across-racks-fault-tolerance)

## 5. Producers (Detailed)

[5.1 Producer Architecture](kafka-syllabus.md#51-producer-architecture)

[5.2 ProducerRecord (topic, partition, key, value, timestamp, headers)](kafka-syllabus.md#52-producerrecord-topic-partition-key-value-timestamp-headers)

[5.3 Serializers (String, Avro, JSON, Protobuf, Custom)](kafka-syllabus.md#53-serializers-string-avro-json-protobuf-custom)

[5.4 Partitioner (Default, Custom, Key-Based)](kafka-syllabus.md#54-partitioner-default-custom-key-based)

[5.5 Interceptors](kafka-syllabus.md#55-interceptors)

[5.6 Acks Levels](kafka-syllabus.md#56-acks-levels)

[5.6.1 acks=0 (Fire & Forget)](kafka-syllabus.md#561-acks0-fire--forget)

[5.6.2 acks=1 (Leader Only)](kafka-syllabus.md#562-acks1-leader-only)

[5.6.3 acks=all / -1 (Leader + ISR)](kafka-syllabus.md#563-acksall--1-leader--isr)

[5.7 Retries](kafka-syllabus.md#57-retries)

[5.7.1 retries Config](kafka-syllabus.md#571-retries-config)

[5.7.2 retry.backoff.ms](kafka-syllabus.md#572-retrybackoffms)

[5.7.3 deliver.timeout.ms](kafka-syllabus.md#573-delivertimeoutms)

[5.7.4 request.timeout.ms](kafka-syllabus.md#574-requesttimeoutms)

[5.8 Batching](kafka-syllabus.md#58-batching)

[5.8.1 batch.size](kafka-syllabus.md#581-batchsize)

[5.8.2 linger.ms](kafka-syllabus.md#582-lingerms)

[5.8.3 buffer.memory](kafka-syllabus.md#583-buffermemory)

[5.8.4 max.block.ms](kafka-syllabus.md#584-maxblockms)

[5.9 Compression](kafka-syllabus.md#59-compression)

[5.9.1 gzip, snappy, lz4, zstd](kafka-syllabus.md#591-gzip-snappy-lz4-zstd)

[5.9.2 Broker-Side Decompression/Compression](kafka-syllabus.md#592-broker-side-decompressioncompression)

[5.9.3 Trade-Offs (CPU vs Network)](kafka-syllabus.md#593-trade-offs-cpu-vs-network)

[5.10 Idempotent Producer (enable.idempotence)](kafka-syllabus.md#510-idempotent-producer-enableidempotence)

[5.10.1 Exactly-Once Semantics (Producer Side)](kafka-syllabus.md#5101-exactly-once-semantics-producer-side)

[5.10.2 Sequence Numbers & Producer IDs](kafka-syllabus.md#5102-sequence-numbers--producer-ids)

[5.11 Transactions (Producer-Only)](kafka-syllabus.md#511-transactions-producer-only)

[5.11.1 initTransactions()](kafka-syllabus.md#5111-inittransactions)

[5.11.2 beginTransaction()](kafka-syllabus.md#5112-begintransaction)

[5.11.3 commitTransaction() / abortTransaction()](kafka-syllabus.md#5113-committransaction--aborttransaction)

[5.12 Producer Monitoring](kafka-syllabus.md#512-producer-monitoring)

[5.12.1 request-rate, error-rate](kafka-syllabus.md#5121-request-rate-error-rate)

[5.12.2 record-retry-rate, record-send-total](kafka-syllabus.md#5122-record-retry-rate-record-send-total)

[5.12.3 batch-size-avg, compression-rate](kafka-syllabus.md#5123-batch-size-avg-compression-rate)

## 6. Consumers (Detailed)

[6.1 Consumer Architecture](kafka-syllabus.md#61-consumer-architecture)

[6.2 Consumer Group Concept](kafka-syllabus.md#62-consumer-group-concept)

[6.3 Group Coordinator & Group Leader](kafka-syllabus.md#63-group-coordinator--group-leader)

[6.4 Offset](kafka-syllabus.md#64-offset)

[6.4.1 Current Offset](kafka-syllabus.md#641-current-offset)

[6.4.2 Committed Offset](kafka-syllabus.md#642-committed-offset)

[6.4.3 Earliest / Latest / Specific Offset](kafka-syllabus.md#643-earliest--latest--specific-offset)

[6.4.4 Offset Storage (__consumer_offsets)](kafka-syllabus.md#644-offset-storage-__consumer_offsets)

[6.5 Offset Commit Strategies](kafka-syllabus.md#65-offset-commit-strategies)

[6.5.1 Auto Commit (enable.auto.commit, auto.commit.interval.ms)](kafka-syllabus.md#651-auto-commit-enableautocommit-autocommitintervalms)

[6.5.2 Manual Sync Commit](kafka-syllabus.md#652-manual-sync-commit)

[6.5.3 Manual Async Commit](kafka-syllabus.md#653-manual-async-commit)

[6.5.4 Commit with Callbacks](kafka-syllabus.md#654-commit-with-callbacks)

[6.6 Deserializers](kafka-syllabus.md#66-deserializers)

[6.7 Poll Loop](kafka-syllabus.md#67-poll-loop)

[6.7.1 poll(Duration)](kafka-syllabus.md#671-pollduration)

[6.7.2 max.poll.records](kafka-syllabus.md#672-maxpollrecords)

[6.7.3 max.poll.interval.ms](kafka-syllabus.md#673-maxpollintervalms)

[6.7.4 fetch.min.bytes](kafka-syllabus.md#674-fetchminbytes)

[6.7.5 fetch.max.wait.ms](kafka-syllabus.md#675-fetchmaxwaitms)

[6.8 Consumer Rebalancing](kafka-syllabus.md#68-consumer-rebalancing)

[6.8.1 Eager Rebalancing (Stop-the-World)](kafka-syllabus.md#681-eager-rebalancing-stop-the-world)

[6.8.2 Cooperative Rebalancing (Incremental)](kafka-syllabus.md#682-cooperative-rebalancing-incremental)

[6.8.3 Partition Rebalance Listeners](kafka-syllabus.md#683-partition-rebalance-listeners)

[6.8.4 Static Group Membership (group.instance.id)](kafka-syllabus.md#684-static-group-membership-groupinstanceid)

[6.9 Consumer Liveness](kafka-syllabus.md#69-consumer-liveness)

[6.9.1 session.timeout.ms](kafka-syllabus.md#691-sessiontimeoutms)

[6.9.2 heartbeat.interval.ms](kafka-syllabus.md#692-heartbeatintervalms)

[6.10 Consumer Position Control](kafka-syllabus.md#610-consumer-position-control)

[6.10.1 seek()](kafka-syllabus.md#6101-seek)

[6.10.2 seekToBeginning() / seekToEnd()](kafka-syllabus.md#6102-seektobeginning--seektomd)

[6.10.3 position()](kafka-syllabus.md#6103-position)

[6.10.4 pause() / resume()](kafka-syllabus.md#6104-pause--resume)

[6.11 Consumer Groups vs No-Group (Simple Consumer)](kafka-syllabus.md#611-consumer-groups-vs-no-group-simple-consumer)

[6.12 Consumer Lag](kafka-syllabus.md#612-consumer-lag)

[6.12.1 Definition & Causes](kafka-syllabus.md#6121-definition--causes)

[6.12.2 Tools: kafka-consumer-groups, Burrow, LagExporter](kafka-syllabus.md#6122-tools-kafka-consumer-groups-burrow-lagexporter)

[6.12.3 Reducing Lag](kafka-syllabus.md#6123-reducing-lag)

[6.13 Multi-Threaded Consumers](kafka-syllabus.md#613-multi-threaded-consumers)

[6.13.1 One Consumer Per Thread](kafka-syllabus.md#6131-one-consumer-per-thread)

[6.13.2 Single Consumer + Multiple Worker Threads](kafka-syllabus.md#6132-single-consumer--multiple-worker-threads)

## 7. Delivery Semantics & Guarantees

[7.1 At-Most-Once](kafka-syllabus.md#71-at-most-once)

[7.2 At-Least-Once](kafka-syllabus.md#72-at-least-once)

[7.3 Exactly-Once](kafka-syllabus.md#73-exactly-once)

[7.3.1 Idempotent Producer (enable.idempotence)](kafka-syllabus.md#731-idempotent-producer-enableidempotence)

[7.3.2 Transactions (Producer + Consumer)](kafka-syllabus.md#732-transactions-producer--consumer)

[7.3.3 read_committed Isolation Level](kafka-syllabus.md#733-read_committed-isolation-level)

[7.3.4 Exactly-Once for Kafka Streams / Kafka Connect](kafka-syllabus.md#734-exactly-once-for-kafka-streams--kafka-connect)

[7.4 When Exactly-Once Fails (Recovery Semantics)](kafka-syllabus.md#74-when-exactly-once-fails-recovery-semantics)

## 8. Kafka Internals (Advanced)

[8.1 Log Segments](kafka-syllabus.md#81-log-segments)

[8.1.1 segment.bytes, segment.ms](kafka-syllabus.md#811-segmentbytes-segmentms)

[8.1.2 .log, .index, .timeindex Files](kafka-syllabus.md#812-log-index-timeindex-files)

[8.1.3 Rolling & Retention](kafka-syllabus.md#813-rolling--retention)

[8.2 Retention Policies](kafka-syllabus.md#82-retention-policies)

[8.2.1 Time-Based (retention.ms)](kafka-syllabus.md#821-time-based-retentionms)

[8.2.2 Size-Based (retention.bytes)](kafka-syllabus.md#822-size-based-retentionbytes)

[8.2.3 Compacted (cleanup.policy=compact)](kafka-syllabus.md#823-compacted-cleanuppolicycompact)

[8.2.4 Delete + Compact](kafka-syllabus.md#824-delete--compact)

[8.3 Log Cleaner (for Compaction)](kafka-syllabus.md#83-log-cleaner-for-compaction)

[8.4 Leader Election](kafka-syllabus.md#84-leader-election)

[8.4.1 Preferred Leader Election](kafka-syllabus.md#841-preferred-leader-election)

[8.4.2 Unclean Election (Pros/Cons)](kafka-syllabus.md#842-unclean-election-proscons)

[8.4.3 Controller's Role](kafka-syllabus.md#843-controllers-role)

[8.5 The Controller (Broker)](kafka-syllabus.md#85-the-controller-broker)

[8.5.1 Controller Election (ZooKeeper / KRaft)](kafka-syllabus.md#851-controller-election-zookeeper--kraft)

[8.5.2 Controller Responsibilities (Partition Re-Assignment, Leader Changes)](kafka-syllabus.md#852-controller-responsibilities-partition-re-assignment-leader-changes)

[8.6 ZooKeeper Roles (Pre-2.8)](kafka-syllabus.md#86-zookeeper-roles-pre-28)

[8.6.1 Metadata Storage](kafka-syllabus.md#861-metadata-storage)

[8.6.2 Broker Registration](kafka-syllabus.md#862-broker-registration)

[8.6.3 Controller Election](kafka-syllabus.md#863-controller-election)

[8.6.4 Topic Configuration](kafka-syllabus.md#864-topic-configuration)

[8.7 KRaft (Kafka Raft) – Post-2.8 / 3.x](kafka-syllabus.md#87-kraft-kafka-raft--post-28--3x)

[8.7.1 Metadata Quorum](kafka-syllabus.md#871-metadata-quorum)

[8.7.2 Controller Nodes & Voters](kafka-syllabus.md#872-controller-nodes--voters)

[8.7.3 No ZooKeeper](kafka-syllabus.md#873-no-zookeeper)

[8.7.4 Migration from ZK Mode](kafka-syllabus.md#874-migration-from-zk-mode)

[8.8 Request Handling](kafka-syllabus.md#88-request-handling)

[8.8.1 Acceptor Threads](kafka-syllabus.md#881-acceptor-threads)

[8.8.2 Processor (Network) Threads](kafka-syllabus.md#882-processor-network-threads)

[8.8.3 Kafka Request Handler Threads](kafka-syllabus.md#883-kafka-request-handler-threads)

[8.8.4 Response Threading](kafka-syllabus.md#884-response-threading)

## 9. Administration & Operations

[9.1 Topic Management (kafka-topics.sh)](kafka-syllabus.md#91-topic-management-kafka-topicssh)

[9.1.1 create, list, describe, alter, delete](kafka-syllabus.md#911-create-list-describe-alter-delete)

[9.1.2 Increase Partitions](kafka-syllabus.md#912-increase-partitions)

[9.1.3 Update Config (min.insync.replicas, retention, etc.)](kafka-syllabus.md#913-update-config-mininsyncreplicas-retention-etc)

[9.2 Partition Reassignment (kafka-reassign-partitions.sh)](kafka-syllabus.md#92-partition-reassignment-kafka-reassign-partitionssh)

[9.3 Preferred Replica Election (kafka-leader-election.sh)](kafka-syllabus.md#93-preferred-replica-election-kafka-leader-electionsh)

[9.4 Dump Log Segments (kafka-dump-log.sh)](kafka-syllabus.md#94-dump-log-segments-kafka-dump-logsh)

[9.5 Consumer Group CLI (kafka-consumer-groups.sh)](kafka-syllabus.md#95-consumer-group-cli-kafka-consumer-groupssh)

[9.5.1 list, describe, reset offsets, delete](kafka-syllabus.md#951-list-describe-reset-offsets-delete)

[9.6 Quotas](kafka-syllabus.md#96-quotas)

[9.6.1 Produce Quota](kafka-syllabus.md#961-produce-quota)

[9.6.2 Fetch Quota](kafka-syllabus.md#962-fetch-quota)

[9.6.3 Request Quota (Controller Mutex)](kafka-syllabus.md#963-request-quota-controller-mutex)

[9.6.4 Client-ID / User Based](kafka-syllabus.md#964-client-id--user-based)

[9.7 Authentication](kafka-syllabus.md#97-authentication)

[9.7.1 SSL / mTLS](kafka-syllabus.md#971-ssl--mtls)

[9.7.2 SASL (PLAIN, SCRAM, GSSAPI (Kerberos), OAUTHBEARER)](kafka-syllabus.md#972-sasl-plain-scram-gssapi-kerberos-oauthbearer)

[9.8 Authorization](kafka-syllabus.md#98-authorization)

[9.8.1 ACLs (Simple, Wildcard)](kafka-syllabus.md#981-acls-simple-wildcard)

[9.8.2 Super Users](kafka-syllabus.md#982-super-users)

[9.8.3 Resource Types (Topic, Group, Cluster, TransactionalId)](kafka-syllabus.md#983-resource-types-topic-group-cluster-transactionalid)

[9.9 Monitoring (Metrics & JMX)](kafka-syllabus.md#99-monitoring-metrics--jmx)

[9.9.1 Under-Replicated Partitions](kafka-syllabus.md#991-under-replicated-partitions)

[9.9.2 Offline Partitions](kafka-syllabus.md#992-offline-partitions)

[9.9.3 ISR Shrink/Expand Rate](kafka-syllabus.md#993-isr-shrinkexpand-rate)

[9.9.4 Request Handler Avg Idle %](kafka-syllabus.md#994-request-handler-avg-idle-)

[9.9.5 Network Processor Avg Idle %](kafka-syllabus.md#995-network-processor-avg-idle-)

[9.9.6 Consumer Lag by Group/Topic](kafka-syllabus.md#996-consumer-lag-by-grouptopic)

[9.10 Logging & Debugging](kafka-syllabus.md#910-logging--debugging)

[9.10.1 Broker Log Levels](kafka-syllabus.md#9101-broker-log-levels)

[9.10.2 Slow Consumer Detection](kafka-syllabus.md#9102-slow-consumer-detection)

[9.10.3 Fetcher Lag](kafka-syllabus.md#9103-fetcher-lag)

## 10. Kafka Connect

[10.1 Connect Architecture (Workers, Tasks, Connectors)](kafka-syllabus.md#101-connect-architecture-workers-tasks-connectors)

[10.1.1 Workers](kafka-syllabus.md#1011-workers)

[10.1.2 Tasks](kafka-syllabus.md#1012-tasks)

[10.1.3 Connectors](kafka-syllabus.md#1013-connectors)

[10.2 Source Connectors (Read from External)](kafka-syllabus.md#102-source-connectors-read-from-external)

[10.3 Sink Connectors (Write to External)](kafka-syllabus.md#103-sink-connectors-write-to-external)

[10.4 Converters (Avro, JSON, Parquet, Protobuf)](kafka-syllabus.md#104-converters-avro-json-parquet-protobuf)

[10.4.1 Avro Converter](kafka-syllabus.md#1041-avro-converter)

[10.4.2 JSON Converter](kafka-syllabus.md#1042-json-converter)

[10.4.3 Parquet Converter](kafka-syllabus.md#1043-parquet-converter)

[10.4.4 Protobuf Converter](kafka-syllabus.md#1044-protobuf-converter)

[10.5 Transforms (Single Message Transforms)](kafka-syllabus.md#105-transforms-single-message-transforms)

[10.6 Dead Letter Queue (DLQ)](kafka-syllabus.md#106-dead-letter-queue-dlq)

[10.7 Distributed vs Standalone Mode](kafka-syllabus.md#107-distributed-vs-standalone-mode)

[10.8 Rebalancing in Connect Workers](kafka-syllabus.md#108-rebalancing-in-connect-workers)

[10.9 Offset Management in Connect](kafka-syllabus.md#109-offset-management-in-connect)

## 11. Kafka Streams

[11.1 Streams Architecture (Embedded Library)](kafka-syllabus.md#111-streams-architecture-embedded-library)

[11.2 KStream vs KTable vs GlobalKTable](kafka-syllabus.md#112-kstream-vs-ktable-vs-globalktable)

[11.3 State Stores](kafka-syllabus.md#113-state-stores)

[11.3.1 RocksDB](kafka-syllabus.md#1131-rocksdb)

[11.3.2 In-Memory](kafka-syllabus.md#1132-in-memory)

[11.3.3 Custom](kafka-syllabus.md#1133-custom)

[11.4 Stream-Table Duality](kafka-syllabus.md#114-stream-table-duality)

[11.5 Operations](kafka-syllabus.md#115-operations)

[11.5.1 Stateless (map, filter, flatMap, groupBy, branch, split, merge)](kafka-syllabus.md#1151-stateless-map-filter-flatmap-groupby-branch-split-merge)

[11.5.2 Stateful (aggregate, reduce, count, windowed joins, session windows)](kafka-syllabus.md#1152-stateful-aggregate-reduce-count-windowed-joins-session-windows)

[11.6 Windowing](kafka-syllabus.md#116-windowing)

[11.6.1 Tumbling Windows](kafka-syllabus.md#1161-tumbling-windows)

[11.6.2 Hopping Windows](kafka-syllabus.md#1162-hopping-windows)

[11.6.3 Session Windows](kafka-syllabus.md#1163-session-windows)

[11.6.4 Sliding Windows (v2.8+)](kafka-syllabus.md#1164-sliding-windows-v28)

[11.6.5 Grace Period](kafka-syllabus.md#1165-grace-period)

[11.7 Exactly-Once in Streams](kafka-syllabus.md#117-exactly-once-in-streams)

[11.8 Interactive Queries (Queryable State Stores)](kafka-syllabus.md#118-interactive-queries-queryable-state-stores)

[11.9 Streams Rebalancing & Scaling](kafka-syllabus.md#119-streams-rebalancing--scaling)

## 12. KSQLDB / KSQL

[12.1 KSQL Architecture](kafka-syllabus.md#121-ksql-architecture)

[12.2 Streams vs Tables in SQL](kafka-syllabus.md#122-streams-vs-tables-in-sql)

[12.3 Pull vs Push Queries](kafka-syllabus.md#123-pull-vs-push-queries)

[12.4 Materialized Views](kafka-syllabus.md#124-materialized-views)

[12.5 UDFs / UDAFs](kafka-syllabus.md#125-udfs--udafs)

[12.5.1 UDFs (User Defined Functions)](kafka-syllabus.md#1251-udfs-user-defined-functions)

[12.5.2 UDAFs (User Defined Aggregate Functions)](kafka-syllabus.md#1252-udafs-user-defined-aggregate-functions)

[12.5.3 UDF vs UDAF Comparison](kafka-syllabus.md#1253-udf-vs-udaf-comparison)

[12.6 Exactly-Once in KSQL](kafka-syllabus.md#126-exactly-once-in-ksql)

## 13. Performance Tuning

[13.1 Producer Tuning](kafka-syllabus.md#131-producer-tuning)

[13.1.1 Batching Optimization](kafka-syllabus.md#1311-batching-optimization)

[13.1.2 Reliability vs Performance](kafka-syllabus.md#1312-reliability-vs-performance)

[13.1.3 Retry Tuning](kafka-syllabus.md#1313-retry-tuning)

[13.1.4 Memory Tuning](kafka-syllabus.md#1314-memory-tuning)

[13.1.5 Compression Tuning](kafka-syllabus.md#1315-compression-tuning)

[13.2 Consumer Tuning](kafka-syllabus.md#132-consumer-tuning)

[13.2.1 Fetch Tuning](kafka-syllabus.md#1321-fetch-tuning)

[13.2.2 Poll Tuning](kafka-syllabus.md#1322-poll-tuning)

[13.2.3 Consumer Group Scaling](kafka-syllabus.md#1323-consumer-group-scaling)

[13.2.4 Offset Commit Optimization](kafka-syllabus.md#1324-offset-commit-optimization)

[13.3 Broker Tuning](kafka-syllabus.md#133-broker-tuning)

[13.3.1 OS Page Cache](kafka-syllabus.md#1331-os-page-cache)

[13.3.2 Filesystem (XFS)](kafka-syllabus.md#1332-filesystem-xfs)

[13.3.3 Heap Size (6-8GB Max)](kafka-syllabus.md#1333-heap-size-6-8gb-max)

[13.3.4 Log Flush Policies (Don't)](kafka-syllabus.md#1334-log-flush-policies-dont)

[13.4 Network & Disk Bandwidth](kafka-syllabus.md#134-network--disk-bandwidth)

[13.5 Partition Count vs Throughput](kafka-syllabus.md#135-partition-count-vs-throughput)

[13.6 Compression Trade-Offs](kafka-syllabus.md#136-compression-trade-offs)

[13.6.1 GZIP](kafka-syllabus.md#1361-gzip)

[13.6.2 Snappy](kafka-syllabus.md#1362-snappy)

[13.6.3 LZ4](kafka-syllabus.md#1363-lz4)

[13.6.4 ZSTD](kafka-syllabus.md#1364-zstd)

[13.6.5 Compression Algorithm Comparison](kafka-syllabus.md#1365-compression-algorithm-comparison)

[13.7 Replication Throttling for Migration](kafka-syllabus.md#137-replication-throttling-for-migration)

## 14. Comparison & Conceptual

[14.1 Kafka vs RabbitMQ](kafka-syllabus.md#141-kafka-vs-rabbitmq)

[14.2 Kafka vs Pulsar](kafka-syllabus.md#142-kafka-vs-pulsar)

[14.3 Kafka vs Kinesis](kafka-syllabus.md#143-kafka-vs-kinesis)

[14.4 Kafka vs Traditional Message Queues (ActiveMQ, IBM MQ)](kafka-syllabus.md#144-kafka-vs-traditional-message-queues-activemq-ibm-mq)

[14.5 Pull vs Push Model (Why Pull Wins)](kafka-syllabus.md#145-pull-vs-push-model-why-pull-wins)

[14.6 Ordering Guarantees (Partition Level Only)](kafka-syllabus.md#146-ordering-guarantees-partition-level-only)

[14.7 Persistence vs Deletion](kafka-syllabus.md#147-persistence-vs-deletion)

[14.8 Distributed Commit Log](kafka-syllabus.md#148-distributed-commit-log)

## 15. Migration & Upgrade

[15.1 Rolling Upgrade (Broker by Broker)](kafka-syllabus.md#151-rolling-upgrade-broker-by-broker)

[15.2 Upgrade from ZooKeeper to KRaft](kafka-syllabus.md#152-upgrade-from-zookeeper-to-kraft)

[15.3 Protocol Version Compatibility](kafka-syllabus.md#153-protocol-version-compatibility)

[15.4 Inter-Broker Protocol (IBP)](kafka-syllabus.md#154-inter-broker-protocol-ibp)

[15.5 Log Format Version](kafka-syllabus.md#155-log-format-version)

[15.6 Downgrade Safety](kafka-syllabus.md#156-downgrade-safety)

## 16. Common Interview Scenarios (Problem-Solving)

[16.1 Duplicate Messages (Causes & Prevention)](kafka-syllabus.md#161-duplicate-messages-causes--prevention)

[16.2 Missing Messages (Acks, Retries, Min.insync)](kafka-syllabus.md#162-missing-messages-acks-retries-mininsync)

[16.3 Consumer Group Stuck (max.poll.interval.ms Too Small)](kafka-syllabus.md#163-consumer-group-stuck-maxpollintervalms-too-small)

[16.4 Rebalancing Too Often (session.timeout.ms, heartbeat.interval.ms)](kafka-syllabus.md#164-rebalancing-too-often-sessiontimeoutms-heartbeatintervalms)

[16.5 High Latency (linger.ms, batch.size, fetch.min.bytes)](kafka-syllabus.md#165-high-latency-lingerms-batchsize-fetchminbytes)

[16.6 Disk Full (Retention, Low Capacity)](kafka-syllabus.md#166-disk-full-retention-low-capacity)

[16.7 Leader Not Replicating (ISR Shrinks, Network Issues)](kafka-syllabus.md#167-leader-not-replicating-isr-shrinks-network-issues)

[16.8 Exactly-Once Across Multiple Topics/Partitions](kafka-syllabus.md#168-exactly-once-across-multiple-topicspartitions)

[16.9 Resetting Offsets for Reprocessing](kafka-syllabus.md#169-resetting-offsets-for-reprocessing)

[16.10 Designing a Chat System / Event-Driven Pipeline](kafka-syllabus.md#1610-designing-a-chat-system--event-driven-pipeline)

---

# Kafka Hands-On Labs

## Topics Labs

[Lab 1: Create Production Topics for an E-Commerce Platform](kafka-labs.md#lab-1-create-production-topics-for-an-e-commerce-platform)

[Lab 2: Implement Enterprise Topic Naming Standards](kafka-labs.md#lab-2-implement-enterprise-topic-naming-standards)

[Lab 3: Verify Publish-Subscribe Behavior](kafka-labs.md#lab-3-verify-publish-subscribe-behavior)

[Lab 4: Verify Queue-Like Processing Using Consumer Groups](kafka-labs.md#lab-4-verify-queue-like-processing-using-consumer-groups)

[Lab 5: Enable Topic Auto-Creation and Validate Behavior](kafka-labs.md#lab-5-enable-topic-auto-creation-and-validate-behavior)

[Lab 6: Disable Topic Auto-Creation](kafka-labs.md#lab-6-disable-topic-auto-creation)

[Lab 7: Audit Automatically Created Topics](kafka-labs.md#lab-7-audit-automatically-created-topics)

[Lab 8: Inspect Kafka Internal Topics](kafka-labs.md#lab-8-inspect-kafka-internal-topics)

[Lab 9: Observe Offset Storage](kafka-labs.md#lab-9-observe-offset-storage)

[Lab 10: Topic Configuration Review](kafka-labs.md#lab-10-topic-configuration-review)

[Lab 11: Topic Deletion Validation](kafka-labs.md#lab-11-topic-deletion-validation)

[Lab 12: Recreate Deleted Topic](kafka-labs.md#lab-12-recreate-deleted-topic)

[Lab 13: Production Topic Inventory Exercise](kafka-labs.md#lab-13-production-topic-inventory-exercise)

[Lab 14: Topic Governance Review](kafka-labs.md#lab-14-topic-governance-review)

[Lab 15: Production Readiness Validation](kafka-labs.md#lab-15-production-readiness-validation)

## Partitions Labs

[Lab 1: Create a Multi-Partition Topic](kafka-labs.md#lab-1-create-a-multi-partition-topic)

[Lab 2: Explore Partition Storage](kafka-labs.md#lab-2-explore-partition-storage)

[Lab 3: Key-Based Partition Routing](kafka-labs.md#lab-3-key-based-partition-routing)

[Lab 4: Keyless Message Distribution](kafka-labs.md#lab-4-keyless-message-distribution)

[Lab 5: Partition Key Design Exercise](kafka-labs.md#lab-5-partition-key-design-exercise)

[Lab 6: Round-Robin Partition Assignment](kafka-labs.md#lab-6-round-robin-partition-assignment)

[Lab 7: Sticky Partition Assignment](kafka-labs.md#lab-7-sticky-partition-assignment)

[Lab 8: Uniform Sticky Partition Assignment](kafka-labs.md#lab-8-uniform-sticky-partition-assignment)

[Lab 9: Partition Count Planning Exercise](kafka-labs.md#lab-9-partition-count-planning-exercise)

[Lab 10: Consumer Parallelism Using Partitions](kafka-labs.md#lab-10-consumer-parallelism-using-partitions)

[Lab 11: Consumer Rebalancing - New Consumer Join](kafka-labs.md#lab-11-consumer-rebalancing---new-consumer-join)

[Lab 12: Consumer Rebalancing - Consumer Failure](kafka-labs.md#lab-12-consumer-rebalancing---consumer-failure)

[Lab 13: Partition Leadership Inspection](kafka-labs.md#lab-13-partition-leadership-inspection)

[Lab 14: Leader Failover Simulation](kafka-labs.md#lab-14-leader-failover-simulation)

[Lab 15: Preferred Replica Election](kafka-labs.md#lab-15-preferred-replica-election)

[Lab 16: Offset Exploration](kafka-labs.md#lab-16-offset-exploration)

[Lab 17: Consumer Offset Tracking](kafka-labs.md#lab-17-consumer-offset-tracking)

[Lab 18: Partition Scaling for Throughput](kafka-labs.md#lab-18-partition-scaling-for-throughput)

[Lab 19: Increase Partition Count](kafka-labs.md#lab-19-increase-partition-count)

[Lab 20: Impact of Partition Increase on Consumer Groups](kafka-labs.md#lab-20-impact-of-partition-increase-on-consumer-groups)

[Lab 21: Partition Migration Strategy](kafka-labs.md#lab-21-partition-migration-strategy)

[Lab 22: Topic Recreation for Partition Reduction](kafka-labs.md#lab-22-topic-recreation-for-partition-reduction)

## Brokers Labs

[Lab 1: Deploy a Single Kafka Broker](kafka-labs.md#lab-1-deploy-a-single-kafka-broker)

[Lab 2: Build a Multi-Broker Kafka Cluster](kafka-labs.md#lab-2-build-a-multi-broker-kafka-cluster)

[Lab 3: Broker ID Configuration and Validation](kafka-labs.md#lab-3-broker-id-configuration-and-validation)

[Lab 4: Bootstrap Server Discovery](kafka-labs.md#lab-4-bootstrap-server-discovery)

[Lab 5: Broker Configuration Audit](kafka-labs.md#lab-5-broker-configuration-audit)

[Lab 6: Modify Broker Configuration and Restart](kafka-labs.md#lab-6-modify-broker-configuration-and-restart)

[Lab 7: Configure Rack Awareness](kafka-labs.md#lab-7-configure-rack-awareness)

[Lab 8: Cross-Rack Replication Validation](kafka-labs.md#lab-8-cross-rack-replication-validation)

[Lab 9: Broker Failure Detection](kafka-labs.md#lab-9-broker-failure-detection)

[Lab 10: Producer and Consumer Behavior During Broker Failure](kafka-labs.md#lab-10-producer-and-consumer-behavior-during-broker-failure)

[Lab 11: Controlled Broker Shutdown](kafka-labs.md#lab-11-controlled-broker-shutdown)

[Lab 12: Uncontrolled Broker Shutdown](kafka-labs.md#lab-12-uncontrolled-broker-shutdown)

[Lab 13: Broker Metadata Inspection (KRaft)](kafka-labs.md#lab-13-broker-metadata-inspection-kraft)

[Lab 14: ZooKeeper-Based Metadata Inspection](kafka-labs.md#lab-14-zookeeper-based-metadata-inspection)

[Lab 15: KRaft vs ZooKeeper Comparison Exercise](kafka-labs.md#lab-15-kraft-vs-zookeeper-comparison-exercise)

[Lab 16: Broker Recovery After Restart](kafka-labs.md#lab-16-broker-recovery-after-restart)

[Lab 17: Broker Recovery After Crash](kafka-labs.md#lab-17-broker-recovery-after-crash)

[Lab 18: Replica Synchronization After Recovery](kafka-labs.md#lab-18-replica-synchronization-after-recovery)

[Lab 19: Broker Health Monitoring](kafka-labs.md#lab-19-broker-health-monitoring)

[Lab 20: Production Broker Readiness Validation](kafka-labs.md#lab-20-production-broker-readiness-validation)

## Replication Labs

[Lab 1: Create Topics with Different Replication Factors](kafka-labs.md#lab-1-create-topics-with-different-replication-factors)

[Lab 2: Replication Factor Failure Tolerance Validation](kafka-labs.md#lab-2-replication-factor-failure-tolerance-validation)

[Lab 3: Inspect Leader and Follower Replicas](kafka-labs.md#lab-3-inspect-leader-and-follower-replicas)

[Lab 4: Validate Producer Writes Through Leaders](kafka-labs.md#lab-4-validate-producer-writes-through-leaders)

[Lab 5: Validate Consumer Reads from Leaders](kafka-labs.md#lab-5-validate-consumer-reads-from-leaders)

[Lab 6: Leader Failure and Automatic Failover](kafka-labs.md#lab-6-leader-failure-and-automatic-failover)

[Lab 7: ISR Inspection and Monitoring](kafka-labs.md#lab-7-isr-inspection-and-monitoring)

[Lab 8: Trigger ISR Shrink Event](kafka-labs.md#lab-8-trigger-isr-shrink-event)

[Lab 9: ISR Recovery Validation](kafka-labs.md#lab-9-isr-recovery-validation)

[Lab 10: Simulate Out-of-Sync Replica (OSR)](kafka-labs.md#lab-10-simulate-out-of-sync-replica-osr)

[Lab 11: OSR Recovery and Synchronization](kafka-labs.md#lab-11-osr-recovery-and-synchronization)

[Lab 12: Unclean Leader Election Demonstration](kafka-labs.md#lab-12-unclean-leader-election-demonstration)

[Lab 13: Clean vs Unclean Leader Election Comparison](kafka-labs.md#lab-13-clean-vs-unclean-leader-election-comparison)

[Lab 14: Configure Min In-Sync Replicas](kafka-labs.md#lab-14-configure-min-in-sync-replicas)

[Lab 15: Write Failure Due to Insufficient ISR](kafka-labs.md#lab-15-write-failure-due-to-insufficient-isr)

[Lab 16: Compare Different min.insync.replicas Values](kafka-labs.md#lab-16-compare-different-mininsyncreplicas-values)

[Lab 17: Replication Throttling During Recovery](kafka-labs.md#lab-17-replication-throttling-during-recovery)

[Lab 18: Replication Throttling During Partition Reassignment](kafka-labs.md#lab-18-replication-throttling-during-partition-reassignment)

[Lab 19: Monitor Replication Metrics](kafka-labs.md#lab-19-monitor-replication-metrics)

[Lab 20: Configure Fetch from Follower](kafka-labs.md#lab-20-configure-fetch-from-follower)

[Lab 21: Cross-Rack Consumer Optimization](kafka-labs.md#lab-21-cross-rack-consumer-optimization)

[Lab 22: Rack-Aware Replication Configuration](kafka-labs.md#lab-22-rack-aware-replication-configuration)

[Lab 23: Simulate Rack Failure](kafka-labs.md#lab-23-simulate-rack-failure)

[Lab 24: Availability Zone Failure Simulation](kafka-labs.md#lab-24-availability-zone-failure-simulation)

[Lab 25: Production Replication Readiness Assessment](kafka-labs.md#lab-25-production-replication-readiness-assessment)

## Producers Labs

[Lab 1: Build a Basic Kafka Producer](kafka-labs.md#lab-1-build-a-basic-kafka-producer)

[Lab 2: Analyze Producer Architecture](kafka-labs.md#lab-2-analyze-producer-architecture)

[Lab 3: ProducerRecord Field Validation](kafka-labs.md#lab-3-producerrecord-field-validation)

[Lab 4: Message Headers Implementation](kafka-labs.md#lab-4-message-headers-implementation)

[Lab 5: String Serializer Implementation](kafka-labs.md#lab-5-string-serializer-implementation)

[Lab 6: JSON Serializer Implementation](kafka-labs.md#lab-6-json-serializer-implementation)

[Lab 7: Avro Serialization with Schema Registry](kafka-labs.md#lab-7-avro-serialization-with-schema-registry)

[Lab 8: Protobuf Serialization](kafka-labs.md#lab-8-protobuf-serialization)

[Lab 9: Custom Serializer Development](kafka-labs.md#lab-9-custom-serializer-development)

[Lab 10: Default Partitioner Validation](kafka-labs.md#lab-10-default-partitioner-validation)

[Lab 11: Key-Based Partitioning](kafka-labs.md#lab-11-key-based-partitioning)

[Lab 12: Sticky Partitioner Demonstration](kafka-labs.md#lab-12-sticky-partitioner-demonstration)

[Lab 13: Custom Partitioner Development](kafka-labs.md#lab-13-custom-partitioner-development)

[Lab 14: Producer Interceptor Implementation](kafka-labs.md#lab-14-producer-interceptor-implementation)

[Lab 15: Audit and Monitoring Interceptors](kafka-labs.md#lab-15-audit-and-monitoring-interceptors)

[Lab 16: acks=0 Performance Test](kafka-labs.md#lab-16-acks0-performance-test)

[Lab 17: acks=1 Reliability Test](kafka-labs.md#lab-17-acks1-reliability-test)

[Lab 18: acks=all Durability Test](kafka-labs.md#lab-18-acksall-durability-test)

[Lab 19: Compare Acknowledgment Modes](kafka-labs.md#lab-19-compare-acknowledgment-modes)

[Lab 20: Retry Mechanism Validation](kafka-labs.md#lab-20-retry-mechanism-validation)

[Lab 21: retry.backoff.ms Experiment](kafka-labs.md#lab-21-retrybackoffms-experiment)

[Lab 22: deliver.timeout.ms Validation](kafka-labs.md#lab-22-delivertimeoutms-validation)

[Lab 23: request.timeout.ms Validation](kafka-labs.md#lab-23-requesttimeoutms-validation)

[Lab 24: Producer Batching Performance Test](kafka-labs.md#lab-24-producer-batching-performance-test)

[Lab 25: batch.size Optimization](kafka-labs.md#lab-25-batchsize-optimization)

[Lab 26: linger.ms Optimization](kafka-labs.md#lab-26-lingerms-optimization)

[Lab 27: buffer.memory Stress Test](kafka-labs.md#lab-27-buffermemory-stress-test)

[Lab 28: max.block.ms Validation](kafka-labs.md#lab-28-maxblockms-validation)

[Lab 29: GZIP Compression Benchmark](kafka-labs.md#lab-29-gzip-compression-benchmark)

[Lab 30: Snappy Compression Benchmark](kafka-labs.md#lab-30-snappy-compression-benchmark)

[Lab 31: LZ4 Compression Benchmark](kafka-labs.md#lab-31-lz4-compression-benchmark)

[Lab 32: ZSTD Compression Benchmark](kafka-labs.md#lab-32-zstd-compression-benchmark)

[Lab 33: Compression Comparison Report](kafka-labs.md#lab-33-compression-comparison-report)

[Lab 34: Enable Idempotent Producer](kafka-labs.md#lab-34-enable-idempotent-producer)

[Lab 35: Idempotence Failure Recovery Test](kafka-labs.md#lab-35-idempotence-failure-recovery-test)

[Lab 36: Producer ID and Sequence Number Analysis](kafka-labs.md#lab-36-producer-id-and-sequence-number-analysis)

[Lab 37: Transactional Producer Setup](kafka-labs.md#lab-37-transactional-producer-setup)

[Lab 38: Transaction Commit Workflow](kafka-labs.md#lab-38-transaction-commit-workflow)

[Lab 39: Transaction Abort Workflow](kafka-labs.md#lab-39-transaction-abort-workflow)

[Lab 40: Multi-Partition Transaction Test](kafka-labs.md#lab-40-multi-partition-transaction-test)

[Lab 41: Multi-Topic Transaction Test](kafka-labs.md#lab-41-multi-topic-transaction-test)

[Lab 42: Transaction Failure Recovery](kafka-labs.md#lab-42-transaction-failure-recovery)

[Lab 43: Producer Metrics Monitoring](kafka-labs.md#lab-43-producer-metrics-monitoring)

[Lab 44: Request Rate and Error Rate Analysis](kafka-labs.md#lab-44-request-rate-and-error-rate-analysis)

[Lab 45: Retry Metrics Analysis](kafka-labs.md#lab-45-retry-metrics-analysis)

[Lab 46: Batch Size and Compression Monitoring](kafka-labs.md#lab-46-batch-size-and-compression-monitoring)

[Lab 47: End-to-End Producer Performance Tuning](kafka-labs.md#lab-47-end-to-end-producer-performance-tuning)

[Lab 48: Production Producer Readiness Assessment](kafka-labs.md#lab-48-production-producer-readiness-assessment)

## Consumers Labs

[Lab 1: Create and Run a Basic Consumer](kafka-labs.md#lab-1-create-and-run-a-basic-consumer)

[Lab 2: Consumer Group Load Balancing](kafka-labs.md#lab-2-consumer-group-load-balancing)

[Lab 3: Multiple Consumer Groups](kafka-labs.md#lab-3-multiple-consumer-groups)

[Lab 4: Observe Group Coordinator Activity](kafka-labs.md#lab-4-observe-group-coordinator-activity)

[Lab 5: Offset Tracking Validation](kafka-labs.md#lab-5-offset-tracking-validation)

[Lab 6: Inspect __consumer_offsets Topic](kafka-labs.md#lab-6-inspect-__consumer_offsets-topic)

[Lab 7: Auto Commit Testing](kafka-labs.md#lab-7-auto-commit-testing)

[Lab 8: Manual Sync Commit Testing](kafka-labs.md#lab-8-manual-sync-commit-testing)

[Lab 9: Manual Async Commit Testing](kafka-labs.md#lab-9-manual-async-commit-testing)

[Lab 10: Commit Callback Validation](kafka-labs.md#lab-10-commit-callback-validation)

[Lab 11: Consumer Restart Recovery](kafka-labs.md#lab-11-consumer-restart-recovery)

[Lab 12: Earliest Offset Consumption](kafka-labs.md#lab-12-earliest-offset-consumption)

[Lab 13: Latest Offset Consumption](kafka-labs.md#lab-13-latest-offset-consumption)

[Lab 14: Seek to Specific Offset](kafka-labs.md#lab-14-seek-to-specific-offset)

[Lab 15: Seek to Beginning and End](kafka-labs.md#lab-15-seek-to-beginning-and-end)

[Lab 16: Position Monitoring](kafka-labs.md#lab-16-position-monitoring)

[Lab 17: Pause and Resume Consumption](kafka-labs.md#lab-17-pause-and-resume-consumption)

[Lab 18: Poll Loop Tuning](kafka-labs.md#lab-18-poll-loop-tuning)

[Lab 19: max.poll.interval.ms Failure Scenario](kafka-labs.md#lab-19-maxpollintervalms-failure-scenario)

[Lab 20: fetch.min.bytes Optimization](kafka-labs.md#lab-20-fetchminbytes-optimization)

[Lab 21: fetch.max.wait.ms Optimization](kafka-labs.md#lab-21-fetchmaxwaitms-optimization)

[Lab 22: Eager Rebalancing Demonstration](kafka-labs.md#lab-22-eager-rebalancing-demonstration)

[Lab 23: Cooperative Rebalancing Demonstration](kafka-labs.md#lab-23-cooperative-rebalancing-demonstration)

[Lab 24: Rebalance Listener Testing](kafka-labs.md#lab-24-rebalance-listener-testing)

[Lab 25: Static Membership Testing](kafka-labs.md#lab-25-static-membership-testing)

[Lab 26: Session Timeout Failure Simulation](kafka-labs.md#lab-26-session-timeout-failure-simulation)

[Lab 27: Heartbeat Configuration Testing](kafka-labs.md#lab-27-heartbeat-configuration-testing)

[Lab 28: Consumer Lag Generation](kafka-labs.md#lab-28-consumer-lag-generation)

[Lab 29: Consumer Lag Reduction](kafka-labs.md#lab-29-consumer-lag-reduction)

[Lab 30: Consumer Group Monitoring](kafka-labs.md#lab-30-consumer-group-monitoring)

[Lab 31: Multi-Threaded Consumer Design](kafka-labs.md#lab-31-multi-threaded-consumer-design)

[Lab 32: Worker Thread Processing Model](kafka-labs.md#lab-32-worker-thread-processing-model)

[Lab 33: Consumer Failure Recovery](kafka-labs.md#lab-33-consumer-failure-recovery)

[Lab 34: Production Consumer Validation](kafka-labs.md#lab-34-production-consumer-validation)

## Kafka Administration & Operations - Hands-On Labs

[Lab 1: Topic Lifecycle Management](kafka-labs.md#lab-1-topic-lifecycle-management)

[Lab 2: Production Topic Administration Audit](kafka-labs.md#lab-2-production-topic-administration-audit)

[Lab 3: Increase Topic Partitions](kafka-labs.md#lab-3-increase-topic-partitions)

[Lab 4: Partition Expansion Impact Analysis](kafka-labs.md#lab-4-partition-expansion-impact-analysis)

[Lab 5: Update Topic Retention Policies](kafka-labs.md#lab-5-update-topic-retention-policies)

[Lab 6: Configure min.insync.replicas](kafka-labs.md#lab-6-configure-mininsyncreplicas)

[Lab 7: Topic Configuration Governance](kafka-labs.md#lab-7-topic-configuration-governance)

[Lab 8: Partition Reassignment Between Brokers](kafka-labs.md#lab-8-partition-reassignment-between-brokers)

[Lab 9: Broker Expansion and Rebalancing](kafka-labs.md#lab-9-broker-expansion-and-rebalancing)

[Lab 10: Reassignment Throttling Validation](kafka-labs.md#lab-10-reassignment-throttling-validation)

[Lab 11: Preferred Leader Election](kafka-labs.md#lab-11-preferred-leader-election)

[Lab 12: Leader Imbalance Recovery](kafka-labs.md#lab-12-leader-imbalance-recovery)

[Lab 13: Dump Kafka Log Segments](kafka-labs.md#lab-13-dump-kafka-log-segments)

[Lab 14: Analyze Index and Time Index Files](kafka-labs.md#lab-14-analyze-index-and-time-index-files)

[Lab 15: Log Corruption Investigation](kafka-labs.md#lab-15-log-corruption-investigation)

[Lab 16: Consumer Group Administration](kafka-labs.md#lab-16-consumer-group-administration)

[Lab 17: Consumer Lag Analysis](kafka-labs.md#lab-17-consumer-lag-analysis)

[Lab 18: Offset Reset Operations](kafka-labs.md#lab-18-offset-reset-operations)

[Lab 19: Consumer Group Cleanup](kafka-labs.md#lab-19-consumer-group-cleanup)

[Lab 20: Producer Quota Enforcement](kafka-labs.md#lab-20-producer-quota-enforcement)

[Lab 21: Consumer Fetch Quota Enforcement](kafka-labs.md#lab-21-consumer-fetch-quota-enforcement)

[Lab 22: Client-Based Quota Management](kafka-labs.md#lab-22-client-based-quota-management)

[Lab 23: SSL Encryption Setup](kafka-labs.md#lab-23-ssl-encryption-setup)

[Lab 24: Mutual TLS (mTLS) Authentication](kafka-labs.md#lab-24-mutual-tls-mtls-authentication)

[Lab 25: SASL Authentication Setup](kafka-labs.md#lab-25-sasl-authentication-setup)

[Lab 26: ACL-Based Authorization](kafka-labs.md#lab-26-acl-based-authorization)

[Lab 27: Wildcard ACL Management](kafka-labs.md#lab-27-wildcard-acl-management)

[Lab 28: Super User Administration](kafka-labs.md#lab-28-super-user-administration)

[Lab 29: Transactional ID Authorization](kafka-labs.md#lab-29-transactional-id-authorization)

[Lab 30: JMX Metrics Monitoring](kafka-labs.md#lab-30-jmx-metrics-monitoring)

[Lab 31: Prometheus and Grafana Integration](kafka-labs.md#lab-31-prometheus-and-grafana-integration)

[Lab 32: Under-Replicated Partition Investigation](kafka-labs.md#lab-32-under-replicated-partition-investigation)

[Lab 33: Offline Partition Recovery](kafka-labs.md#lab-33-offline-partition-recovery)

[Lab 34: ISR Shrink and Expansion Monitoring](kafka-labs.md#lab-34-isr-shrink-and-expansion-monitoring)

[Lab 35: Request Handler Capacity Analysis](kafka-labs.md#lab-35-request-handler-capacity-analysis)

[Lab 36: Network Processor Capacity Analysis](kafka-labs.md#lab-36-network-processor-capacity-analysis)

[Lab 37: Consumer Lag Dashboard Creation](kafka-labs.md#lab-37-consumer-lag-dashboard-creation)

[Lab 38: Dynamic Broker Log Level Changes](kafka-labs.md#lab-38-dynamic-broker-log-level-changes)

[Lab 39: Slow Consumer Investigation](kafka-labs.md#lab-39-slow-consumer-investigation)

[Lab 40: Fetcher Lag Troubleshooting](kafka-labs.md#lab-40-fetcher-lag-troubleshooting)

[Lab 41: Production Operations Runbook Exercise](kafka-labs.md#lab-41-production-operations-runbook-exercise)

## Performance Tuning Labs

[LAB 1: Producer Batching Performance Tuning](kafka-labs.md#lab-1-producer-batching-performance-tuning)

[LAB 2: Producer Reliability vs Performance](kafka-labs.md#lab-2-producer-reliability-vs-performance)

[LAB 3: Producer Retry Configuration Tuning](kafka-labs.md#lab-3-producer-retry-configuration-tuning)

[LAB 4: Producer Memory Buffer Tuning](kafka-labs.md#lab-4-producer-memory-buffer-tuning)

[LAB 5: Producer Compression Benchmark](kafka-labs.md#lab-5-producer-compression-benchmark)

[LAB 6: Consumer Fetch Tuning](kafka-labs.md#lab-6-consumer-fetch-tuning)

[LAB 7: Consumer Poll Loop Optimization](kafka-labs.md#lab-7-consumer-poll-loop-optimization)

[LAB 8: Consumer Group Scaling](kafka-labs.md#lab-8-consumer-group-scaling)

[LAB 9: Offset Commit Performance Analysis](kafka-labs.md#lab-9-offset-commit-performance-analysis)

[LAB 10: Consumer Lag Investigation and Tuning](kafka-labs.md#lab-10-consumer-lag-investigation-and-tuning)

[LAB 11: Broker Page Cache Analysis](kafka-labs.md#lab-11-broker-page-cache-analysis)

[LAB 12: Broker Heap Size Tuning](kafka-labs.md#lab-12-broker-heap-size-tuning)

[LAB 13: Log Flush Performance Analysis](kafka-labs.md#lab-13-log-flush-performance-analysis)

[LAB 14: Disk and Network Bandwidth Monitoring](kafka-labs.md#lab-14-disk-and-network-bandwidth-monitoring)

[LAB 15: Partition Count vs Throughput Benchmark](kafka-labs.md#lab-15-partition-count-vs-throughput-benchmark)

[LAB 16: Compression Algorithm Comparison](kafka-labs.md#lab-16-compression-algorithm-comparison)

[LAB 17: Replication Throttling During Migration](kafka-labs.md#lab-17-replication-throttling-during-migration)

[LAB 18: End-to-End Kafka Performance Benchmark](kafka-labs.md#lab-18-end-to-end-kafka-performance-benchmark)
