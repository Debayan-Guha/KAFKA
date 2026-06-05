# 1. TOPICS (Core Logical Unit)

## 1.1 Definition & Purpose

1.1.1 What is a Kafka Topic?

1.1.2 Purpose of Topics in Kafka Architecture

1.1.3 Why Topics are used for message organization

1.1.4 Topics as logical categories of data

1.1.5 Relationship between Topics, Producers, and Consumers

1.1.6 Topic-based publish-subscribe model

1.1.7 Real-world use cases of Topics

1.1.8 Importance of Topics in distributed messaging systems

## 1.2 Naming Conventions

1.2.1 Importance of proper topic naming

1.2.2 Kafka topic naming rules and restrictions

1.2.3 Use of lowercase letters

1.2.4 Use of hyphens (`-`), underscores (`_`), and dots (`.`)

1.2.5 Environment-based naming (`dev`, `test`, `prod`)

1.2.6 Domain-driven naming strategies

1.2.7 Naming based on business events

1.2.8 Naming consistency across organizations

1.2.9 Common naming mistakes to avoid

1.2.10 Best practices for topic naming

## 1.3 Topic vs Queue vs Stream

1.3.1 What is a Topic?

1.3.2 What is a Queue?

1.3.3 What is a Stream?

1.3.4 Messaging models: Queue vs Publish-Subscribe

1.3.5 Topic characteristics

1.3.6 Queue characteristics

1.3.7 Stream characteristics

1.3.8 Topic vs Queue comparison

1.3.9 Topic vs Stream comparison

1.3.10 Message consumption behavior differences

1.3.11 Data retention differences

1.3.12 Scalability differences

1.3.13 Real-world use cases for each

1.3.14 When to use Topics, Queues, or Streams

## 1.4 Internal Topics (`__consumer_offsets`, `__transaction_state`)

1.4.1 What are Internal Topics?

1.4.2 Purpose of Internal Topics in Kafka

1.4.3 Kafka-managed system topics

1.4.4 Understanding `__consumer_offsets`

1.4.4.1 Consumer offset storage

1.4.4.2 Consumer group tracking

1.4.4.3 Offset commits and retrieval

1.4.4.4 Role in fault tolerance

1.4.5 Understanding `__transaction_state`

1.4.5.1 Transaction metadata storage

1.4.5.2 Transaction coordinator functionality

1.4.5.3 Support for Exactly-Once Semantics (EOS)

1.4.6 Internal topic replication

1.4.7 Internal topic partitioning

1.4.8 Monitoring internal topics

1.4.9 Best practices when working with internal topics

## 1.5 Auto-Creation of Topics (`enable.auto.create`)

1.5.1 What is automatic topic creation?

1.5.2 How Kafka creates topics automatically

1.5.3 Configuration of `enable.auto.create.topics`

1.5.4 Producer-triggered topic creation

1.5.5 Consumer-triggered topic creation

1.5.6 Default topic configuration during auto-creation

1.5.7 Advantages of auto-creation

1.5.8 Disadvantages and risks

1.5.9 Impact on production environments

1.5.10 Common operational issues

1.5.11 Best practices

1.5.12 When to disable auto-creation

## 1.6 Topic Deletion (`delete.topic.enable`)

1.6.1 Purpose of topic deletion

1.6.2 Configuration of `delete.topic.enable`

1.6.3 How topic deletion works internally

1.6.4 Topic deletion workflow

1.6.5 Steps to delete a topic

1.6.6 Topic deletion lifecycle

1.6.7 Metadata cleanup process

1.6.8 Effects on producers and consumers

1.6.9 Impact on stored messages

1.6.10 Recovery considerations

1.6.11 Common issues during deletion

1.6.12 Best practices

1.6.13 Production precautions

1.6.14 When topic deletion should be restricted

# 2. PARTITIONS

## 2.1 What is a Partition

2.1.1 Definition of a Kafka Partition

2.1.2 Why Topics are divided into partitions

2.1.3 Partition as an ordered, immutable log

2.1.4 Relationship between Topics and Partitions

2.1.5 Message storage within a partition

2.1.6 Partition identifiers

2.1.7 Benefits of partitioning

2.1.8 Real-world examples

2.1.9 Role of partitions in Kafka scalability

## 2.2 Partition Key & Hashing

2.2.1 What is a Partition Key?

2.2.2 Purpose of partition keys

2.2.3 Key-based message routing

2.2.4 Hashing mechanism in Kafka

2.2.5 How Kafka calculates target partitions

2.2.6 Same key → Same partition concept

2.2.7 Ordering guarantees using keys

2.2.8 Keyed vs keyless messages

2.2.9 Choosing an effective partition key

2.2.10 Common partition key mistakes

2.2.11 Real-world use cases

## 2.3 Partition Assignment (Round-Robin, Sticky, Uniform Sticky)

2.3.1 What is partition assignment?

2.3.2 Why partition assignment is important

2.3.3 Default producer partitioning behavior

2.3.4 Round-Robin partition assignment

2.3.4.1 Working mechanism

2.3.4.2 Advantages

2.3.4.3 Limitations

2.3.5 Sticky partition assignment

2.3.5.1 Working mechanism

2.3.5.2 Batch optimization

2.3.5.3 Performance benefits

2.3.6 Uniform Sticky partition assignment

2.3.6.1 Working mechanism

2.3.6.2 Load balancing improvements

2.3.6.3 Distribution efficiency

2.3.7 Comparison of assignment strategies

2.3.8 Use cases for each strategy

2.3.9 Best practices

## 2.4 Partition Counts (How to Choose)

2.4.1 Importance of partition count selection

2.4.2 Factors affecting partition count

2.4.3 Throughput requirements

2.4.4 Consumer parallelism requirements

2.4.5 Future scalability considerations

2.4.6 Broker capacity considerations

2.4.7 Replication impact

2.4.8 Performance implications

2.4.9 Under-partitioning vs over-partitioning

2.4.10 Capacity planning strategies

2.4.11 Real-world sizing examples

2.4.12 Best practices for choosing partition counts

## 2.5 Partition Rebalancing

2.5.1 What is partition rebalancing?

2.5.2 Why rebalancing occurs

2.5.3 Consumer group membership changes

2.5.4 Consumer failure scenarios

2.5.5 New consumer joining a group

2.5.6 Consumer leaving a group

2.5.7 Reassignment of partitions

2.5.8 Impact on message processing

2.5.9 Rebalance lifecycle

2.5.10 Cooperative vs eager rebalancing

2.5.11 Challenges during rebalancing

2.5.12 Best practices to minimize rebalance impact

## 2.6 Partition Leadership

2.6.1 Concept of partition leaders and followers

2.6.2 Leader election process

2.6.3 Responsibilities of the leader partition

2.6.4 Responsibilities of follower replicas

2.6.5 Read and write operations through leaders

2.6.6 Leader failure handling

2.6.7 Automatic leader failover

2.6.8 Impact on producers and consumers

2.6.9 Importance of leader distribution

2.6.10 Monitoring partition leadership

## 2.7 Preferred Replica & Leader Rebalance

2.7.1 What is a preferred replica?

2.7.2 Preferred replica assignment

2.7.3 Purpose of preferred leaders

2.7.4 Uneven leader distribution issues

2.7.5 Leader rebalance process

2.7.6 Automatic leader balancing

2.7.7 Manual leader rebalance operations

2.7.8 Impact on cluster performance

2.7.9 Monitoring leader distribution

2.7.10 Best practices

## 2.8 Offsets Inside a Partition

2.8.1 What is an offset?

2.8.2 Offset numbering mechanism

2.8.3 Sequential message ordering

2.8.4 Offset uniqueness within a partition

2.8.5 Message retrieval using offsets

2.8.6 Consumer offset tracking

2.8.7 Offset commits

2.8.8 Earliest and latest offsets

2.8.9 Offset management concepts

2.8.10 Real-world examples

## 2.9 Partition Granularity for Parallelism

2.9.1 Relationship between partitions and parallelism

2.9.2 Consumer parallel processing model

2.9.3 One consumer per partition rule

2.9.4 Impact of partition count on throughput

2.9.5 Scaling consumers using partitions

2.9.6 Workload distribution strategies

2.9.7 Parallelism limitations

2.9.8 Performance optimization techniques

2.9.9 Real-world examples

2.9.10 Best practices

## 2.10 Increasing Partitions (Limitations)

2.10.1 Why partition counts are increased

2.10.2 Methods to increase partitions

2.10.3 Effects on data distribution

2.10.4 Impact on new messages

2.10.5 Impact on existing messages

2.10.6 Ordering implications

2.10.7 Key-hashing changes

2.10.8 Consumer group considerations

2.10.9 Operational challenges

2.10.10 Best practices before increasing partitions

2.10.11 Real-world scenarios

## 2.11 Decreasing Partitions (Not Allowed)

2.11.1 Why Kafka does not allow partition reduction

2.11.2 Risks of partition removal

2.11.3 Data consistency concerns

2.11.4 Offset management challenges

2.11.5 Ordering issues

2.11.6 Impact on consumer groups

2.11.7 Impact on producers

2.11.8 Alternative approaches

2.11.9 Topic recreation strategy

2.11.10 Data migration considerations

2.11.11 Best practices when redesigning partition layouts

# 3. BROKERS

## 3.1 Broker Role & Responsibilities

3.1.1 What is a Kafka Broker?

3.1.2 Role of a Broker in a Kafka Cluster

3.1.3 Message Storage Responsibilities

3.1.4 Producer Request Handling

3.1.5 Consumer Request Handling

3.1.6 Partition Leadership Management

3.1.7 Replica Synchronization Responsibilities

3.1.8 Metadata Management

3.1.9 Cluster Coordination Responsibilities

3.1.10 Real-World Broker Workflow

3.1.11 Importance of Brokers in Kafka Scalability and Reliability

## 3.2 Broker IDs

3.2.1 What is a Broker ID?

3.2.2 Purpose of Broker Identification

3.2.3 Broker ID Assignment

3.2.4 Manual vs Automatic Broker IDs

3.2.5 Uniqueness Requirements

3.2.6 Broker IDs in Cluster Metadata

3.2.7 Broker IDs and Partition Leadership

3.2.8 Broker IDs and Replication

3.2.9 Common Issues with Broker IDs

3.2.10 Best Practices

## 3.3 Bootstrap Servers

3.3.1 What are Bootstrap Servers?

3.3.2 Purpose of Bootstrap Servers

3.3.3 Client Connection Process

3.3.4 Metadata Discovery Mechanism

3.3.5 Producer Bootstrap Servers

3.3.6 Consumer Bootstrap Servers

3.3.7 Multiple Bootstrap Servers

3.3.8 Fault Tolerance Benefits

3.3.9 Common Misconceptions About Bootstrap Servers

3.3.10 Best Practices

## 3.4 Broker Configuration (`server.properties`)

3.4.1 Purpose of `server.properties`

3.4.2 Location of Broker Configuration File

3.4.3 Essential Broker Configurations

3.4.4 Network Configurations

3.4.5 Log Storage Configurations

3.4.6 Replication Configurations

3.4.7 Performance-Related Configurations

3.4.8 Retention Configurations

3.4.9 Security-Related Configurations

3.4.10 Configuration Validation

3.4.11 Common Configuration Mistakes

3.4.12 Best Practices

## 3.5 Rack Awareness (`rack.id`)

3.5.1 What is Rack Awareness?

3.5.2 Purpose of `rack.id`

3.5.3 Physical Rack and Availability Zone Concepts

3.5.4 Replica Placement Strategy

3.5.5 Fault Tolerance Benefits

3.5.6 Cross-Rack Replication

3.5.7 Rack Awareness in Multi-Datacenter Environments

3.5.8 Configuration of `rack.id`

3.5.9 Common Challenges

3.5.10 Best Practices

## 3.6 Broker Failure Detection

3.6.1 What is Broker Failure?

3.6.2 Failure Detection Mechanism

3.6.3 Heartbeats and Cluster Monitoring

3.6.4 Controller's Role in Failure Detection

3.6.5 Detection Time and Timeouts

3.6.6 Leader Re-Election After Failure

3.6.7 Impact on Producers

3.6.8 Impact on Consumers

3.6.9 Common Failure Scenarios

3.6.10 Monitoring Broker Health

3.6.11 Best Practices

## 3.7 Controlled Shutdown

3.7.1 What is Controlled Shutdown?

3.7.2 Purpose of Controlled Shutdown

3.7.3 Controlled vs Uncontrolled Shutdown

3.7.4 Leader Migration During Shutdown

3.7.5 Replica Reassignment During Shutdown

3.7.6 Impact on Producers and Consumers

3.7.7 Controlled Shutdown Process

3.7.8 Failure Handling During Shutdown

3.7.9 Operational Benefits

3.7.10 Best Practices

## 3.8 Broker Metadata (in KRaft vs ZK Mode)

3.8.1 What is Broker Metadata?

3.8.2 Importance of Metadata in Kafka

3.8.3 Metadata Components

3.8.4 Metadata Storage in ZooKeeper Mode

3.8.5 ZooKeeper's Role in Broker Management

3.8.6 Metadata Storage in KRaft Mode

3.8.7 KRaft Controller Responsibilities

3.8.8 KRaft Metadata Log

3.8.9 KRaft vs ZooKeeper Comparison

3.8.10 Migration Considerations

3.8.11 Best Practices

## 3.9 Broker Recovery

3.9.1 What is Broker Recovery?

3.9.2 Causes of Broker Failure

3.9.3 Broker Restart Process

3.9.4 Log Recovery Mechanism

3.9.5 Replica Synchronization After Recovery

3.9.6 Leader Reassignment After Recovery

3.9.7 Metadata Recovery

3.9.8 Recovery in KRaft Mode

3.9.9 Recovery in ZooKeeper Mode

3.9.10 Recovery Validation

3.9.11 Common Recovery Challenges

3.9.12 Best Practices

# 4. REPLICATION

## 4.1 Replication Factor (RF)

4.1.1 What is Replication Factor?

4.1.2 Purpose of Replication

4.1.3 How Replication Factor Works

4.1.4 Replica Placement Across Brokers

4.1.5 Choosing an Appropriate Replication Factor

4.1.6 RF = 1 vs RF = 2 vs RF = 3

4.1.7 Impact on Availability

4.1.8 Impact on Fault Tolerance

4.1.9 Impact on Storage Requirements

4.1.10 Performance Considerations

4.1.11 Best Practices

## 4.2 Leader & Follower Replicas

4.2.1 What are Leader Replicas?

4.2.2 What are Follower Replicas?

4.2.3 Responsibilities of Leader Replicas

4.2.4 Responsibilities of Follower Replicas

4.2.5 Read Operations and Leaders

4.2.6 Write Operations and Leaders

4.2.7 Replica Synchronization Process

4.2.8 Leader Failure Handling

4.2.9 Leader Election Process

4.2.10 Benefits of Leader-Follower Architecture

4.2.11 Best Practices

## 4.3 ISR (In-Sync Replicas)

4.3.1 What is ISR?

4.3.2 Purpose of ISR

4.3.3 Conditions for Joining ISR

4.3.4 Conditions for Leaving ISR

4.3.5 ISR Maintenance Process

4.3.6 ISR and Data Consistency

4.3.7 ISR and High Availability

4.3.8 ISR During Broker Failures

4.3.9 ISR Monitoring

4.3.10 Common ISR Issues

4.3.11 Best Practices

## 4.4 OSR (Out-of-Sync Replicas)

4.4.1 What is OSR?

4.4.2 Difference Between ISR and OSR

4.4.3 Causes of OSR

4.4.4 Replica Lag and Synchronization Delays

4.4.5 Impact on Availability

4.4.6 Impact on Fault Tolerance

4.4.7 OSR Recovery Process

4.4.8 Monitoring Out-of-Sync Replicas

4.4.9 Common OSR Scenarios

4.4.10 Best Practices

## 4.5 Unclean Leader Election (`unclean.leader.election.enable`)

4.5.1 What is Unclean Leader Election?

4.5.2 Purpose of Unclean Leader Election

4.5.3 Clean vs Unclean Leader Election

4.5.4 How Unclean Leader Election Works

4.5.5 Configuration of `unclean.leader.election.enable`

4.5.6 Data Loss Risks

4.5.7 Availability Benefits

4.5.8 Impact on Consistency

4.5.9 Real-World Failure Scenarios

4.5.10 When to Enable or Disable It

4.5.11 Best Practices

## 4.6 Min In-Sync Replicas (`min.insync.replicas`)

4.6.1 What is `min.insync.replicas`?

4.6.2 Purpose of Minimum In-Sync Replicas

4.6.3 Relationship with ISR

4.6.4 Interaction with Producer Acknowledgments

4.6.5 `acks=all` and Data Durability

4.6.6 Write Success Conditions

4.6.7 Write Failure Scenarios

4.6.8 Impact on Availability

4.6.9 Impact on Data Safety

4.6.10 Configuration Guidelines

4.6.11 Best Practices

## 4.7 Replication Throttling

4.7.1 What is Replication Throttling?

4.7.2 Purpose of Replication Throttling

4.7.3 Why Throttling is Required

4.7.4 Network Bandwidth Management

4.7.5 Throttling During Partition Reassignment

4.7.6 Throttling During Replica Recovery

4.7.7 Configuration of Replication Throttling

4.7.8 Performance Impacts

4.7.9 Monitoring Replication Traffic

4.7.10 Common Operational Scenarios

4.7.11 Best Practices

## 4.8 Fetch from Follower (`consumer.rack`)

4.8.1 What is Fetch from Follower?

4.8.2 Traditional Consumer Read Path

4.8.3 Purpose of Follower Fetching

4.8.4 Role of `consumer.rack`

4.8.5 Rack-Aware Consumer Routing

4.8.6 Local Replica Selection

4.8.7 Benefits for Network Optimization

4.8.8 Benefits for Cross-Rack Traffic Reduction

4.8.9 Performance Considerations

4.8.10 Limitations and Challenges

4.8.11 Best Practices

## 4.9 Replication Across Racks (Fault Tolerance)

4.9.1 What is Cross-Rack Replication?

4.9.2 Purpose of Rack-Aware Replication

4.9.3 Fault Tolerance Benefits

4.9.4 Rack Failure Scenarios

4.9.5 Availability Zone Awareness

4.9.6 Replica Distribution Strategies

4.9.7 Relationship with `broker.rack`

4.9.8 Replica Placement Rules

4.9.9 Impact on Availability

4.9.10 Impact on Disaster Recovery

4.9.11 Performance Trade-Offs

4.9.12 Best Practices

# 5. PRODUCERS (Detailed)

## 5.1 Producer Architecture

5.1.1 What is a Kafka Producer?

5.1.2 Role of Producers in Kafka Architecture

5.1.3 Producer Components

5.1.4 Producer Lifecycle

5.1.5 Producer Request Flow

5.1.6 Interaction with Brokers

5.1.7 Interaction with Partitions

5.1.8 Producer Thread Model

5.1.9 Record Accumulator

5.1.10 Sender Thread

5.1.11 Metadata Management

5.1.12 Producer Workflow End-to-End

## 5.2 ProducerRecord (topic, partition, key, value, timestamp, headers)

5.2.1 What is ProducerRecord?

5.2.2 Topic Field

5.2.3 Partition Field

5.2.4 Key Field

5.2.5 Value Field

5.2.6 Timestamp Field

5.2.7 Headers Field

5.2.8 Record Metadata

5.2.9 ProducerRecord Construction

5.2.10 Common Use Cases

## 5.3 Serializers (String, Avro, JSON, Protobuf, Custom)

5.3.1 What is Serialization?

5.3.2 Why Serializers are Required

5.3.3 Key Serializer vs Value Serializer

5.3.4 String Serializer

5.3.5 JSON Serializer

5.3.6 Avro Serializer

5.3.7 Protobuf Serializer

5.3.8 ByteArray Serializer

5.3.9 Custom Serializers

5.3.10 Schema Evolution Considerations

5.3.11 Performance Considerations

5.3.12 Best Practices

## 5.4 Partitioner (Default, Custom, Key-Based)

5.4.1 What is a Partitioner?

5.4.2 Purpose of Partitioning

5.4.3 Default Kafka Partitioner

5.4.4 Key-Based Partitioning

5.4.5 Partitioning Without a Key

5.4.6 Sticky Partitioning

5.4.7 Uniform Sticky Partitioning

5.4.8 Custom Partitioner

5.4.9 Partition Selection Algorithms

5.4.10 Ordering Implications

5.4.11 Performance Considerations

5.4.12 Best Practices

## 5.5 Interceptors

5.5.1 What are Producer Interceptors?

5.5.2 Purpose of Interceptors

5.5.3 Interceptor Lifecycle

5.5.4 Record Modification Before Sending

5.5.5 Response Interception

5.5.6 Logging Use Cases

5.5.7 Monitoring Use Cases

5.5.8 Audit Use Cases

5.5.9 Custom Interceptors

5.5.10 Performance Considerations

5.5.11 Best Practices

## 5.6 Acks Levels

5.6.1 Purpose of Acknowledgments

5.6.2 How Acknowledgments Work

5.6.3 Relationship with Reliability

5.6.4 Relationship with Performance

5.6.5 Choosing the Right Acks Level

5.6.6 Acks and Replication

5.6.7 Acks and Data Durability

### 5.6.1 acks=0 (Fire & Forget)

5.6.1.1 How acks=0 Works

5.6.1.2 Producer Behavior

5.6.1.3 Performance Benefits

5.6.1.4 Reliability Risks

5.6.1.5 Use Cases

5.6.1.6 Limitations

### 5.6.2 acks=1 (Leader Only)

5.6.2.1 How acks=1 Works

5.6.2.2 Leader Acknowledgment Process

5.6.2.3 Performance Characteristics

5.6.2.4 Failure Scenarios

5.6.2.5 Reliability Considerations

5.6.2.6 Use Cases

### 5.6.3 acks=all / -1 (Leader + ISR)

5.6.3.1 How acks=all Works

5.6.3.2 ISR Acknowledgment Requirements

5.6.3.3 Relationship with min.insync.replicas

5.6.3.4 Data Durability Benefits

5.6.3.5 Performance Trade-Offs

5.6.3.6 Failure Handling

5.6.3.7 Use Cases

## 5.7 Retries

5.7.1 Why Retries are Needed

5.7.2 Retry Mechanism

5.7.3 Recoverable vs Non-Recoverable Errors

5.7.4 Retries and Message Ordering

5.7.5 Retries and Idempotence

5.7.6 Performance Considerations

5.7.7 Best Practices

### 5.7.1 retries Config

5.7.1.1 Purpose of retries

5.7.1.2 Default Behavior

5.7.1.3 Unlimited vs Limited Retries

5.7.1.4 Failure Recovery

5.7.1.5 Configuration Guidelines

### 5.7.2 retry.backoff.ms

5.7.2.1 Purpose of Backoff

5.7.2.2 Retry Delay Mechanism

5.7.2.3 Avoiding Broker Overload

5.7.2.4 Configuration Guidelines

### 5.7.3 deliver.timeout.ms

5.7.3.1 Purpose of Delivery Timeout

5.7.3.2 Message Expiration Logic

5.7.3.3 Interaction with Retries

5.7.3.4 Configuration Guidelines

### 5.7.4 request.timeout.ms

5.7.4.1 Purpose of Request Timeout

5.7.4.2 Broker Response Waiting Period

5.7.4.3 Timeout Scenarios

5.7.4.4 Configuration Guidelines

## 5.8 Batching

5.8.1 Why Batching is Important

5.8.2 Batch Creation Process

5.8.3 Performance Benefits

5.8.4 Network Optimization

5.8.5 Throughput Improvements

5.8.6 Batching Trade-Offs

5.8.7 Best Practices

### 5.8.1 batch.size

5.8.1.1 Purpose of batch.size

5.8.1.2 Batch Allocation Process

5.8.1.3 Performance Impact

5.8.1.4 Configuration Guidelines

### 5.8.2 linger.ms

5.8.2.1 Purpose of linger.ms

5.8.2.2 Waiting for Additional Records

5.8.2.3 Throughput vs Latency Trade-Off

5.8.2.4 Configuration Guidelines

### 5.8.3 buffer.memory

5.8.3.1 Purpose of buffer.memory

5.8.3.2 Record Buffering Process

5.8.3.3 Memory Management

5.8.3.4 Configuration Guidelines

### 5.8.4 max.block.ms

5.8.4.1 Purpose of max.block.ms

5.8.4.2 Blocking Scenarios

5.8.4.3 Timeout Behavior

5.8.4.4 Configuration Guidelines

## 5.9 Compression

5.9.1 Why Compression is Used

5.9.2 Compression Workflow

5.9.3 Network Savings

5.9.4 Storage Savings

5.9.5 Compression vs CPU Usage

5.9.6 Best Practices

### 5.9.1 gzip, snappy, lz4, zstd

5.9.1.1 GZIP Compression

5.9.1.2 Snappy Compression

5.9.1.3 LZ4 Compression

5.9.1.4 ZSTD Compression

5.9.1.5 Comparison of Compression Algorithms

5.9.1.6 Use Cases

### 5.9.2 Broker-Side Decompression/Compression

5.9.2.1 Compression Flow Through Kafka

5.9.2.2 Broker Handling of Compressed Data

5.9.2.3 Log Storage Considerations

5.9.2.4 Replication Considerations

### 5.9.3 Trade-Offs (CPU vs Network)

5.9.3.1 CPU Consumption

5.9.3.2 Network Utilization

5.9.3.3 Throughput Impacts

5.9.3.4 Choosing the Right Compression Strategy

## 5.10 Idempotent Producer (`enable.idempotence`)

5.10.1 What is Idempotence?

5.10.2 Why Idempotence is Needed

5.10.3 Duplicate Message Prevention

5.10.4 Configuration of `enable.idempotence`

5.10.5 Idempotence Requirements

5.10.6 Reliability Improvements

5.10.7 Failure Recovery

5.10.8 Best Practices

### 5.10.1 Exactly-Once Semantics (Producer Side)

5.10.1.1 What is Exactly-Once Delivery?

5.10.1.2 At-Most-Once vs At-Least-Once vs Exactly-Once

5.10.1.3 Role of Idempotence

5.10.1.4 Producer Guarantees

5.10.1.5 Limitations

### 5.10.2 Sequence Numbers & Producer IDs

5.10.2.1 Producer ID (PID)

5.10.2.2 Sequence Numbers

5.10.2.3 Duplicate Detection Mechanism

5.10.2.4 Failure Handling

5.10.2.5 Ordering Guarantees

## 5.11 Transactions (Producer-Only)

5.11.1 What are Kafka Transactions?

5.11.2 Purpose of Transactions

5.11.3 Transactional Producers

5.11.4 Atomic Writes

5.11.5 Transactions and Exactly-Once Semantics

5.11.6 Transaction Lifecycle

5.11.7 Failure Handling

5.11.8 Best Practices

### 5.11.1 initTransactions()

5.11.1.1 Purpose of initTransactions()

5.11.1.2 Producer Initialization Process

5.11.1.3 Transaction Coordinator Interaction

5.11.1.4 Common Errors

### 5.11.2 beginTransaction()

5.11.2.1 Purpose of beginTransaction()

5.11.2.2 Transaction Start Process

5.11.2.3 Producer Workflow

5.11.2.4 Usage Considerations

### 5.11.3 commitTransaction() / abortTransaction()

#### 5.11.3.1 commitTransaction()

5.11.3.1.1 Commit Process

5.11.3.1.2 Successful Completion

5.11.3.1.3 Visibility to Consumers

#### 5.11.3.2 abortTransaction()

5.11.3.2.1 Abort Process

5.11.3.2.2 Rollback Behavior

5.11.3.2.3 Failure Recovery

## 5.12 Producer Monitoring

5.12.1 Purpose of Producer Monitoring

5.12.2 JMX Metrics

5.12.3 Monitoring Tools

5.12.4 Performance Troubleshooting

5.12.5 Capacity Planning

5.12.6 Best Practices

### 5.12.1 request-rate, error-rate

5.12.1.1 Request Rate

5.12.1.2 Error Rate

5.12.1.3 Interpretation of Metrics

5.12.1.4 Alerting Strategies

### 5.12.2 record-retry-rate, record-send-total

5.12.2.1 Record Retry Rate

5.12.2.2 Record Send Total

5.12.2.3 Retry Analysis

5.12.2.4 Performance Indicators

### 5.12.3 batch-size-avg, compression-rate

5.12.3.1 Average Batch Size

5.12.3.2 Compression Rate

5.12.3.3 Throughput Analysis

5.12.3.4 Optimization Opportunities

# 6. CONSUMERS (Detailed)

## 6.1 Consumer Architecture

6.1.1 What is a Kafka Consumer?

6.1.2 Role of Consumers in Kafka Architecture

6.1.3 Consumer Components

6.1.4 Consumer Lifecycle

6.1.5 Consumer Request Flow

6.1.6 Interaction with Brokers

6.1.7 Interaction with Partitions

6.1.8 Consumer Thread Model

6.1.9 Fetch Requests

6.1.10 Metadata Management

6.1.11 Consumer Workflow End-to-End

## 6.2 Consumer Group Concept

6.2.1 What is a Consumer Group?

6.2.2 Purpose of Consumer Groups

6.2.3 Group Membership

6.2.4 Partition Assignment within a Group

6.2.5 Load Balancing

6.2.6 Fault Tolerance

6.2.7 Consumer Groups vs Individual Consumers

6.2.8 Real-World Use Cases

6.2.9 Best Practices

## 6.3 Group Coordinator & Group Leader

6.3.1 What is a Group Coordinator?

6.3.2 Responsibilities of the Group Coordinator

6.3.3 Coordinator Selection Process

6.3.4 What is a Group Leader?

6.3.5 Responsibilities of the Group Leader

6.3.6 Partition Assignment Strategy

6.3.7 Coordinator vs Leader

6.3.8 Failure Handling

6.3.9 Rebalance Coordination

## 6.4 Offset

6.4.1 What is an Offset?

6.4.2 Purpose of Offsets

6.4.3 Offset Assignment

6.4.4 Offset Tracking

6.4.5 Offset Management

6.4.6 Offset Lifecycle

6.4.7 Importance of Offsets in Reliability

### 6.4.1 Current Offset

6.4.1.1 Definition of Current Offset

6.4.1.2 Current Processing Position

6.4.1.3 Current Offset vs Committed Offset

6.4.1.4 Monitoring Current Offsets

### 6.4.2 Committed Offset

6.4.2.1 Definition of Committed Offset

6.4.2.2 Offset Commit Process

6.4.2.3 Recovery Using Committed Offsets

6.4.2.4 Reliability Considerations

### 6.4.3 Earliest / Latest / Specific Offset

6.4.3.1 Earliest Offset

6.4.3.2 Latest Offset

6.4.3.3 Specific Offset

6.4.3.4 Offset Reset Strategies

6.4.3.5 Real-World Use Cases

### 6.4.4 Offset Storage (`__consumer_offsets`)

6.4.4.1 Purpose of `__consumer_offsets`

6.4.4.2 Offset Persistence

6.4.4.3 Offset Retrieval

6.4.4.4 Internal Topic Structure

6.4.4.5 Fault Tolerance Benefits

## 6.5 Offset Commit Strategies

6.5.1 Why Offset Commits are Required

6.5.2 Commit Lifecycle

6.5.3 At-Most-Once Processing

6.5.4 At-Least-Once Processing

6.5.5 Exactly-Once Considerations

6.5.6 Choosing the Right Strategy

6.5.7 Best Practices

### 6.5.1 Auto Commit (`enable.auto.commit`, `auto.commit.interval.ms`)

6.5.1.1 What is Auto Commit?

6.5.1.2 Configuration of `enable.auto.commit`

6.5.1.3 Configuration of `auto.commit.interval.ms`

6.5.1.4 Auto Commit Workflow

6.5.1.5 Advantages

6.5.1.6 Disadvantages

6.5.1.7 Use Cases

### 6.5.2 Manual Sync Commit

6.5.2.1 What is Synchronous Commit?

6.5.2.2 commitSync() Workflow

6.5.2.3 Reliability Benefits

6.5.2.4 Performance Considerations

6.5.2.5 Use Cases

### 6.5.3 Manual Async Commit

6.5.3.1 What is Asynchronous Commit?

6.5.3.2 commitAsync() Workflow

6.5.3.3 Performance Benefits

6.5.3.4 Reliability Trade-Offs

6.5.3.5 Use Cases

### 6.5.4 Commit with Callbacks

6.5.4.1 Purpose of Commit Callbacks

6.5.4.2 Callback Workflow

6.5.4.3 Success Handling

6.5.4.4 Failure Handling

6.5.4.5 Best Practices

## 6.6 Deserializers

6.6.1 What is Deserialization?

6.6.2 Why Deserializers are Required

6.6.3 Key Deserializer vs Value Deserializer

6.6.4 String Deserializer

6.6.5 JSON Deserializer

6.6.6 Avro Deserializer

6.6.7 Protobuf Deserializer

6.6.8 ByteArray Deserializer

6.6.9 Custom Deserializers

6.6.10 Schema Compatibility Considerations

6.6.11 Best Practices

## 6.7 Poll Loop

6.7.1 Purpose of Polling

6.7.2 Consumer Polling Workflow

6.7.3 Poll Loop Lifecycle

6.7.4 Message Fetching Process

6.7.5 Error Handling

6.7.6 Performance Considerations

6.7.7 Best Practices

### 6.7.1 poll(Duration)

6.7.1.1 Purpose of poll()

6.7.1.2 Poll Timeout Behavior

6.7.1.3 Record Retrieval Process

6.7.1.4 Best Practices

### 6.7.2 max.poll.records

6.7.2.1 Purpose of max.poll.records

6.7.2.2 Batch Processing Impact

6.7.2.3 Throughput Considerations

6.7.2.4 Configuration Guidelines

### 6.7.3 max.poll.interval.ms

6.7.3.1 Purpose of max.poll.interval.ms

6.7.3.2 Consumer Processing Time Limits

6.7.3.3 Rebalance Implications

6.7.3.4 Configuration Guidelines

### 6.7.4 fetch.min.bytes

6.7.4.1 Purpose of fetch.min.bytes

6.7.4.2 Broker Fetch Behavior

6.7.4.3 Throughput Optimization

6.7.4.4 Configuration Guidelines

### 6.7.5 fetch.max.wait.ms

6.7.5.1 Purpose of fetch.max.wait.ms

6.7.5.2 Broker Wait Strategy

6.7.5.3 Latency vs Throughput Trade-Off

6.7.5.4 Configuration Guidelines

## 6.8 Consumer Rebalancing

6.8.1 What is Consumer Rebalancing?

6.8.2 Why Rebalancing Occurs

6.8.3 Rebalance Lifecycle

6.8.4 Partition Reassignment

6.8.5 Consumer Join Events

6.8.6 Consumer Leave Events

6.8.7 Performance Impact

6.8.8 Best Practices

### 6.8.1 Eager Rebalancing (Stop-the-World)

6.8.1.1 What is Eager Rebalancing?

6.8.1.2 Rebalance Workflow

6.8.1.3 Partition Revocation Process

6.8.1.4 Advantages

6.8.1.5 Limitations

### 6.8.2 Cooperative Rebalancing (Incremental)

6.8.2.1 What is Cooperative Rebalancing?

6.8.2.2 Incremental Assignment Process

6.8.2.3 Advantages

6.8.2.4 Performance Benefits

6.8.2.5 Limitations

### 6.8.3 Partition Rebalance Listeners

6.8.3.1 onPartitionsRevoked()

6.8.3.2 Purpose of Revocation Callback

6.8.3.3 onPartitionsAssigned()

6.8.3.4 Purpose of Assignment Callback

6.8.3.5 Offset Handling During Rebalance

6.8.3.6 Best Practices

### 6.8.4 Static Group Membership (`group.instance.id`)

6.8.4.1 What is Static Membership?

6.8.4.2 Purpose of `group.instance.id`

6.8.4.3 Reducing Rebalances

6.8.4.4 Consumer Restart Behavior

6.8.4.5 Best Practices

## 6.9 Consumer Liveness

6.9.1 What is Consumer Liveness?

6.9.2 Heartbeat Mechanism

6.9.3 Failure Detection

6.9.4 Liveness Monitoring

6.9.5 Best Practices

### 6.9.1 session.timeout.ms

6.9.1.1 Purpose of session.timeout.ms

6.9.1.2 Failure Detection Window

6.9.1.3 Rebalance Trigger Conditions

6.9.1.4 Configuration Guidelines

### 6.9.2 heartbeat.interval.ms

6.9.2.1 Purpose of heartbeat.interval.ms

6.9.2.2 Heartbeat Frequency

6.9.2.3 Relationship with session.timeout.ms

6.9.2.4 Configuration Guidelines

## 6.10 Consumer Position Control

6.10.1 Why Position Control is Important

6.10.2 Replay Use Cases

6.10.3 Recovery Use Cases

6.10.4 Best Practices

### 6.10.1 seek()

6.10.1.1 Purpose of seek()

6.10.1.2 Jumping to Specific Offsets

6.10.1.3 Replay Scenarios

6.10.1.4 Best Practices

### 6.10.2 seekToBeginning() / seekToEnd()

6.10.2.1 seekToBeginning()

6.10.2.2 seekToEnd()

6.10.2.3 Common Use Cases

6.10.2.4 Best Practices

### 6.10.3 position()

6.10.3.1 Purpose of position()

6.10.3.2 Retrieving Current Position

6.10.3.3 Monitoring Progress

6.10.3.4 Best Practices

### 6.10.4 pause() / resume()

6.10.4.1 Purpose of pause()

6.10.4.2 Purpose of resume()

6.10.4.3 Flow Control

6.10.4.4 Backpressure Handling

6.10.4.5 Best Practices

## 6.11 Consumer Groups vs No-Group (Simple Consumer)

6.11.1 Consumer Group Model

6.11.2 Simple Consumer Model

6.11.3 Partition Assignment Differences

6.11.4 Scalability Comparison

6.11.5 Fault Tolerance Comparison

6.11.6 Offset Management Differences

6.11.7 Use Cases

6.11.8 Best Practices

## 6.12 Consumer Lag

### 6.12.1 Definition & Causes

6.12.1.1 What is Consumer Lag?

6.12.1.2 How Lag is Calculated

6.12.1.3 Common Causes of Lag

6.12.1.4 Effects of High Lag

6.12.1.5 Monitoring Lag

### 6.12.2 Tools: kafka-consumer-groups, Burrow, LagExporter

6.12.2.1 kafka-consumer-groups Tool

6.12.2.2 Burrow

6.12.2.3 LagExporter

6.12.2.4 Tool Comparison

6.12.2.5 Best Practices

### 6.12.3 Reducing Lag

6.12.3.1 Scaling Consumers

6.12.3.2 Increasing Partitions

6.12.3.3 Optimizing Processing Logic

6.12.3.4 Tuning Consumer Configurations

6.12.3.5 Monitoring and Alerting

## 6.13 Multi-Threaded Consumers

6.13.1 Why Multi-Threading is Needed

6.13.2 Thread Safety Considerations

6.13.3 Performance Benefits

6.13.4 Design Trade-Offs

6.13.5 Best Practices

### 6.13.1 One Consumer Per Thread

6.13.1.1 Architecture Overview

6.13.1.2 Partition Ownership

6.13.1.3 Advantages

6.13.1.4 Limitations

6.13.1.5 Use Cases

### 6.13.2 Single Consumer + Multiple Worker Threads

6.13.2.1 Architecture Overview

6.13.2.2 Work Distribution Strategy

6.13.2.3 Offset Management Challenges

6.13.2.4 Advantages

6.13.2.5 Limitations

6.13.2.6 Use Cases

# 7. DELIVERY SEMANTICS & GUARANTEES

## 7.1 At-Most-Once

7.1.1 What is At-Most-Once Delivery?

7.1.2 Core Principle of At-Most-Once

7.1.3 Message Loss Characteristics

7.1.4 Processing Workflow

7.1.5 Role of Auto Offset Commit

7.1.6 `enable.auto.commit=true` Behavior

7.1.7 Impact of `acks=0`

7.1.8 Impact of `acks=1`

7.1.9 Failure Scenarios

7.1.10 Advantages

7.1.11 Disadvantages

7.1.12 Use Cases

7.1.13 Best Practices

## 7.2 At-Least-Once

7.2.1 What is At-Least-Once Delivery?

7.2.2 Core Principle of At-Least-Once

7.2.3 Duplicate Message Characteristics

7.2.4 Processing Workflow

7.2.5 Role of `acks=all`

7.2.6 Role of Producer Retries

7.2.7 Manual Offset Commit After Processing

7.2.8 Failure Recovery Process

7.2.9 Duplicate Processing Scenarios

7.2.10 Advantages

7.2.11 Disadvantages

7.2.12 Use Cases

7.2.13 Best Practices

## 7.3 Exactly-Once

7.3.1 What is Exactly-Once Delivery?

7.3.2 Why Exactly-Once is Difficult

7.3.3 At-Most-Once vs At-Least-Once vs Exactly-Once

7.3.4 Components Required for Exactly-Once

7.3.5 End-to-End Processing Guarantees

7.3.6 Producer-Side Guarantees

7.3.7 Consumer-Side Guarantees

7.3.8 Transactional Processing Workflow

7.3.9 Failure Handling

7.3.10 Advantages

7.3.11 Limitations

7.3.12 Use Cases

7.3.13 Best Practices

### 7.3.1 Idempotent Producer (`enable.idempotence`)

7.3.1.1 What is Idempotence?

7.3.1.2 Purpose of Idempotent Producers

7.3.1.3 Duplicate Message Prevention

7.3.1.4 Configuration of `enable.idempotence`

7.3.1.5 Producer ID (PID)

7.3.1.6 Sequence Numbers

7.3.1.7 Broker Duplicate Detection

7.3.1.8 Ordering Guarantees

7.3.1.9 Failure Recovery

7.3.1.10 Benefits and Limitations

### 7.3.2 Transactions (Producer + Consumer)

7.3.2.1 What are Kafka Transactions?

7.3.2.2 Transactional Producer Concept

7.3.2.3 Transactional Consumer Concept

7.3.2.4 Atomic Writes Across Partitions

7.3.2.5 Atomic Offset Commits

7.3.2.6 Transaction Coordinator

7.3.2.7 Transaction Lifecycle

7.3.2.8 Commit Transaction Workflow

7.3.2.9 Abort Transaction Workflow

7.3.2.10 Failure Recovery

7.3.2.11 Benefits and Limitations

### 7.3.3 `read_committed` Isolation Level

7.3.3.1 What is Isolation Level?

7.3.3.2 `read_uncommitted` vs `read_committed`

7.3.3.3 Purpose of `read_committed`

7.3.3.4 Visibility of Transactional Messages

7.3.3.5 Handling Aborted Transactions

7.3.3.6 Consumer Configuration

7.3.3.7 Performance Considerations

7.3.3.8 Use Cases

### 7.3.4 Exactly-Once for Kafka Streams / Kafka Connect

7.3.4.1 Exactly-Once in Kafka Streams

7.3.4.2 Stream Processing Guarantees

7.3.4.3 State Store Consistency

7.3.4.4 Transactional Processing in Streams

7.3.4.5 Exactly-Once in Kafka Connect

7.3.4.6 Source Connector Guarantees

7.3.4.7 Sink Connector Guarantees

7.3.4.8 Configuration Requirements

7.3.4.9 Operational Considerations

## 7.4 When Exactly-Once Fails (Recovery Semantics)

7.4.1 Failure Scenarios in Exactly-Once Processing

7.4.2 Producer Failure During Transactions

7.4.3 Broker Failure During Transactions

7.4.4 Consumer Failure During Processing

7.4.5 Transaction Timeout Scenarios

7.4.6 Zombie Producer Problem

7.4.7 Transaction Coordinator Failure

7.4.8 Network Partition Scenarios

7.4.9 Recovery Mechanisms

7.4.10 Transaction Abort and Rollback

7.4.11 Offset Recovery

7.4.12 Data Consistency Guarantees During Recovery

7.4.13 Remaining Edge Cases

7.4.14 Operational Challenges

7.4.15 Best Practices for Recovery Handling

# 8. KAFKA INTERNALS (Advanced)

## 8.1 Log Segments

8.1.1 What are Log Segments?

8.1.2 Why Kafka Uses Log Segments

8.1.3 Segment Structure

8.1.4 Active Segment vs Closed Segment

8.1.5 Message Storage Inside Segments

8.1.6 Segment Creation Process

8.1.7 Segment Lifecycle

8.1.8 Performance Benefits

8.1.9 Storage Management Benefits

8.1.10 Best Practices

### 8.1.1 `segment.bytes`, `segment.ms`

8.1.1.1 Purpose of `segment.bytes`

8.1.1.2 Purpose of `segment.ms`

8.1.1.3 Segment Rolling Conditions

8.1.1.4 Size-Based Rolling

8.1.1.5 Time-Based Rolling

8.1.1.6 Configuration Trade-Offs

8.1.1.7 Performance Considerations

### 8.1.2 `.log`, `.index`, `.timeindex` Files

8.1.2.1 `.log` File Structure

8.1.2.2 Purpose of `.log` Files

8.1.2.3 `.index` File Structure

8.1.2.4 Purpose of Offset Indexes

8.1.2.5 `.timeindex` File Structure

8.1.2.6 Timestamp-Based Lookups

8.1.2.7 Relationship Between the Files

8.1.2.8 Storage Layout Examples

### 8.1.3 Rolling & Retention

8.1.3.1 What is Segment Rolling?

8.1.3.2 Why Rolling is Required

8.1.3.3 Segment Closure Process

8.1.3.4 Retention Eligibility

8.1.3.5 Rolling and Compaction Relationship

8.1.3.6 Operational Considerations

## 8.2 Retention Policies

8.2.1 Purpose of Retention Policies

8.2.2 Data Lifecycle Management

8.2.3 Retention Evaluation Process

8.2.4 Retention and Storage Planning

8.2.5 Topic-Level vs Broker-Level Retention

8.2.6 Retention Best Practices

### 8.2.1 Time-Based (`retention.ms`)

8.2.1.1 What is Time-Based Retention?

8.2.1.2 Purpose of `retention.ms`

8.2.1.3 Message Expiration Process

8.2.1.4 Segment Deletion Workflow

8.2.1.5 Configuration Guidelines

8.2.1.6 Real-World Use Cases

### 8.2.2 Size-Based (`retention.bytes`)

8.2.2.1 What is Size-Based Retention?

8.2.2.2 Purpose of `retention.bytes`

8.2.2.3 Storage Threshold Calculation

8.2.2.4 Segment Removal Process

8.2.2.5 Capacity Planning

8.2.2.6 Real-World Use Cases

### 8.2.3 Compacted (`cleanup.policy=compact`)

8.2.3.1 What is Log Compaction?

8.2.3.2 Purpose of `cleanup.policy=compact`

8.2.3.3 Key-Based Retention

8.2.3.4 Latest Record Preservation

8.2.3.5 Tombstone Records

8.2.3.6 Compaction Workflow

8.2.3.7 Real-World Use Cases

### 8.2.4 Delete + Compact

8.2.4.1 Combined Cleanup Policy

8.2.4.2 Configuration of Multiple Policies

8.2.4.3 Retention and Compaction Interaction

8.2.4.4 Data Lifecycle Behavior

8.2.4.5 Use Cases

8.2.4.6 Best Practices

## 8.3 Log Cleaner (for Compaction)

8.3.1 What is the Log Cleaner?

8.3.2 Purpose of Log Cleaning

8.3.3 Log Cleaner Architecture

8.3.4 Cleaner Threads

8.3.5 Compaction Workflow

8.3.6 Handling Tombstones

8.3.7 Dirty Ratio Concept

8.3.8 Log Cleaner Performance

8.3.9 Monitoring Log Cleaner Activity

8.3.10 Best Practices

## 8.4 Leader Election

8.4.1 What is Leader Election?

8.4.2 Why Leader Election is Needed

8.4.3 Election Triggers

8.4.4 Election Workflow

8.4.5 ISR-Based Elections

8.4.6 Impact on Availability

8.4.7 Impact on Consistency

8.4.8 Failure Recovery

8.4.9 Monitoring Elections

8.4.10 Best Practices

### 8.4.1 Preferred Leader Election

8.4.1.1 What is a Preferred Leader?

8.4.1.2 Preferred Replica Assignment

8.4.1.3 Preferred Leader Election Workflow

8.4.1.4 Load Balancing Benefits

8.4.1.5 Operational Considerations

### 8.4.2 Unclean Election (Pros/Cons)

8.4.2.1 What is Unclean Election?

8.4.2.2 Election Process

8.4.2.3 Availability Advantages

8.4.2.4 Data Loss Risks

8.4.2.5 Consistency Trade-Offs

8.4.2.6 Production Recommendations

### 8.4.3 Controller's Role

8.4.3.1 Controller Participation in Elections

8.4.3.2 Election Coordination

8.4.3.3 Leader Assignment Decisions

8.4.3.4 Metadata Updates

8.4.3.5 Failure Handling

## 8.5 The Controller (Broker)

8.5.1 What is the Controller?

8.5.2 Purpose of the Controller

8.5.3 Controller Architecture

8.5.4 Controller Responsibilities

8.5.5 Controller Communication with Brokers

8.5.6 Controller Failure Handling

8.5.7 Metadata Management

8.5.8 Monitoring Controller Health

8.5.9 Best Practices

### 8.5.1 Controller Election (ZooKeeper / KRaft)

8.5.1.1 Controller Election in ZooKeeper Mode

8.5.1.2 Controller Election in KRaft Mode

8.5.1.3 Election Workflow

8.5.1.4 Failure Recovery

8.5.1.5 Comparison of Election Mechanisms

### 8.5.2 Controller Responsibilities (Partition Re-Assignment, Leader Changes)

8.5.2.1 Partition Reassignment Management

8.5.2.2 Leader Election Coordination

8.5.2.3 Broker Failure Handling

8.5.2.4 Replica State Management

8.5.2.5 Metadata Updates

8.5.2.6 Cluster Balancing Tasks

## 8.6 ZooKeeper Roles (Pre-2.8)

8.6.1 What is ZooKeeper?

8.6.2 Why Kafka Used ZooKeeper

8.6.3 ZooKeeper Architecture

8.6.4 ZooKeeper and Kafka Interaction

8.6.5 Limitations of ZooKeeper-Based Kafka

8.6.6 Deprecation and Transition to KRaft

### 8.6.1 Metadata Storage

8.6.1.1 Topic Metadata Storage

8.6.1.2 Broker Metadata Storage

8.6.1.3 Partition Metadata Storage

8.6.1.4 Configuration Storage

### 8.6.2 Broker Registration

8.6.2.1 Broker Registration Process

8.6.2.2 Ephemeral Nodes

8.6.2.3 Broker Discovery

8.6.2.4 Broker Failure Detection

### 8.6.3 Controller Election

8.6.3.1 Controller Election Workflow

8.6.3.2 Election Coordination

8.6.3.3 Failure Recovery

8.6.3.4 Limitations

### 8.6.4 Topic Configuration

8.6.4.1 Topic Metadata Management

8.6.4.2 Topic Creation Workflow

8.6.4.3 Topic Deletion Workflow

8.6.4.4 Configuration Updates

## 8.7 KRaft (Kafka Raft) – Post-2.8 / 3.x

8.7.1 What is KRaft?

8.7.2 Why KRaft Was Introduced

8.7.3 KRaft Architecture

8.7.4 Metadata Management in KRaft

8.7.5 Benefits Over ZooKeeper

8.7.6 Operational Advantages

8.7.7 Migration Considerations

8.7.8 Best Practices

### 8.7.1 Metadata Quorum

8.7.1.1 What is a Metadata Quorum?

8.7.1.2 Raft Consensus Basics

8.7.1.3 Quorum Formation

8.7.1.4 Metadata Replication

8.7.1.5 Fault Tolerance

### 8.7.2 Controller Nodes & Voters

8.7.2.1 Dedicated Controller Nodes

8.7.2.2 Voter Nodes

8.7.2.3 Controller Quorum Configuration

8.7.2.4 Leader Election in Quorum

8.7.2.5 Failure Handling

### 8.7.3 No ZooKeeper

8.7.3.1 ZooKeeper Removal Benefits

8.7.3.2 Simplified Architecture

8.7.3.3 Reduced Operational Complexity

8.7.3.4 Performance Improvements

8.7.3.5 Scalability Improvements

### 8.7.4 Migration from ZK Mode

8.7.4.1 Migration Prerequisites

8.7.4.2 Migration Workflow

8.7.4.3 Compatibility Considerations

8.7.4.4 Operational Risks

8.7.4.5 Validation Steps

## 8.8 Request Handling

8.8.1 Kafka Network Architecture

8.8.2 Request Processing Lifecycle

8.8.3 Client-to-Broker Communication

8.8.4 Threading Model Overview

8.8.5 Performance Optimization

8.8.6 Monitoring Request Handling

### 8.8.1 Acceptor Threads

8.8.1.1 What are Acceptor Threads?

8.8.1.2 Connection Acceptance Workflow

8.8.1.3 Role in Network Processing

8.8.1.4 Configuration Considerations

### 8.8.2 Processor (Network) Threads

8.8.2.1 What are Processor Threads?

8.8.2.2 Request Parsing

8.8.2.3 Request Queuing

8.8.2.4 Network Throughput Considerations

### 8.8.3 Kafka Request Handler Threads

8.8.3.1 What are Request Handler Threads?

8.8.3.2 Request Execution Workflow

8.8.3.3 Disk and Memory Operations

8.8.3.4 Performance Considerations

### 8.8.4 Response Threading

8.8.4.1 Response Generation

8.8.4.2 Response Queueing

8.8.4.3 Network Transmission

8.8.4.4 End-to-End Request/Response Flow

# 9. ADMINISTRATION & OPERATIONS

## 9.1 Topic Management (`kafka-topics.sh`)

9.1.1 What is `kafka-topics.sh`?

9.1.2 Purpose of Topic Administration

9.1.3 Broker Communication Process

9.1.4 Topic Lifecycle Management

9.1.5 Common Administrative Operations

9.1.6 Best Practices

### 9.1.1 create, list, describe, alter, delete

9.1.1.1 Creating Topics

9.1.1.2 Required Topic Parameters

9.1.1.3 Listing Topics

9.1.1.4 Describing Topics

9.1.1.5 Viewing Topic Configurations

9.1.1.6 Altering Topic Settings

9.1.1.7 Deleting Topics

9.1.1.8 Common Administrative Commands

9.1.1.9 Operational Best Practices

### 9.1.2 Increase Partitions

9.1.2.1 Why Increase Partitions?

9.1.2.2 Partition Expansion Process

9.1.2.3 Effects on Existing Data

9.1.2.4 Effects on New Data

9.1.2.5 Key Hashing Implications

9.1.2.6 Ordering Considerations

9.1.2.7 Consumer Group Impact

9.1.2.8 Best Practices

### 9.1.3 Update Config (`min.insync.replicas`, `retention`, etc.)

9.1.3.1 Topic-Level Configuration Updates

9.1.3.2 Updating `min.insync.replicas`

9.1.3.3 Updating Retention Policies

9.1.3.4 Updating Cleanup Policies

9.1.3.5 Updating Compression Settings

9.1.3.6 Dynamic vs Static Configurations

9.1.3.7 Configuration Validation

9.1.3.8 Best Practices

## 9.2 Partition Reassignment (`kafka-reassign-partitions.sh`)

9.2.1 What is Partition Reassignment?

9.2.2 Why Reassignment is Needed

9.2.3 Broker Expansion Scenarios

9.2.4 Load Balancing Scenarios

9.2.5 Reassignment Workflow

9.2.6 Reassignment JSON Structure

9.2.7 Execution Process

9.2.8 Verification Process

9.2.9 Performance Impact

9.2.10 Best Practices

## 9.3 Preferred Replica Election (`kafka-leader-election.sh`)

9.3.1 What is Preferred Replica Election?

9.3.2 Preferred Leader Concept

9.3.3 Why Leader Rebalancing is Needed

9.3.4 Manual Leader Election Process

9.3.5 Automatic Leader Election

9.3.6 Cluster Load Balancing

9.3.7 Operational Considerations

9.3.8 Best Practices

## 9.4 Dump Log Segments (`kafka-dump-log.sh`)

9.4.1 Purpose of `kafka-dump-log.sh`

9.4.2 Inspecting Log Segment Files

9.4.3 Reading Message Contents

9.4.4 Viewing Offsets

9.4.5 Viewing Index Files

9.4.6 Viewing Time Indexes

9.4.7 Troubleshooting Use Cases

9.4.8 Corruption Analysis

9.4.9 Best Practices

## 9.5 Consumer Group CLI (`kafka-consumer-groups.sh`)

9.5.1 What is `kafka-consumer-groups.sh`?

9.5.2 Purpose of Consumer Group Administration

9.5.3 Consumer Group Monitoring

9.5.4 Offset Management

9.5.5 Lag Analysis

9.5.6 Best Practices

### 9.5.1 list, describe, reset offsets, delete

9.5.1.1 Listing Consumer Groups

9.5.1.2 Describing Consumer Groups

9.5.1.3 Viewing Consumer Lag

9.5.1.4 Viewing Partition Assignments

9.5.1.5 Resetting Offsets

9.5.1.6 Earliest Offset Reset

9.5.1.7 Latest Offset Reset

9.5.1.8 Specific Offset Reset

9.5.1.9 Deleting Consumer Groups

9.5.1.10 Administrative Best Practices

## 9.6 Quotas

9.6.1 What are Kafka Quotas?

9.6.2 Why Quotas are Needed

9.6.3 Resource Protection

9.6.4 Multi-Tenant Clusters

9.6.5 Quota Enforcement Mechanism

9.6.6 Dynamic Quota Updates

9.6.7 Monitoring Quotas

9.6.8 Best Practices

### 9.6.1 Produce Quota

9.6.1.1 Purpose of Produce Quotas

9.6.1.2 Producer Throughput Limits

9.6.1.3 Bandwidth Control

9.6.1.4 Throttling Behavior

9.6.1.5 Monitoring Produce Quotas

### 9.6.2 Fetch Quota

9.6.2.1 Purpose of Fetch Quotas

9.6.2.2 Consumer Throughput Limits

9.6.2.3 Broker Resource Protection

9.6.2.4 Throttling Behavior

9.6.2.5 Monitoring Fetch Quotas

### 9.6.3 Request Quota (Controller Mutex)

9.6.3.1 Purpose of Request Quotas

9.6.3.2 Broker Request Limits

9.6.3.3 Controller Resource Protection

9.6.3.4 Request Throttling

9.6.3.5 Operational Considerations

### 9.6.4 Client-ID / User Based

9.6.4.1 Client-Based Quotas

9.6.4.2 User-Based Quotas

9.6.4.3 Authentication Integration

9.6.4.4 Quota Hierarchies

9.6.4.5 Best Practices

## 9.7 Authentication

9.7.1 What is Authentication?

9.7.2 Why Authentication is Required

9.7.3 Authentication Workflow

9.7.4 Broker Authentication Process

9.7.5 Client Authentication Process

9.7.6 Security Considerations

9.7.7 Best Practices

### 9.7.1 SSL / mTLS

9.7.1.1 What is SSL/TLS?

9.7.1.2 Purpose of Encryption

9.7.1.3 SSL Handshake Process

9.7.1.4 Certificate Management

9.7.1.5 What is Mutual TLS (mTLS)?

9.7.1.6 Client Certificate Authentication

9.7.1.7 Security Benefits

9.7.1.8 Operational Challenges

### 9.7.2 SASL (PLAIN, SCRAM, GSSAPI (Kerberos), OAUTHBEARER)

9.7.2.1 What is SASL?

9.7.2.2 SASL Authentication Workflow

9.7.2.3 SASL/PLAIN

9.7.2.4 SASL/SCRAM

9.7.2.5 SASL/GSSAPI (Kerberos)

9.7.2.6 SASL/OAUTHBEARER

9.7.2.7 Comparison of SASL Mechanisms

9.7.2.8 Best Practices

## 9.8 Authorization

9.8.1 What is Authorization?

9.8.2 Authentication vs Authorization

9.8.3 Authorization Workflow

9.8.4 Access Control Enforcement

9.8.5 Security Best Practices

### 9.8.1 ACLs (Simple, Wildcard)

9.8.1.1 What are ACLs?

9.8.1.2 ACL Components

9.8.1.3 Simple ACL Rules

9.8.1.4 Wildcard ACL Rules

9.8.1.5 Allow vs Deny Permissions

9.8.1.6 ACL Evaluation Process

9.8.1.7 Common Use Cases

9.8.1.8 Best Practices

### 9.8.2 Super Users

9.8.2.1 What are Super Users?

9.8.2.2 Configuration of Super Users

9.8.2.3 Administrative Privileges

9.8.2.4 Security Risks

9.8.2.5 Best Practices

### 9.8.3 Resource Types (Topic, Group, Cluster, TransactionalId)

9.8.3.1 Topic Resources

9.8.3.2 Consumer Group Resources

9.8.3.3 Cluster Resources

9.8.3.4 TransactionalId Resources

9.8.3.5 ACL Mapping to Resources

9.8.3.6 Security Design Considerations

## 9.9 Monitoring (Metrics & JMX)

9.9.1 Why Kafka Monitoring is Important

9.9.2 JMX Metrics Architecture

9.9.3 Monitoring Tools

9.9.4 Prometheus Integration

9.9.5 Grafana Dashboards

9.9.6 Alerting Strategies

9.9.7 Capacity Planning

9.9.8 Best Practices

### 9.9.1 Under-Replicated Partitions

9.9.1.1 What are Under-Replicated Partitions?

9.9.1.2 Causes of Under-Replication

9.9.1.3 Risks

9.9.1.4 Monitoring Strategies

9.9.1.5 Troubleshooting

### 9.9.2 Offline Partitions

9.9.2.1 What are Offline Partitions?

9.9.2.2 Common Causes

9.9.2.3 Impact on Availability

9.9.2.4 Monitoring Strategies

9.9.2.5 Recovery Procedures

### 9.9.3 ISR Shrink/Expand Rate

9.9.3.1 What is ISR Shrink?

9.9.3.2 What is ISR Expansion?

9.9.3.3 Causes of ISR Changes

9.9.3.4 Monitoring Stability

9.9.3.5 Operational Significance

### 9.9.4 Request Handler Avg Idle %

9.9.4.1 Purpose of the Metric

9.9.4.2 Interpreting Idle Percentage

9.9.4.3 Capacity Analysis

9.9.4.4 Performance Troubleshooting

### 9.9.5 Network Processor Avg Idle %

9.9.5.1 Purpose of the Metric

9.9.5.2 Network Saturation Detection

9.9.5.3 Capacity Planning

9.9.5.4 Troubleshooting

### 9.9.6 Consumer Lag by Group/Topic

9.9.6.1 Lag Calculation

9.9.6.2 Group-Level Monitoring

9.9.6.3 Topic-Level Monitoring

9.9.6.4 Alerting Strategies

9.9.6.5 Lag Troubleshooting

## 9.10 Logging & Debugging

9.10.1 Importance of Kafka Logs

9.10.2 Logging Architecture

9.10.3 Troubleshooting Workflow

9.10.4 Debugging Strategies

9.10.5 Best Practices

### 9.10.1 Broker Log Levels

9.10.1.1 ERROR Level

9.10.1.2 WARN Level

9.10.1.3 INFO Level

9.10.1.4 DEBUG Level

9.10.1.5 TRACE Level

9.10.1.6 Dynamic Log Level Changes

### 9.10.2 Slow Consumer Detection

9.10.2.1 What is a Slow Consumer?

9.10.2.2 Symptoms of Slow Consumption

9.10.2.3 Lag-Based Detection

9.10.2.4 Throughput Analysis

9.10.2.5 Troubleshooting Techniques

9.10.2.6 Mitigation Strategies

### 9.10.3 Fetcher Lag

9.10.3.1 What is Fetcher Lag?

9.10.3.2 Replica Fetching Process

9.10.3.3 Causes of Fetcher Lag

9.10.3.4 Impact on Replication

9.10.3.5 Monitoring Fetcher Lag

9.10.3.6 Troubleshooting and Recovery

# 10. KAFKA CONNECT

## 10.1 Connect Architecture (Workers, Tasks, Connectors)

10.1.1 What is Kafka Connect?

10.1.2 Purpose of Kafka Connect

10.1.3 Connect Architecture Overview

10.1.4 Components of Kafka Connect

10.1.5 Connect Runtime Model

10.1.6 Data Integration Workflow

10.1.7 Scalability Features

10.1.8 Fault Tolerance Features

10.1.9 Benefits of Kafka Connect

10.1.10 Common Use Cases

### 10.1.1 Workers

10.1.1.1 What are Connect Workers?

10.1.1.2 Worker Responsibilities

10.1.1.3 Worker Configuration

10.1.1.4 Worker Coordination

10.1.1.5 Fault Tolerance

10.1.1.6 Scaling Workers

### 10.1.2 Tasks

10.1.2.1 What are Tasks?

10.1.2.2 Purpose of Tasks

10.1.2.3 Task Parallelism

10.1.2.4 Task Assignment

10.1.2.5 Task Lifecycle

10.1.2.6 Performance Considerations

### 10.1.3 Connectors

10.1.3.1 What are Connectors?

10.1.3.2 Connector Responsibilities

10.1.3.3 Connector Configuration

10.1.3.4 Source vs Sink Connectors

10.1.3.5 Connector Lifecycle

10.1.3.6 Connector Plugins

## 10.2 Source Connectors (Read from External)

10.2.1 What are Source Connectors?

10.2.2 Source Connector Architecture

10.2.3 Data Ingestion Workflow

10.2.4 Reading from External Systems

10.2.5 Offset Tracking

10.2.6 Reliability Guarantees

10.2.7 Scalability Considerations

10.2.8 Common Source Connectors

10.2.9 Real-World Use Cases

10.2.10 Best Practices

## 10.3 Sink Connectors (Write to External)

10.3.1 What are Sink Connectors?

10.3.2 Sink Connector Architecture

10.3.3 Data Export Workflow

10.3.4 Writing to External Systems

10.3.5 Delivery Guarantees

10.3.6 Error Handling

10.3.7 Scalability Considerations

10.3.8 Common Sink Connectors

10.3.9 Real-World Use Cases

10.3.10 Best Practices

## 10.4 Converters (Avro, JSON, Parquet, Protobuf)

10.4.1 What are Converters?

10.4.2 Purpose of Converters

10.4.3 Internal Data Format in Kafka Connect

10.4.4 Serialization and Deserialization Workflow

10.4.5 Schema Handling

10.4.6 Converter Configuration

10.4.7 Performance Considerations

10.4.8 Best Practices

### 10.4.1 Avro Converter

10.4.1.1 What is Avro?

10.4.1.2 Avro Schema Structure

10.4.1.3 Schema Registry Integration

10.4.1.4 Schema Evolution

10.4.1.5 Advantages

10.4.1.6 Use Cases

### 10.4.2 JSON Converter

10.4.2.1 What is JSON Conversion?

10.4.2.2 Schema-Based JSON

10.4.2.3 Schema-Less JSON

10.4.2.4 Advantages

10.4.2.5 Limitations

10.4.2.6 Use Cases

### 10.4.3 Parquet Converter

10.4.3.1 What is Parquet?

10.4.3.2 Columnar Storage Concepts

10.4.3.3 Storage Optimization

10.4.3.4 Analytics Benefits

10.4.3.5 Advantages

10.4.3.6 Use Cases

### 10.4.4 Protobuf Converter

10.4.4.1 What is Protobuf?

10.4.4.2 Schema Definition

10.4.4.3 Schema Registry Integration

10.4.4.4 Serialization Efficiency

10.4.4.5 Advantages

10.4.4.6 Use Cases

## 10.5 Transforms (Single Message Transforms)

10.5.1 What are Single Message Transforms (SMTs)?

10.5.2 Purpose of SMTs

10.5.3 Message Transformation Workflow

10.5.4 Source-Side Transformations

10.5.5 Sink-Side Transformations

10.5.6 Transformation Chaining

10.5.7 Performance Considerations

10.5.8 Best Practices

10.5.9 Common SMT Examples

10.5.10 Real-World Use Cases

## 10.6 Dead Letter Queue (DLQ)

10.6.1 What is a Dead Letter Queue?

10.6.2 Purpose of DLQ

10.6.3 Error Handling Workflow

10.6.4 Failed Record Processing

10.6.5 DLQ Configuration

10.6.6 Error Metadata Storage

10.6.7 Retry vs DLQ

10.6.8 Monitoring DLQ

10.6.9 Common Use Cases

10.6.10 Best Practices

## 10.7 Distributed vs Standalone Mode

10.7.1 What is Standalone Mode?

10.7.2 Standalone Architecture

10.7.3 Standalone Configuration

10.7.4 Advantages of Standalone Mode

10.7.5 Limitations of Standalone Mode

10.7.6 Use Cases

10.7.7 What is Distributed Mode?

10.7.8 Distributed Architecture

10.7.9 Distributed Configuration

10.7.10 Advantages of Distributed Mode

10.7.11 Fault Tolerance Benefits

10.7.12 Scalability Benefits

10.7.13 Use Cases

10.7.14 Distributed vs Standalone Comparison

10.7.15 Best Practices

## 10.8 Rebalancing in Connect Workers

10.8.1 What is Connect Rebalancing?

10.8.2 Why Rebalancing Occurs

10.8.3 Worker Join Events

10.8.4 Worker Leave Events

10.8.5 Connector Reassignment

10.8.6 Task Reassignment

10.8.7 Incremental Cooperative Rebalancing

10.8.8 Rebalance Workflow

10.8.9 Performance Impact

10.8.10 Monitoring Rebalances

10.8.11 Best Practices

## 10.9 Offset Management in Connect

10.9.1 Purpose of Offset Management

10.9.2 Source Connector Offsets

10.9.3 Sink Connector Offsets

10.9.4 Offset Storage Mechanism

10.9.5 Offset Commit Process

10.9.6 Offset Recovery

10.9.7 Fault Tolerance Benefits

10.9.8 Offset Topics in Kafka Connect

10.9.9 Exactly-Once Considerations

10.9.10 Monitoring Offsets

10.9.11 Common Offset Issues

10.9.12 Best Practices

# 11. KAFKA STREAMS

## 11.1 Streams Architecture (Embedded Library)

11.1.1 What is Kafka Streams?

11.1.2 Purpose of Kafka Streams

11.1.3 Embedded Library Architecture

11.1.4 Kafka Streams Components

11.1.5 Stream Processing Workflow

11.1.6 Stream Topology Concept

11.1.7 Streams vs Traditional Consumers

11.1.8 Streams vs Kafka Connect

11.1.9 Scalability Features

11.1.10 Fault Tolerance Features

11.1.11 Benefits of Kafka Streams

11.1.12 Real-World Use Cases

## 11.2 KStream vs KTable vs GlobalKTable

11.2.1 What is a KStream?

11.2.2 Characteristics of KStream

11.2.3 KStream Processing Model

11.2.4 Use Cases for KStream

11.2.5 What is a KTable?

11.2.6 Characteristics of KTable

11.2.7 KTable Update Semantics

11.2.8 Use Cases for KTable

11.2.9 What is a GlobalKTable?

11.2.10 Characteristics of GlobalKTable

11.2.11 Replication Behavior of GlobalKTable

11.2.12 Use Cases for GlobalKTable

11.2.13 KStream vs KTable Comparison

11.2.14 KTable vs GlobalKTable Comparison

11.2.15 Choosing the Right Abstraction

## 11.3 State Stores

11.3.1 What are State Stores?

11.3.2 Purpose of State Stores

11.3.3 Stateful Processing Architecture

11.3.4 Local State Management

11.3.5 Changelog Topics

11.3.6 State Recovery Process

11.3.7 Fault Tolerance

11.3.8 Performance Considerations

11.3.9 Monitoring State Stores

11.3.10 Best Practices

### 11.3.1 RocksDB

11.3.1.1 What is RocksDB?

11.3.1.2 Why Kafka Streams Uses RocksDB

11.3.1.3 Persistent Storage Model

11.3.1.4 State Recovery Process

11.3.1.5 Performance Characteristics

11.3.1.6 Advantages

11.3.1.7 Limitations

11.3.1.8 Use Cases

### 11.3.2 In-Memory

11.3.2.1 What is an In-Memory State Store?

11.3.2.2 Memory-Based Processing

11.3.2.3 Performance Benefits

11.3.2.4 Recovery Considerations

11.3.2.5 Advantages

11.3.2.6 Limitations

11.3.2.7 Use Cases

### 11.3.3 Custom

11.3.3.1 What are Custom State Stores?

11.3.3.2 Custom Store Architecture

11.3.3.3 Integration Requirements

11.3.3.4 Performance Considerations

11.3.3.5 Use Cases

11.3.3.6 Best Practices

## 11.4 Stream-Table Duality

11.4.1 What is Stream-Table Duality?

11.4.2 Streams as Change Logs

11.4.3 Tables as Materialized State

11.4.4 Converting Streams to Tables

11.4.5 Converting Tables to Streams

11.4.6 KStream to KTable Transformation

11.4.7 KTable to KStream Transformation

11.4.8 Real-World Examples

11.4.9 Benefits of Stream-Table Duality

11.4.10 Use Cases

## 11.5 Operations

11.5.1 Kafka Streams Processing Operations

11.5.2 Stateless vs Stateful Operations

11.5.3 Transformation Pipelines

11.5.4 Topology Construction

11.5.5 Performance Considerations

11.5.6 Best Practices

### 11.5.1 Stateless (map, filter, flatMap, groupBy, branch, split, merge)

11.5.1.1 What are Stateless Operations?

11.5.1.2 Characteristics of Stateless Processing

11.5.1.3 `map()`

11.5.1.4 `filter()`

11.5.1.5 `flatMap()`

11.5.1.6 `groupBy()`

11.5.1.7 `branch()`

11.5.1.8 `split()`

11.5.1.9 `merge()`

11.5.1.10 Performance Benefits

11.5.1.11 Common Use Cases

### 11.5.2 Stateful (aggregate, reduce, count, windowed joins, session windows)

11.5.2.1 What are Stateful Operations?

11.5.2.2 Characteristics of Stateful Processing

11.5.2.3 `aggregate()`

11.5.2.4 `reduce()`

11.5.2.5 `count()`

11.5.2.6 Windowed Joins

11.5.2.7 Session Windows

11.5.2.8 State Store Dependency

11.5.2.9 Fault Tolerance Considerations

11.5.2.10 Common Use Cases

## 11.6 Windowing

11.6.1 What is Windowing?

11.6.2 Why Windowing is Needed

11.6.3 Event-Time Processing

11.6.4 Processing-Time vs Event-Time

11.6.5 Window Lifecycle

11.6.6 Late Arriving Data

11.6.7 Window Retention

11.6.8 Window Store Management

11.6.9 Best Practices

### 11.6.1 Tumbling Windows

11.6.1.1 What are Tumbling Windows?

11.6.1.2 Fixed-Size Window Concept

11.6.1.3 Non-Overlapping Windows

11.6.1.4 Processing Workflow

11.6.1.5 Use Cases

11.6.1.6 Advantages and Limitations

### 11.6.2 Hopping Windows

11.6.2.1 What are Hopping Windows?

11.6.2.2 Window Size vs Advance Interval

11.6.2.3 Overlapping Windows

11.6.2.4 Processing Workflow

11.6.2.5 Use Cases

11.6.2.6 Advantages and Limitations

### 11.6.3 Session Windows

11.6.3.1 What are Session Windows?

11.6.3.2 Activity Gap Concept

11.6.3.3 Dynamic Window Sizes

11.6.3.4 Session Merging

11.6.3.5 Processing Workflow

11.6.3.6 Use Cases

11.6.3.7 Advantages and Limitations

### 11.6.4 Sliding Windows (v2.8+)

11.6.4.1 What are Sliding Windows?

11.6.4.2 Continuous Evaluation Concept

11.6.4.3 Window Movement Behavior

11.6.4.4 Processing Workflow

11.6.4.5 Use Cases

11.6.4.6 Advantages and Limitations

### 11.6.5 Grace Period

11.6.5.1 What is a Grace Period?

11.6.5.2 Purpose of Grace Periods

11.6.5.3 Handling Late Arriving Events

11.6.5.4 Window Closing Behavior

11.6.5.5 Configuration Considerations

11.6.5.6 Best Practices

## 11.7 Exactly-Once in Streams

11.7.1 What is Exactly-Once Processing?

11.7.2 Why Exactly-Once is Important in Streams

11.7.3 Streams Processing Guarantees

11.7.4 Idempotent Producers in Streams

11.7.5 Transactions in Streams

11.7.6 Atomic Read-Process-Write Cycle

11.7.7 Offset Commit Integration

11.7.8 Changelog Consistency

11.7.9 State Store Consistency

11.7.10 Failure Recovery

11.7.11 EOS v1 vs EOS v2

11.7.12 Performance Trade-Offs

11.7.13 Best Practices

## 11.8 Interactive Queries (Queryable State Stores)

11.8.1 What are Interactive Queries?

11.8.2 Purpose of Queryable State Stores

11.8.3 Local State Access

11.8.4 Remote State Access

11.8.5 Query Routing

11.8.6 State Store Lookup Process

11.8.7 Metadata Discovery

11.8.8 High Availability Considerations

11.8.9 Performance Benefits

11.8.10 Common Use Cases

11.8.11 Best Practices

## 11.9 Streams Rebalancing & Scaling

11.9.1 Why Rebalancing Occurs

11.9.2 Stream Thread Architecture

11.9.3 Task Assignment

11.9.4 Active Tasks

11.9.5 Standby Tasks

11.9.6 Rebalance Workflow

11.9.7 State Store Migration

11.9.8 State Restoration Process

11.9.9 Scaling Out Streams Applications

11.9.10 Scaling In Streams Applications

11.9.11 Cooperative Rebalancing

11.9.12 High Availability Considerations

11.9.13 Performance Impact

11.9.14 Monitoring Rebalances

11.9.15 Best Practices

# 12. KSQLDB / KSQL

## 12.1 KSQL Architecture

12.1.1 What is ksqlDB?

12.1.2 Purpose of ksqlDB

12.1.3 Event Streaming with SQL

12.1.4 ksqlDB Architecture Overview

12.1.5 Relationship with Kafka Streams

12.1.6 ksqlDB Server Components

12.1.7 Query Processing Workflow

12.1.8 Persistent vs Transient Queries

12.1.9 Scalability Features

12.1.10 Fault Tolerance Features

12.1.11 Advantages of ksqlDB

12.1.12 Real-World Use Cases

## 12.2 Streams vs Tables in SQL

12.2.1 What is a Stream in ksqlDB?

12.2.2 Characteristics of Streams

12.2.3 Append-Only Data Model

12.2.4 Stream Processing Semantics

12.2.5 Common Stream Use Cases

12.2.6 What is a Table in ksqlDB?

12.2.7 Characteristics of Tables

12.2.8 Upsert Data Model

12.2.9 Table Processing Semantics

12.2.10 Common Table Use Cases

12.2.11 Stream-to-Table Conversion

12.2.12 Table-to-Stream Conversion

12.2.13 Stream vs Table Comparison

12.2.14 Choosing Between Streams and Tables

12.2.15 Real-World Examples

## 12.3 Pull vs Push Queries

12.3.1 What are Query Types in ksqlDB?

12.3.2 Purpose of Pull Queries

12.3.3 Purpose of Push Queries

12.3.4 Pull Query Architecture

12.3.5 Push Query Architecture

12.3.6 Continuous Query Processing

12.3.7 One-Time Query Processing

12.3.8 Query Execution Workflow

12.3.9 Performance Considerations

12.3.10 Pull vs Push Query Comparison

12.3.11 Common Use Cases

12.3.12 Best Practices

## 12.4 Materialized Views

12.4.1 What are Materialized Views?

12.4.2 Purpose of Materialized Views

12.4.3 Continuous Aggregation Concept

12.4.4 Materialization Process

12.4.5 Backing State Stores

12.4.6 Incremental Updates

12.4.7 Querying Materialized Views

12.4.8 Fault Tolerance

12.4.9 Performance Benefits

12.4.10 Storage Considerations

12.4.11 Common Use Cases

12.4.12 Best Practices

## 12.5 UDFs / UDAFs

12.5.1 What are User Defined Functions (UDFs)?

12.5.2 Purpose of UDFs

12.5.3 UDF Execution Model

12.5.4 Creating Custom UDFs

12.5.5 UDF Lifecycle

12.5.6 Common UDF Use Cases

12.5.7 Best Practices for UDFs

### 12.5.1 UDFs (User Defined Functions)

12.5.1.1 What is a UDF?

12.5.1.2 Row-Level Transformations

12.5.1.3 Input and Output Processing

12.5.1.4 Stateless Function Behavior

12.5.1.5 Common Examples

12.5.1.6 Performance Considerations

12.5.1.7 Best Practices

### 12.5.2 UDAFs (User Defined Aggregate Functions)

12.5.2.1 What is a UDAF?

12.5.2.2 Aggregate Processing Model

12.5.2.3 Stateful Aggregation

12.5.2.4 Grouped Data Processing

12.5.2.5 Custom Aggregate Logic

12.5.2.6 Common Examples

12.5.2.7 Performance Considerations

12.5.2.8 Best Practices

### 12.5.3 UDF vs UDAF Comparison

12.5.3.1 Processing Differences

12.5.3.2 Stateful vs Stateless Behavior

12.5.3.3 Use Case Comparison

12.5.3.4 Performance Comparison

12.5.3.5 When to Use Each

## 12.6 Exactly-Once in KSQL

12.6.1 What is Exactly-Once Processing?

12.6.2 Why Exactly-Once is Important in ksqlDB

12.6.3 Processing Guarantees in ksqlDB

12.6.4 Kafka Streams Foundation

12.6.5 Idempotent Producers

12.6.6 Transactions in ksqlDB

12.6.7 Atomic Read-Process-Write Operations

12.6.8 Offset Management

12.6.9 State Store Consistency

12.6.10 Materialized View Consistency

12.6.11 Failure Recovery

12.6.12 Performance Trade-Offs

12.6.13 Configuration Considerations

12.6.14 Best Practices

12.6.15 Common Limitations

# 13. PERFORMANCE TUNING

## 13.1 Producer Tuning

13.1.1 Why Producer Tuning is Important

13.1.2 Producer Performance Metrics

13.1.3 Throughput vs Latency Trade-Off

13.1.4 Producer Bottlenecks

13.1.5 Tuning Methodology

13.1.6 Benchmarking Approaches

13.1.7 Monitoring Producer Performance

13.1.8 Best Practices

### 13.1.1 Batching Optimization

13.1.1.1 `batch.size`

13.1.1.2 `linger.ms`

13.1.1.3 Batch Efficiency

13.1.1.4 Throughput Impact

13.1.1.5 Latency Trade-Offs

### 13.1.2 Reliability vs Performance

13.1.2.1 `acks=0`

13.1.2.2 `acks=1`

13.1.2.3 `acks=all`

13.1.2.4 Performance Comparison

13.1.2.5 Reliability Comparison

### 13.1.3 Retry Tuning

13.1.3.1 `retries`

13.1.3.2 `retry.backoff.ms`

13.1.3.3 `delivery.timeout.ms`

13.1.3.4 Failure Handling Strategies

### 13.1.4 Memory Tuning

13.1.4.1 `buffer.memory`

13.1.4.2 `max.block.ms`

13.1.4.3 Memory Pressure Handling

13.1.4.4 Buffer Utilization

### 13.1.5 Compression Tuning

13.1.5.1 Compression Benefits

13.1.5.2 Compression Cost

13.1.5.3 Compression Algorithm Selection

13.1.5.4 Network Savings

## 13.2 Consumer Tuning

13.2.1 Why Consumer Tuning is Important

13.2.2 Consumer Throughput Optimization

13.2.3 Consumer Latency Optimization

13.2.4 Consumer Bottlenecks

13.2.5 Scaling Consumer Workloads

13.2.6 Monitoring Consumer Performance

13.2.7 Lag-Based Tuning

13.2.8 Best Practices

### 13.2.1 Fetch Tuning

13.2.1.1 `fetch.min.bytes`

13.2.1.2 `fetch.max.wait.ms`

13.2.1.3 Fetch Efficiency

13.2.1.4 Throughput Impact

### 13.2.2 Poll Tuning

13.2.2.1 `max.poll.records`

13.2.2.2 `max.poll.interval.ms`

13.2.2.3 Poll Loop Optimization

13.2.2.4 Processing Time Considerations

### 13.2.3 Consumer Group Scaling

13.2.3.1 Partition-to-Consumer Ratio

13.2.3.2 Rebalance Minimization

13.2.3.3 Static Membership

13.2.3.4 Parallel Processing Strategies

### 13.2.4 Offset Commit Optimization

13.2.4.1 Auto Commit Performance

13.2.4.2 Manual Commit Performance

13.2.4.3 Commit Frequency Trade-Offs

13.2.4.4 Reliability Considerations

## 13.3 Broker Tuning

13.3.1 Why Broker Tuning is Important

13.3.2 Broker Performance Architecture

13.3.3 CPU Optimization

13.3.4 Memory Optimization

13.3.5 Network Optimization

13.3.6 Disk Optimization

13.3.7 JVM Optimization

13.3.8 Monitoring Broker Performance

13.3.9 Best Practices

### 13.3.1 OS Page Cache

13.3.1.1 What is OS Page Cache?

13.3.1.2 Kafka's Dependence on Page Cache

13.3.1.3 Read Performance Benefits

13.3.1.4 Write Performance Benefits

13.3.1.5 Why Kafka Prefers OS Cache Over JVM Heap

13.3.1.6 Memory Allocation Strategies

13.3.1.7 Monitoring Page Cache Usage

13.3.1.8 Best Practices

### 13.3.2 Filesystem (XFS)

13.3.2.1 Recommended Filesystems for Kafka

13.3.2.2 Why XFS is Preferred

13.3.2.3 XFS Performance Characteristics

13.3.2.4 Sequential I/O Benefits

13.3.2.5 XFS vs EXT4

13.3.2.6 Deployment Considerations

13.3.2.7 Best Practices

### 13.3.3 Heap Size (6-8GB Max)

13.3.3.1 Why Heap Size Matters

13.3.3.2 Recommended Heap Sizing

13.3.3.3 Why Large Heaps are Discouraged

13.3.3.4 Garbage Collection Impact

13.3.3.5 Heap vs Page Cache Trade-Off

13.3.3.6 JVM Tuning Considerations

13.3.3.7 Monitoring Heap Usage

13.3.3.8 Best Practices

### 13.3.4 Log Flush Policies (Don't)

13.3.4.1 What are Log Flush Policies?

13.3.4.2 Default Kafka Write Strategy

13.3.4.3 OS-Controlled Flushing

13.3.4.4 Forced Flush Configuration

13.3.4.5 Performance Impact of Forced Flushes

13.3.4.6 Durability Considerations

13.3.4.7 Why Forced Flushing is Generally Avoided

13.3.4.8 Best Practices

## 13.4 Network & Disk Bandwidth

13.4.1 Why Bandwidth Matters

13.4.2 Network Throughput Requirements

13.4.3 Disk Throughput Requirements

13.4.4 Sequential vs Random I/O

13.4.5 Broker Network Traffic Patterns

13.4.6 Replication Traffic

13.4.7 Consumer Fetch Traffic

13.4.8 Monitoring Network Utilization

13.4.9 Monitoring Disk Utilization

13.4.10 Common Bottlenecks

13.4.11 Capacity Planning

13.4.12 Best Practices

## 13.5 Partition Count vs Throughput

13.5.1 Relationship Between Partitions and Throughput

13.5.2 Producer Parallelism

13.5.3 Consumer Parallelism

13.5.4 Broker Parallelism

13.5.5 Scaling Through Partitioning

13.5.6 Too Few Partitions

13.5.7 Too Many Partitions

13.5.8 Metadata Overhead

13.5.9 Rebalance Impact

13.5.10 Capacity Planning Considerations

13.5.11 Real-World Sizing Strategies

13.5.12 Best Practices

## 13.6 Compression Trade-Offs

13.6.1 Why Compression is Used

13.6.2 Compression Workflow

13.6.3 Network Savings

13.6.4 Storage Savings

13.6.5 CPU Cost of Compression

13.6.6 CPU Cost of Decompression

13.6.7 Throughput Impact

13.6.8 Latency Impact

13.6.9 Compression Selection Criteria

13.6.10 Monitoring Compression Efficiency

13.6.11 Best Practices

### 13.6.1 GZIP

13.6.1.1 Compression Ratio

13.6.1.2 CPU Utilization

13.6.1.3 Throughput Characteristics

13.6.1.4 Common Use Cases

### 13.6.2 Snappy

13.6.2.1 Compression Ratio

13.6.2.2 CPU Utilization

13.6.2.3 Throughput Characteristics

13.6.2.4 Common Use Cases

### 13.6.3 LZ4

13.6.3.1 Compression Ratio

13.6.3.2 CPU Utilization

13.6.3.3 Throughput Characteristics

13.6.3.4 Common Use Cases

### 13.6.4 ZSTD

13.6.4.1 Compression Ratio

13.6.4.2 CPU Utilization

13.6.4.3 Throughput Characteristics

13.6.4.4 Common Use Cases

### 13.6.5 Compression Algorithm Comparison

13.6.5.1 Compression Ratio Comparison

13.6.5.2 CPU Usage Comparison

13.6.5.3 Throughput Comparison

13.6.5.4 Latency Comparison

13.6.5.5 Selection Guidelines

## 13.7 Replication Throttling for Migration

13.7.1 What is Replication Throttling?

13.7.2 Why Throttling is Needed During Migration

13.7.3 Broker Expansion Scenarios

13.7.4 Partition Reassignment Scenarios

13.7.5 Replication Traffic Management

13.7.6 Throttle Configuration

13.7.7 Leader Movement Considerations

13.7.8 Impact on Production Workloads

13.7.9 Monitoring Throttled Replication

13.7.10 Removing Throttles After Migration

13.7.11 Common Operational Mistakes

13.7.12 Best Practices

# 14. COMPARISON & CONCEPTUAL

## 14.1 Kafka vs RabbitMQ

14.1.1 Introduction to Kafka

14.1.2 Introduction to RabbitMQ

14.1.3 Architecture Comparison

14.1.4 Messaging Model Comparison

14.1.5 Pull vs Push Consumption

14.1.6 Throughput Comparison

14.1.7 Latency Comparison

14.1.8 Message Ordering Comparison

14.1.9 Message Retention Comparison

14.1.10 Scalability Comparison

14.1.11 Fault Tolerance Comparison

14.1.12 Consumer Model Comparison

14.1.13 Replay Capability Comparison

14.1.14 Operational Complexity Comparison

14.1.15 Real-World Use Cases

14.1.16 When to Choose Kafka

14.1.17 When to Choose RabbitMQ

## 14.2 Kafka vs Pulsar

14.2.1 Introduction to Apache Pulsar

14.2.2 Kafka Architecture Overview

14.2.3 Pulsar Architecture Overview

14.2.4 Storage Architecture Comparison

14.2.5 Broker Design Comparison

14.2.6 Segment Storage vs BookKeeper

14.2.7 Multi-Tenancy Comparison

14.2.8 Topic Management Comparison

14.2.9 Scalability Comparison

14.2.10 Replication Comparison

14.2.11 Geo-Replication Comparison

14.2.12 Consumer Model Comparison

14.2.13 Performance Comparison

14.2.14 Operational Complexity Comparison

14.2.15 Real-World Use Cases

14.2.16 When to Choose Kafka

14.2.17 When to Choose Pulsar

## 14.3 Kafka vs Kinesis

14.3.1 Introduction to Amazon Kinesis

14.3.2 Managed Service vs Self-Managed Platform

14.3.3 Architecture Comparison

14.3.4 Kafka Partitions vs Kinesis Shards

14.3.5 Scalability Comparison

14.3.6 Data Retention Comparison

14.3.7 Consumer Model Comparison

14.3.8 Replay Capability Comparison

14.3.9 Monitoring Comparison

14.3.10 Cost Considerations

14.3.11 AWS Ecosystem Integration

14.3.12 Performance Comparison

14.3.13 Operational Complexity Comparison

14.3.14 Real-World Use Cases

14.3.15 When to Choose Kafka

14.3.16 When to Choose Kinesis

## 14.4 Kafka vs Traditional Message Queues (ActiveMQ, IBM MQ)

14.4.1 What are Traditional Message Queues?

14.4.2 Introduction to ActiveMQ

14.4.3 Introduction to IBM MQ

14.4.4 Queue-Based Architecture

14.4.5 Distributed Log Architecture

14.4.6 Message Storage Comparison

14.4.7 Consumer Model Comparison

14.4.8 Throughput Comparison

14.4.9 Scalability Comparison

14.4.10 Message Replay Comparison

14.4.11 Ordering Guarantees Comparison

14.4.12 Fault Tolerance Comparison

14.4.13 Enterprise Integration Features

14.4.14 Operational Complexity Comparison

14.4.15 Real-World Use Cases

14.4.16 When to Choose Kafka

14.4.17 When to Choose Traditional MQs

## 14.5 Pull vs Push Model (Why Pull Wins)

14.5.1 What is the Push Model?

14.5.2 Push-Based Message Delivery Workflow

14.5.3 Advantages of Push Model

14.5.4 Limitations of Push Model

14.5.5 What is the Pull Model?

14.5.6 Pull-Based Message Delivery Workflow

14.5.7 Advantages of Pull Model

14.5.8 Consumer-Controlled Consumption

14.5.9 Backpressure Handling

14.5.10 Consumer Lag Management

14.5.11 Batch Fetch Optimization

14.5.12 Scalability Benefits

14.5.13 Resource Utilization Comparison

14.5.14 Pull vs Push Comparison

14.5.15 Why Kafka Uses Pull

14.5.16 Real-World Examples

## 14.6 Ordering Guarantees (Partition Level Only)

14.6.1 What is Message Ordering?

14.6.2 Why Ordering Matters

14.6.3 Ordering Within a Partition

14.6.4 Offset-Based Ordering

14.6.5 Leader-Based Ordering

14.6.6 Same Key → Same Partition Principle

14.6.7 Ordering Guarantees for Keyed Messages

14.6.8 Ordering Guarantees for Keyless Messages

14.6.9 Ordering Across Multiple Partitions

14.6.10 Why Global Ordering is Not Guaranteed

14.6.11 Impact of Increasing Partitions

14.6.12 Ordering and Retries

14.6.13 Ordering and Idempotent Producers

14.6.14 Common Misconceptions

14.6.15 Best Practices for Maintaining Ordering

## 14.7 Persistence vs Deletion

14.7.1 What is Persistence in Kafka?

14.7.2 Durable Message Storage

14.7.3 Disk-Based Log Storage

14.7.4 Retention-Based Storage Model

14.7.5 What is Message Deletion?

14.7.6 Time-Based Retention

14.7.7 Size-Based Retention

14.7.8 Log Compaction

14.7.9 Cleanup Policies

14.7.10 Persistence vs Traditional Queue Deletion

14.7.11 Replay Capability

14.7.12 Consumer Independence

14.7.13 Storage Cost Considerations

14.7.14 Real-World Examples

14.7.15 Best Practices

## 14.8 Distributed Commit Log

14.8.1 What is a Commit Log?

14.8.2 Evolution of Commit Logs

14.8.3 Distributed Commit Log Concept

14.8.4 Kafka as a Distributed Commit Log

14.8.5 Append-Only Storage Model

14.8.6 Sequential Disk Writes

14.8.7 Offsets and Log Entries

14.8.8 Partitioned Commit Logs

14.8.9 Replicated Commit Logs

14.8.10 Fault Tolerance Mechanisms

14.8.11 Consumer Read Model

14.8.12 Replay and Event Sourcing

14.8.13 Log Compaction and Commit Logs

14.8.14 Advantages of Distributed Commit Logs

14.8.15 Limitations of Distributed Commit Logs

14.8.16 Real-World Use Cases

14.8.17 Why Kafka is Called a Distributed Commit Log

# 15. MIGRATION & UPGRADE

## 15.1 Rolling Upgrade (Broker by Broker)

15.1.1 What is a Rolling Upgrade?

15.1.2 Purpose of Rolling Upgrades

15.1.3 Zero-Downtime Upgrade Concept

15.1.4 Upgrade Planning Considerations

15.1.5 Pre-Upgrade Validation

15.1.6 Broker-by-Broker Upgrade Workflow

15.1.7 Leader Migration During Upgrades

15.1.8 ISR Stability Considerations

15.1.9 Client Connectivity During Upgrades

15.1.10 Monitoring Upgrade Progress

15.1.11 Common Risks

15.1.12 Rollback Considerations

15.1.13 Best Practices

## 15.2 Upgrade from ZooKeeper to KRaft

15.2.1 Why Migrate from ZooKeeper?

15.2.2 Evolution of Kafka Metadata Management

15.2.3 ZooKeeper Architecture Overview

15.2.4 KRaft Architecture Overview

15.2.5 Benefits of KRaft

15.2.6 Migration Prerequisites

15.2.7 Metadata Migration Process

15.2.8 KRaft Controller Quorum Setup

15.2.9 Hybrid Migration Phase

15.2.10 Migration Validation

15.2.11 Operational Changes After Migration

15.2.12 Common Challenges

15.2.13 Rollback Considerations

15.2.14 Best Practices

## 15.3 Protocol Version Compatibility

15.3.1 What is Protocol Compatibility?

15.3.2 Kafka Client-Broker Communication

15.3.3 API Version Negotiation

15.3.4 Forward Compatibility

15.3.5 Backward Compatibility

15.3.6 Mixed-Version Cluster Support

15.3.7 Client Upgrade Considerations

15.3.8 Broker Upgrade Considerations

15.3.9 Compatibility Testing

15.3.10 Common Compatibility Issues

15.3.11 Best Practices

## 15.4 Inter-Broker Protocol (IBP)

15.4.1 What is Inter-Broker Protocol?

15.4.2 Purpose of IBP

15.4.3 Broker-to-Broker Communication

15.4.4 IBP Version Configuration

15.4.5 Role During Rolling Upgrades

15.4.6 Mixed-Version Cluster Operation

15.4.7 Feature Compatibility Control

15.4.8 Upgrade Workflow Using IBP

15.4.9 Risks of Premature IBP Changes

15.4.10 Monitoring IBP Compatibility

15.4.11 Best Practices

## 15.5 Log Format Version

15.5.1 What is Log Format Version?

15.5.2 Purpose of Log Format Compatibility

15.5.3 Message Storage Format Evolution

15.5.4 Broker Compatibility with Older Logs

15.5.5 Log Format During Upgrades

15.5.6 Relationship Between Log Format and IBP

15.5.7 Feature Availability Based on Log Format

15.5.8 Performance Considerations

15.5.9 Risks of Format Changes

15.5.10 Validation After Upgrades

15.5.11 Best Practices

## 15.6 Downgrade Safety

15.6.1 Why Downgrades are Challenging

15.6.2 Upgrade vs Downgrade Complexity

15.6.3 Compatibility Risks

15.6.4 Metadata Compatibility Issues

15.6.5 Log Format Compatibility Issues

15.6.6 Protocol Compatibility Issues

15.6.7 Feature Dependency Risks

15.6.8 Safe Downgrade Planning

15.6.9 Backup and Recovery Strategies

15.6.10 Testing Downgrade Procedures

15.6.11 Rollback vs Downgrade

15.6.12 Operational Best Practices

15.6.13 Common Mistakes

15.6.14 When Downgrades Should Be Avoided

# 16. COMMON INTERVIEW SCENARIOS (Problem-Solving)

## 16.1 Duplicate Messages (Causes & Prevention)

16.1.1 What are Duplicate Messages?

16.1.2 How Duplicate Messages Occur

16.1.3 Producer Retry Scenarios

16.1.4 Network Failure Scenarios

16.1.5 Acknowledgment Loss Scenarios

16.1.6 Consumer Reprocessing Scenarios

16.1.7 Offset Commit Timing Issues

16.1.8 Duplicate Messages in At-Least-Once Delivery

16.1.9 Impact on Business Applications

16.1.10 Idempotent Producer (`enable.idempotence`)

16.1.11 Transactions for Duplicate Prevention

16.1.12 Consumer-Side Deduplication

16.1.13 Database-Level Deduplication

16.1.14 Monitoring Duplicate Messages

16.1.15 Best Practices

## 16.2 Missing Messages (Acks, Retries, Min.insync)

16.2.1 What are Missing Messages?

16.2.2 Causes of Message Loss

16.2.3 Producer Failure Scenarios

16.2.4 Broker Failure Scenarios

16.2.5 Replica Failure Scenarios

16.2.6 Impact of `acks=0`

16.2.7 Impact of `acks=1`

16.2.8 Impact of `acks=all`

16.2.9 Role of Retries

16.2.10 Role of `min.insync.replicas`

16.2.11 ISR Shrink Scenarios

16.2.12 Unclean Leader Election Risks

16.2.13 Message Durability Strategies

16.2.14 Monitoring Message Loss Risks

16.2.15 Best Practices

## 16.3 Consumer Group Stuck (`max.poll.interval.ms` Too Small)

16.3.1 Symptoms of a Stuck Consumer Group

16.3.2 Understanding `max.poll.interval.ms`

16.3.3 Poll Loop Requirements

16.3.4 Long Processing Time Problems

16.3.5 Consumer Eviction from Group

16.3.6 Rebalance Triggered by Poll Timeout

16.3.7 Offset Commit Implications

16.3.8 Diagnosing Poll Interval Issues

16.3.9 Monitoring Consumer Health

16.3.10 Increasing `max.poll.interval.ms`

16.3.11 Reducing Processing Time

16.3.12 Using Worker Threads

16.3.13 Common Interview Troubleshooting Steps

16.3.14 Best Practices

## 16.4 Rebalancing Too Often (`session.timeout.ms`, `heartbeat.interval.ms`)

16.4.1 Symptoms of Frequent Rebalancing

16.4.2 Why Rebalancing Happens

16.4.3 Consumer Failure Detection

16.4.4 Understanding `session.timeout.ms`

16.4.5 Understanding `heartbeat.interval.ms`

16.4.6 Heartbeat Failure Scenarios

16.4.7 Network Instability Effects

16.4.8 Long Garbage Collection Pauses

16.4.9 Slow Consumer Processing

16.4.10 Static Membership Benefits

16.4.11 Cooperative Rebalancing Benefits

16.4.12 Diagnosing Frequent Rebalances

16.4.13 Monitoring Rebalance Events

16.4.14 Best Practices

## 16.5 High Latency (`linger.ms`, `batch.size`, `fetch.min.bytes`)

16.5.1 What is Kafka Latency?

16.5.2 Producer-Side Latency

16.5.3 Broker-Side Latency

16.5.4 Consumer-Side Latency

16.5.5 Impact of `linger.ms`

16.5.6 Impact of `batch.size`

16.5.7 Impact of `fetch.min.bytes`

16.5.8 Impact of `fetch.max.wait.ms`

16.5.9 Network Latency Considerations

16.5.10 Disk Performance Impact

16.5.11 Replication Impact

16.5.12 Diagnosing Latency Problems

16.5.13 Performance Monitoring Metrics

16.5.14 Tuning Strategies

16.5.15 Best Practices

## 16.6 Disk Full (Retention, Low Capacity)

16.6.1 Symptoms of Disk Full Conditions

16.6.2 Why Kafka Consumes Large Disk Space

16.6.3 Retention Configuration Impact

16.6.4 Time-Based Retention Issues

16.6.5 Size-Based Retention Issues

16.6.6 Log Compaction Considerations

16.6.7 Broker Behavior When Disk is Full

16.6.8 Impact on Producers

16.6.9 Impact on Consumers

16.6.10 Capacity Planning Mistakes

16.6.11 Increasing Storage Capacity

16.6.12 Reducing Retention

16.6.13 Monitoring Disk Utilization

16.6.14 Preventive Measures

16.6.15 Best Practices

## 16.7 Leader Not Replicating (ISR Shrinks, Network Issues)

16.7.1 Symptoms of Replication Problems

16.7.2 Understanding ISR Shrink Events

16.7.3 Follower Replication Workflow

16.7.4 Network Failure Scenarios

16.7.5 Slow Follower Scenarios

16.7.6 Disk Bottleneck Scenarios

16.7.7 Broker Resource Constraints

16.7.8 Replication Lag Analysis

16.7.9 Impact on Availability

16.7.10 Impact on Durability

16.7.11 Diagnosing Replication Problems

16.7.12 Monitoring ISR Metrics

16.7.13 Recovery Procedures

16.7.14 Best Practices

## 16.8 Exactly-Once Across Multiple Topics/Partitions

16.8.1 Problem Statement

16.8.2 Challenges of Multi-Partition Consistency

16.8.3 Challenges of Multi-Topic Consistency

16.8.4 Why At-Least-Once is Insufficient

16.8.5 Idempotent Producers

16.8.6 Kafka Transactions

16.8.7 Transaction Coordinator

16.8.8 Transaction Lifecycle

16.8.9 Atomic Writes Across Partitions

16.8.10 Atomic Writes Across Topics

16.8.11 Offset Commit Integration

16.8.12 Consumer Isolation Levels

16.8.13 `read_committed`

16.8.14 Failure Recovery Scenarios

16.8.15 Common Limitations

16.8.16 Best Practices

## 16.9 Resetting Offsets for Reprocessing

16.9.1 Why Offset Resetting is Needed

16.9.2 Reprocessing Historical Data

16.9.3 Consumer Offset Fundamentals

16.9.4 Earliest Offset Reset

16.9.5 Latest Offset Reset

16.9.6 Reset to Specific Offset

16.9.7 Reset to Specific Timestamp

16.9.8 Resetting Consumer Groups

16.9.9 Using `kafka-consumer-groups.sh`

16.9.10 Impact on Consumers

16.9.11 Duplicate Processing Risks

16.9.12 Validation Before Reset

16.9.13 Monitoring Reprocessing

16.9.14 Best Practices

## 16.10 Designing a Chat System / Event-Driven Pipeline

16.10.1 Understanding Business Requirements

16.10.2 Functional Requirements

16.10.3 Non-Functional Requirements

16.10.4 Topic Design Strategy

16.10.5 Partition Design Strategy

16.10.6 Choosing Partition Keys

16.10.7 Ordering Requirements

16.10.8 Scalability Requirements

16.10.9 Replication Strategy

16.10.10 Consumer Group Design

16.10.11 Message Retention Strategy

16.10.12 Fault Tolerance Requirements

16.10.13 Exactly-Once Considerations

16.10.14 Monitoring and Alerting

16.10.15 Chat Application Architecture Example

16.10.16 Event-Driven Pipeline Architecture Example

16.10.17 Common Design Trade-Offs

16.10.18 Interview Discussion Approach

16.10.19 Best Practices