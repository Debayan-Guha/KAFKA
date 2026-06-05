
# Topics LABS

## Lab 1: Create Production Topics for an E-Commerce Platform

### Objective
Create production-ready topics for an e-commerce platform with proper configuration.

### Tasks
- Create orders-created topic
- Create payment-success topic
- Create inventory-updated topic
- Create shipment-created topic
- Verify topic creation
- Verify partition count
- Verify replication factor
- Verify topic metadata

---

## Lab 2: Implement Enterprise Topic Naming Standards

### Objective
Implement and validate enterprise-wide topic naming conventions.

### Tasks
- Create development environment topics
- Create QA environment topics
- Create production environment topics
- Apply naming convention standards
- Validate topic naming consistency
- Document naming standards

---

## Lab 3: Verify Publish-Subscribe Behavior

### Objective
Validate Kafka's publish-subscribe messaging pattern with multiple consumer groups.

### Tasks
- Create notifications topic
- Start producer
- Create consumer group A
- Create consumer group B
- Publish test messages
- Verify message delivery to consumer group A
- Verify message delivery to consumer group B
- Validate publish-subscribe behavior

---

## Lab 4: Verify Queue-Like Processing Using Consumer Groups

### Objective
Demonstrate queue-like processing behavior using consumer groups.

### Tasks
- Create order-processing topic
- Start producer
- Start consumer instance 1
- Start consumer instance 2
- Configure same consumer group
- Publish multiple messages
- Verify load distribution
- Verify queue-like processing behavior

---

## Lab 5: Enable Topic Auto-Creation and Validate Behavior

### Objective
Enable auto-topic creation and observe its behavior.

### Tasks
- Enable auto topic creation
- Publish data to non-existing topic
- Verify automatic topic creation
- Verify default partition count
- Verify default replication factor
- Review auto-created topic configuration

---

## Lab 6: Disable Topic Auto-Creation

### Objective
Disable auto-topic creation and validate restricted behavior.

### Tasks
- Disable auto topic creation
- Publish data to non-existing topic
- Observe producer failure
- Review broker response
- Analyze error logs
- Validate topic creation restrictions

---

## Lab 7: Audit Automatically Created Topics

### Objective
Audit and identify risks from automatically created topics.

### Tasks
- Generate multiple auto-created topics
- Identify incorrect topic names
- Identify unused topics
- Analyze operational risks
- Prepare cleanup plan
- Document audit findings

---

## Lab 8: Inspect Kafka Internal Topics

### Objective
Inspect and understand Kafka's internal topic structures.

### Tasks
- List internal topics
- Inspect __consumer_offsets
- Inspect __transaction_state
- Verify internal topic partitions
- Verify internal topic replication
- Review internal topic configuration

---

## Lab 9: Observe Offset Storage

### Objective
Observe how Kafka stores consumer offsets internally.

### Tasks
- Create consumer group
- Produce test messages
- Consume messages
- Commit offsets
- Verify offset storage
- Analyze consumer group metadata

---

## Lab 10: Topic Configuration Review

### Objective
Review and audit existing topic configurations.

### Tasks
- Select existing topic
- Review partition configuration
- Review replication configuration
- Review retention configuration
- Review cleanup policy
- Prepare configuration audit report

---

## Lab 11: Topic Deletion Validation

### Objective
Validate topic deletion behavior and lifecycle.

### Tasks
- Create topic
- Publish messages
- Delete topic
- Verify metadata removal
- Verify producer behavior
- Verify consumer behavior
- Analyze topic deletion lifecycle

---

## Lab 12: Recreate Deleted Topic

### Objective
Understand state changes when recreating a deleted topic.

### Tasks
- Delete existing topic
- Recreate topic using same name
- Verify new metadata
- Verify offset reset
- Validate consumer behavior
- Compare old and new topic state

---

## Lab 13: Production Topic Inventory Exercise

### Objective
Design topic strategies for different business domains.

### Tasks
- Design e-commerce topics
- Design banking topics
- Design food delivery topics
- Design chat application topics
- Define partition strategy
- Define replication strategy
- Document topic architecture

---

## Lab 14: Topic Governance Review

### Objective
Perform governance review of existing topics.

### Tasks
- Review existing topics
- Identify unused topics
- Identify misnamed topics
- Identify incorrect configurations
- Recommend improvements
- Prepare governance report

---

## Lab 15: Production Readiness Validation

### Objective
Validate topic production readiness against standards.

### Tasks
- Create new business topic
- Validate naming standards
- Validate partition configuration
- Validate replication configuration
- Validate retention configuration
- Validate documentation requirements
- Prepare production deployment checklist










# PARTITIONS LABS

## Lab 1: Create a Multi-Partition Topic

### Objective
Create and understand multi-partition topics in Kafka.

### Tasks
- Create a topic with multiple partitions
- Verify partition creation
- Inspect partition metadata
- Compare single partition vs multi-partition topics
- Observe message distribution across partitions

---

## Lab 2: Explore Partition Storage

### Objective
Understand how Kafka stores partition data on disk.

### Tasks
- Produce messages to a topic
- Inspect partition data storage
- Verify ordered message storage
- Observe partition log files
- Analyze partition structure

---

## Lab 3: Key-Based Partition Routing

### Objective
Learn how message keys determine partition assignment.

### Tasks
- Create a multi-partition topic
- Produce messages using a customer ID key
- Verify same key routes to same partition
- Verify message ordering within a partition
- Analyze hash-based routing

---

## Lab 4: Keyless Message Distribution

### Objective
Observe partition distribution behavior for messages without keys.

### Tasks
- Produce messages without keys
- Observe partition assignment
- Compare distribution with keyed messages
- Analyze load distribution
- Document findings

---

## Lab 5: Partition Key Design Exercise

### Objective
Design effective partition keys for different business scenarios.

### Tasks
- Design keys for an e-commerce system
- Design keys for a banking system
- Design keys for a food delivery platform
- Design keys for a chat application
- Evaluate ordering requirements
- Evaluate distribution efficiency

---

## Lab 6: Round-Robin Partition Assignment

### Objective
Understand round-robin partition assignment behavior.

### Tasks
- Produce messages using round-robin assignment
- Observe partition selection
- Measure distribution uniformity
- Analyze assignment behavior
- Document results

---

## Lab 7: Sticky Partition Assignment

### Objective
Learn sticky partitioning for batch optimization.

### Tasks
- Enable sticky partitioning
- Produce messages in batches
- Observe partition usage
- Analyze batch optimization
- Compare with round-robin

---

## Lab 8: Uniform Sticky Partition Assignment

### Objective
Optimize load distribution using uniform sticky assignment.

### Tasks
- Configure uniform sticky assignment
- Produce high-volume messages
- Observe load distribution
- Compare with sticky assignment
- Evaluate performance benefits

---

## Lab 9: Partition Count Planning Exercise

### Objective
Design partition strategies based on business requirements.

### Tasks
- Design partition strategy for an e-commerce platform
- Design partition strategy for a banking platform
- Design partition strategy for a chat application
- Estimate consumer parallelism requirements
- Estimate throughput requirements
- Prepare capacity planning report

---

## Lab 10: Consumer Parallelism Using Partitions

### Objective
Scale consumer processing using partitions.

### Tasks
- Create a topic with multiple partitions
- Start multiple consumers
- Assign consumers to a consumer group
- Observe partition allocation
- Verify parallel processing
- Analyze throughput improvement

---

## Lab 11: Consumer Rebalancing - New Consumer Join

### Objective
Observe rebalancing behavior when adding consumers.

### Tasks
- Start consumer group
- Produce continuous messages
- Add a new consumer
- Observe partition reassignment
- Analyze rebalance events
- Verify message processing continuity

---

## Lab 12: Consumer Rebalancing - Consumer Failure

### Objective
Understand rebalancing during consumer failures.

### Tasks
- Start multiple consumers
- Produce continuous messages
- Stop one consumer
- Observe reassignment of partitions
- Verify recovery behavior
- Analyze consumer lag during rebalance

---

## Lab 13: Partition Leadership Inspection

### Objective
Identify and understand partition leader and follower roles.

### Tasks
- Create a replicated topic
- Identify partition leaders
- Identify follower replicas
- Verify leader responsibilities
- Verify follower synchronization
- Document leadership distribution

---

## Lab 14: Leader Failover Simulation

### Objective
Test broker failure and leader election behavior.

### Tasks
- Create a multi-broker cluster
- Identify current leaders
- Stop a leader broker
- Observe leader election
- Verify producer availability
- Verify consumer availability
- Analyze recovery time

---

## Lab 15: Preferred Replica Election

### Objective
Trigger preferred replica election for balanced leadership.

### Tasks
- Identify preferred leaders
- Create leader imbalance
- Trigger preferred leader election
- Verify leader redistribution
- Analyze cluster balance
- Document results

---

## Lab 16: Offset Exploration

### Objective
Understand offset generation and management.

### Tasks
- Produce messages to a partition
- Observe offset generation
- Verify sequential ordering
- Consume messages using offsets
- Compare earliest and latest offsets
- Analyze offset behavior

---

## Lab 17: Consumer Offset Tracking

### Objective
Learn how consumers track their position using offsets.

### Tasks
- Create a consumer group
- Consume messages
- Commit offsets
- Restart consumer
- Verify offset recovery
- Analyze consumer position

---

## Lab 18: Partition Scaling for Throughput

### Objective
Understand partition count impact on consumer throughput.

### Tasks
- Create a topic with few partitions
- Measure consumer throughput
- Increase consumer count
- Observe parallelism limits
- Analyze throughput bottlenecks
- Document findings

---

## Lab 19: Increase Partition Count

### Objective
Add partitions and understand the impact on existing data.

### Tasks
- Create a topic
- Produce keyed messages
- Increase partition count
- Produce additional messages
- Observe data distribution changes
- Analyze ordering impact
- Analyze key hashing changes

---

## Lab 20: Impact of Partition Increase on Consumer Groups

### Objective
Observe consumer group behavior after partition expansion.

### Tasks
- Create consumer group
- Produce messages
- Increase partition count
- Observe rebalancing
- Verify new partition assignments
- Analyze consumer behavior

---

## Lab 21: Partition Migration Strategy

### Objective
Plan and document partition expansion strategies.

### Tasks
- Design a partition expansion plan
- Evaluate existing topic configuration
- Estimate future growth
- Determine new partition count
- Assess operational risks
- Prepare migration documentation

---

## Lab 22: Topic Recreation for Partition Reduction

### Objective
Learn the process for reducing partition count via topic recreation.

### Tasks
- Create topic with excessive partitions
- Design reduced partition layout
- Create replacement topic
- Migrate existing data
- Validate data consistency
- Switch consumers and producers
- Verify migration success













# BROKERS LABS

## Lab 1: Deploy a Single Kafka Broker

### Objective
Deploy and verify a standalone Kafka broker instance.

### Tasks
- Install Kafka broker
- Configure broker settings
- Start broker service
- Verify broker registration
- Verify client connectivity

---

## Lab 2: Build a Multi-Broker Kafka Cluster

### Objective
Create a functional multi-broker Kafka cluster.

### Tasks
- Configure broker 1
- Configure broker 2
- Configure broker 3
- Start cluster
- Verify broker membership
- Validate cluster health

---

## Lab 3: Broker ID Configuration and Validation

### Objective
Understand broker ID assignment and uniqueness requirements.

### Tasks
- Configure manual broker IDs
- Verify broker ID uniqueness
- Inspect cluster metadata
- Validate broker registration
- Test duplicate broker ID scenario

---

## Lab 4: Bootstrap Server Discovery

### Objective
Learn bootstrap server configuration for client connectivity.

### Tasks
- Configure multiple bootstrap servers
- Connect producer using bootstrap servers
- Connect consumer using bootstrap servers
- Verify metadata discovery
- Validate connection failover

---

## Lab 5: Broker Configuration Audit

### Objective
Review and audit critical broker configuration parameters.

### Tasks
- Review server.properties
- Configure network settings
- Configure log directories
- Configure replication settings
- Configure retention policies
- Validate broker configuration

---

## Lab 6: Modify Broker Configuration and Restart

### Objective
Apply configuration changes and validate broker restart behavior.

### Tasks
- Change broker configuration
- Restart broker
- Verify configuration changes
- Validate cluster stability
- Analyze broker logs

---

## Lab 7: Configure Rack Awareness

### Objective
Implement rack awareness for fault-tolerant replica placement.

### Tasks
- Configure rack.id for broker 1
- Configure rack.id for broker 2
- Configure rack.id for broker 3
- Create replicated topics
- Verify replica placement
- Validate rack-aware distribution

---

## Lab 8: Cross-Rack Replication Validation

### Objective
Test fault tolerance across multiple racks.

### Tasks
- Deploy brokers across multiple racks
- Create replicated topic
- Inspect replica distribution
- Simulate rack failure
- Verify data availability
- Analyze fault tolerance

---

## Lab 9: Broker Failure Detection

### Objective
Observe how Kafka detects and responds to broker failures.

### Tasks
- Identify current leaders
- Stop a broker
- Observe failure detection
- Verify controller actions
- Observe cluster recovery
- Analyze client impact

---

## Lab 10: Producer and Consumer Behavior During Broker Failure

### Objective
Understand client behavior during broker failures.

### Tasks
- Start producers
- Start consumers
- Simulate broker failure
- Observe producer behavior
- Observe consumer behavior
- Validate service continuity

---

## Lab 11: Controlled Broker Shutdown

### Objective
Perform graceful broker shutdown with leader migration.

### Tasks
- Enable controlled shutdown
- Identify leader partitions
- Gracefully stop broker
- Observe leader migration
- Verify consumer availability
- Verify producer availability

---

## Lab 12: Uncontrolled Broker Shutdown

### Objective
Compare uncontrolled shutdown with controlled shutdown behavior.

### Tasks
- Identify leader partitions
- Forcefully stop broker
- Observe leader election
- Measure recovery time
- Compare with controlled shutdown
- Document findings

---

## Lab 13: Broker Metadata Inspection (KRaft)

### Objective
Inspect cluster metadata in KRaft mode.

### Tasks
- Deploy KRaft cluster
- Inspect cluster metadata
- Verify broker registration
- Verify controller information
- Analyze metadata logs
- Validate metadata consistency

---

## Lab 14: ZooKeeper-Based Metadata Inspection

### Objective
Inspect broker metadata stored in ZooKeeper.

### Tasks
- Deploy ZooKeeper-based cluster
- Inspect broker metadata
- Verify broker registration
- Analyze metadata storage
- Compare with KRaft metadata
- Document differences

---

## Lab 15: KRaft vs ZooKeeper Comparison Exercise

### Objective
Compare metadata management approaches across cluster modes.

### Tasks
- Deploy KRaft cluster
- Deploy ZooKeeper cluster
- Compare metadata management
- Compare controller operations
- Compare cluster administration
- Prepare comparison report

---

## Lab 16: Broker Recovery After Restart

### Objective
Verify broker recovery after a clean restart.

### Tasks
- Stop broker
- Produce additional data
- Restart broker
- Verify broker recovery
- Verify replica synchronization
- Validate cluster stability

---

## Lab 17: Broker Recovery After Crash

### Objective
Test broker recovery after an unclean crash.

### Tasks
- Simulate broker crash
- Restart broker
- Verify log recovery
- Verify metadata recovery
- Verify replica catch-up
- Validate data consistency

---

## Lab 18: Replica Synchronization After Recovery

### Objective
Observe replica catch-up process after broker restart.

### Tasks
- Stop follower broker
- Generate workload
- Restart follower broker
- Observe replica synchronization
- Verify ISR rejoin
- Analyze recovery performance

---

## Lab 19: Broker Health Monitoring

### Objective
Monitor broker health and performance metrics.

### Tasks
- Monitor broker metrics
- Monitor network utilization
- Monitor disk usage
- Monitor request rates
- Monitor replica lag
- Generate health report

---

## Lab 20: Production Broker Readiness Validation

### Objective
Validate brokers for production readiness.

### Tasks
- Validate broker configuration
- Validate replication configuration
- Validate rack awareness
- Validate monitoring setup
- Validate recovery procedures
- Prepare production readiness checklist










# REPLICATION LABS

## Lab 1: Create Topics with Different Replication Factors

### Objective
Understand how different replication factors affect topic configuration and storage.

### Tasks
- Create topic with RF=1
- Create topic with RF=2
- Create topic with RF=3
- Compare replica placement
- Analyze storage consumption
- Document availability differences

---

## Lab 2: Replication Factor Failure Tolerance Validation

### Objective
Test fault tolerance of topics with different replication factors.

### Tasks
- Create topics with different RF values
- Stop one broker
- Verify topic availability
- Compare RF=1, RF=2, and RF=3 behavior
- Analyze fault tolerance
- Prepare findings report

---

## Lab 3: Inspect Leader and Follower Replicas

### Objective
Identify and understand leader and follower replica roles.

### Tasks
- Create replicated topic
- Identify leader replicas
- Identify follower replicas
- Verify replica assignment
- Observe leader responsibilities
- Observe follower synchronization

---

## Lab 4: Validate Producer Writes Through Leaders

### Objective
Understand that all producer writes go through partition leaders.

### Tasks
- Create replicated topic
- Start producer
- Send messages
- Verify writes reach leader first
- Observe replica synchronization
- Analyze write path

---

## Lab 5: Validate Consumer Reads from Leaders

### Objective
Confirm that consumer reads always go through partition leaders.

### Tasks
- Create replicated topic
- Produce messages
- Start consumer
- Verify consumer read path
- Inspect replica states
- Document observations

---

## Lab 6: Leader Failure and Automatic Failover

### Objective
Observe automatic leader election after broker failure.

### Tasks
- Identify current leader
- Stop leader broker
- Observe leader election
- Verify producer availability
- Verify consumer availability
- Measure recovery time

---

## Lab 7: ISR Inspection and Monitoring

### Objective
Inspect and monitor In-Sync Replica (ISR) states.

### Tasks
- Create replicated topic
- Inspect ISR members
- Produce continuous messages
- Monitor ISR state
- Verify replica synchronization
- Document ISR behavior

---

## Lab 8: Trigger ISR Shrink Event

### Objective
Force an ISR shrink by slowing down a follower.

### Tasks
- Create replicated topic
- Generate high workload
- Slow down a follower broker
- Observe ISR shrink
- Verify replica removal from ISR
- Analyze cluster impact

---

## Lab 9: ISR Recovery Validation

### Objective
Verify ISR recovery after restoring broker performance.

### Tasks
- Trigger ISR shrink
- Restore broker performance
- Observe replica catch-up
- Verify rejoining of ISR
- Monitor synchronization progress
- Document recovery process

---

## Lab 10: Simulate Out-of-Sync Replica (OSR)

### Objective
Create and identify Out-of-Sync Replica conditions.

### Tasks
- Create replicated topic
- Stop a follower broker
- Continue producing messages
- Observe replica lag
- Verify OSR state
- Analyze fault tolerance impact

---

## Lab 11: OSR Recovery and Synchronization

### Objective
Recover OSR and observe the catch-up process.

### Tasks
- Create OSR condition
- Restart follower broker
- Monitor replication catch-up
- Verify return to ISR
- Validate data consistency
- Analyze recovery duration

---

## Lab 12: Unclean Leader Election Demonstration

### Objective
Understand unclean leader election and its data loss risks.

### Tasks
- Enable unclean leader election
- Create replicated topic
- Force ISR loss scenario
- Trigger leader failure
- Observe unclean election
- Analyze availability benefits
- Analyze data loss risks

---

## Lab 13: Clean vs Unclean Leader Election Comparison

### Objective
Compare clean and unclean leader election behaviors.

### Tasks
- Configure clean election scenario
- Configure unclean election scenario
- Simulate broker failures
- Compare availability
- Compare data consistency
- Prepare comparison report

---

## Lab 14: Configure Min In-Sync Replicas

### Objective
Set up min.insync.replicas for durability guarantees.

### Tasks
- Create topic with RF=3
- Configure min.insync.replicas
- Verify topic configuration
- Produce messages using acks=all
- Validate write success conditions
- Analyze durability guarantees

---

## Lab 15: Write Failure Due to Insufficient ISR

### Objective
Observe producer write failures when ISR falls below minimum.

### Tasks
- Configure min.insync.replicas
- Remove replicas from ISR
- Produce messages with acks=all
- Observe write failures
- Analyze error responses
- Document findings

---

## Lab 16: Compare Different min.insync.replicas Values

### Objective
Evaluate trade-offs between different min.insync.replicas settings.

### Tasks
- Configure different MISR values
- Simulate replica failures
- Observe producer behavior
- Compare availability
- Compare durability
- Recommend production settings

---

## Lab 17: Replication Throttling During Recovery

### Objective
Control replication traffic using throttling during recovery.

### Tasks
- Create replicated topic
- Trigger replica recovery
- Configure replication throttling
- Monitor replication traffic
- Compare recovery speeds
- Analyze network usage

---

## Lab 18: Replication Throttling During Partition Reassignment

### Objective
Apply throttling during partition reassignment operations.

### Tasks
- Create large topic
- Initiate partition reassignment
- Configure throttling limits
- Monitor cluster performance
- Verify controlled replication traffic
- Analyze results

---

## Lab 19: Monitor Replication Metrics

### Objective
Track replication health using key metrics.

### Tasks
- Monitor under-replicated partitions
- Monitor ISR changes
- Monitor replica lag
- Monitor network throughput
- Generate replication health report
- Analyze operational risks

---

## Lab 20: Configure Fetch from Follower

### Objective
Enable follower fetching for rack-aware read optimization.

### Tasks
- Configure broker rack information
- Configure consumer rack information
- Enable follower fetching
- Produce messages
- Verify consumer read path
- Analyze network optimization

---

## Lab 21: Cross-Rack Consumer Optimization

### Objective
Reduce cross-rack network traffic using rack-aware consumers.

### Tasks
- Deploy brokers across multiple racks
- Configure consumer.rack
- Produce messages
- Verify local replica selection
- Measure network traffic reduction
- Analyze performance improvements

---

## Lab 22: Rack-Aware Replication Configuration

### Objective
Configure rack awareness for fault-tolerant replica placement.

### Tasks
- Configure broker.rack on all brokers
- Create replicated topic
- Verify cross-rack replica placement
- Inspect replica distribution
- Validate fault tolerance strategy
- Document configuration

---

## Lab 23: Simulate Rack Failure

### Objective
Test cluster behavior during a complete rack outage.

### Tasks
- Configure rack-aware cluster
- Create replicated topic
- Simulate rack outage
- Verify topic availability
- Verify leader election
- Analyze disaster recovery capability

---

## Lab 24: Availability Zone Failure Simulation

### Objective
Test business continuity during availability zone failures.

### Tasks
- Deploy brokers across multiple availability zones
- Create RF=3 topic
- Simulate zone failure
- Observe replica behavior
- Verify data availability
- Analyze business continuity

---

## Lab 25: Production Replication Readiness Assessment

### Objective
Validate replication configuration for production readiness.

### Tasks
- Audit replication factor configuration
- Audit ISR configuration
- Audit min.insync.replicas settings
- Audit rack awareness configuration
- Audit replication monitoring
- Prepare production readiness report












# PRODUCERS LABS

## Lab 1: Build a Basic Kafka Producer

### Objective
Create and test a basic Kafka producer application.

### Tasks
- Create a producer application
- Connect producer to Kafka cluster
- Send messages to a topic
- Verify message delivery
- Inspect producer workflow

---

## Lab 2: Analyze Producer Architecture

### Objective
Understand producer internal components and data flow.

### Tasks
- Produce messages continuously
- Observe record accumulator behavior
- Observe sender thread activity
- Inspect metadata refresh
- Analyze end-to-end producer flow

---

## Lab 3: ProducerRecord Field Validation

### Objective
Explore all fields available in ProducerRecord.

### Tasks
- Send records with topic only
- Send records with topic and key
- Send records with explicit partition
- Send records with custom timestamp
- Send records with headers
- Verify record metadata

---

## Lab 4: Message Headers Implementation

### Objective
Implement and use message headers for metadata.

### Tasks
- Add custom headers
- Send business metadata in headers
- Consume and verify headers
- Implement correlation IDs
- Analyze real-world usage

---

## Lab 5: String Serializer Implementation

### Objective
Use string serializer for text messages.

### Tasks
- Configure string serializer
- Send text messages
- Verify serialization
- Consume messages
- Validate data integrity

---

## Lab 6: JSON Serializer Implementation

### Objective
Serialize and send JSON structured messages.

### Tasks
- Create JSON payloads
- Configure JSON serialization
- Send structured messages
- Consume and verify data
- Analyze message format

---

## Lab 7: Avro Serialization with Schema Registry

### Objective
Implement Avro serialization with schema registry.

### Tasks
- Install schema registry
- Create Avro schema
- Configure Avro serializer
- Produce Avro messages
- Verify schema validation
- Test schema evolution

---

## Lab 8: Protobuf Serialization

### Objective
Use Protobuf serialization for efficient messaging.

### Tasks
- Create Protobuf schema
- Configure Protobuf serializer
- Produce messages
- Consume and validate data
- Compare with Avro

---

## Lab 9: Custom Serializer Development

### Objective
Build and implement a custom serializer.

### Tasks
- Create custom serializer
- Serialize custom objects
- Produce messages
- Consume and deserialize data
- Validate serialization logic

---

## Lab 10: Default Partitioner Validation

### Objective
Observe default partitioner behavior for keyless messages.

### Tasks
- Create multi-partition topic
- Send messages without keys
- Observe partition assignment
- Analyze distribution behavior
- Document findings

---

## Lab 11: Key-Based Partitioning

### Objective
Use message keys for deterministic partition assignment.

### Tasks
- Create multi-partition topic
- Send messages with keys
- Verify same key uses same partition
- Validate ordering guarantees
- Analyze partition distribution

---

## Lab 12: Sticky Partitioner Demonstration

### Objective
Implement and test sticky partitioner behavior.

### Tasks
- Configure sticky partitioning
- Produce messages
- Observe batch formation
- Analyze partition usage
- Compare with default behavior

---

## Lab 13: Custom Partitioner Development

### Objective
Build a custom partitioner for business routing logic.

### Tasks
- Create custom partitioner
- Implement business routing logic
- Configure producer
- Produce messages
- Verify custom routing
- Validate ordering requirements

---

## Lab 14: Producer Interceptor Implementation

### Objective
Implement producer interceptor for message interception.

### Tasks
- Create producer interceptor
- Intercept outgoing messages
- Add custom metadata
- Log producer activity
- Verify interceptor execution

---

## Lab 15: Audit and Monitoring Interceptors

### Objective
Build interceptors for auditing and metrics tracking.

### Tasks
- Implement audit interceptor
- Implement metrics interceptor
- Track message activity
- Generate audit logs
- Analyze business events

---

## Lab 16: acks=0 Performance Test

### Objective
Test producer performance with acks=0 setting.

### Tasks
- Configure acks=0
- Produce high-volume messages
- Measure throughput
- Simulate broker failure
- Analyze message loss risks

---

## Lab 17: acks=1 Reliability Test

### Objective
Evaluate reliability characteristics of acks=1.

### Tasks
- Configure acks=1
- Produce messages
- Simulate leader failure
- Observe producer behavior
- Analyze durability risks

---

## Lab 18: acks=all Durability Test

### Objective
Validate durability guarantees with acks=all.

### Tasks
- Configure acks=all
- Produce messages
- Verify ISR acknowledgments
- Simulate failure scenarios
- Validate data durability

---

## Lab 19: Compare Acknowledgment Modes

### Objective
Compare performance and reliability across ack modes.

### Tasks
- Test acks=0
- Test acks=1
- Test acks=all
- Measure throughput
- Measure latency
- Compare reliability characteristics

---

## Lab 20: Retry Mechanism Validation

### Objective
Understand producer retry behavior during failures.

### Tasks
- Configure retries
- Simulate temporary broker failure
- Observe retry behavior
- Verify successful recovery
- Analyze retry metrics

---

## Lab 21: retry.backoff.ms Experiment

### Objective
Test different retry backoff configurations.

### Tasks
- Configure different backoff values
- Trigger producer retries
- Measure recovery behavior
- Analyze broker load
- Compare results

---

## Lab 22: deliver.timeout.ms Validation

### Objective
Observe message expiration and timeout behavior.

### Tasks
- Configure delivery timeout
- Simulate slow broker responses
- Observe message expiration
- Analyze producer errors
- Document findings

---

## Lab 23: request.timeout.ms Validation

### Objective
Test request timeout handling.

### Tasks
- Configure request timeout
- Simulate network delays
- Observe timeout events
- Analyze producer responses
- Compare different values

---

## Lab 24: Producer Batching Performance Test

### Objective
Measure batch formation impact on performance.

### Tasks
- Produce high-volume messages
- Observe batch formation
- Measure throughput
- Measure latency
- Analyze network utilization

---

## Lab 25: batch.size Optimization

### Objective
Optimize batch size for throughput.

### Tasks
- Test small batch size
- Test medium batch size
- Test large batch size
- Measure throughput
- Compare performance results

---

## Lab 26: linger.ms Optimization

### Objective
Balance latency and throughput using linger.ms.

### Tasks
- Configure different linger.ms values
- Produce messages
- Measure throughput
- Measure latency
- Analyze trade-offs

---

## Lab 27: buffer.memory Stress Test

### Objective
Test producer behavior under buffer pressure.

### Tasks
- Configure buffer memory
- Produce large message volumes
- Observe buffer utilization
- Trigger buffer exhaustion
- Analyze producer behavior

---

## Lab 28: max.block.ms Validation

### Objective
Validate blocking behavior when buffers are full.

### Tasks
- Configure max.block.ms
- Exhaust producer buffers
- Observe blocking behavior
- Verify timeout handling
- Analyze error conditions

---

## Lab 29: GZIP Compression Benchmark

### Objective
Benchmark GZIP compression performance.

### Tasks
- Configure GZIP compression
- Produce large workloads
- Measure compression ratio
- Measure CPU usage
- Measure network savings

---

## Lab 30: Snappy Compression Benchmark

### Objective
Benchmark Snappy compression performance.

### Tasks
- Configure Snappy compression
- Produce large workloads
- Measure compression ratio
- Measure throughput
- Compare with GZIP

---

## Lab 31: LZ4 Compression Benchmark

### Objective
Benchmark LZ4 compression performance.

### Tasks
- Configure LZ4 compression
- Measure producer performance
- Measure compression efficiency
- Analyze resource usage
- Compare results

---

## Lab 32: ZSTD Compression Benchmark

### Objective
Benchmark ZSTD compression performance.

### Tasks
- Configure ZSTD compression
- Produce large workloads
- Measure compression ratio
- Measure throughput
- Compare with other algorithms

---

## Lab 33: Compression Comparison Report

### Objective
Compare all compression algorithms and recommend settings.

### Tasks
- Benchmark GZIP
- Benchmark Snappy
- Benchmark LZ4
- Benchmark ZSTD
- Compare CPU usage
- Compare network savings
- Recommend production settings

---

## Lab 34: Enable Idempotent Producer

### Objective
Enable and test idempotent producer behavior.

### Tasks
- Configure enable.idempotence=true
- Produce messages
- Simulate retry scenarios
- Verify duplicate prevention
- Analyze reliability improvements

---

## Lab 35: Idempotence Failure Recovery Test

### Objective
Test idempotent producer recovery from failures.

### Tasks
- Enable idempotence
- Simulate network failures
- Trigger producer retries
- Verify message deduplication
- Validate ordering guarantees

---

## Lab 36: Producer ID and Sequence Number Analysis

### Objective
Inspect producer IDs and sequence numbers.

### Tasks
- Enable idempotent producer
- Produce messages
- Inspect producer IDs
- Analyze sequence numbers
- Verify duplicate detection

---

## Lab 37: Transactional Producer Setup

### Objective
Configure and initialize transactional producer.

### Tasks
- Configure transactional producer
- Initialize transactions
- Verify coordinator registration
- Validate transaction configuration

---

## Lab 38: Transaction Commit Workflow

### Objective
Execute and verify transaction commit operations.

### Tasks
- Begin transaction
- Produce messages
- Commit transaction
- Verify consumer visibility
- Validate atomic writes

---

## Lab 39: Transaction Abort Workflow

### Objective
Test transaction abort and rollback behavior.

### Tasks
- Begin transaction
- Produce messages
- Abort transaction
- Verify message visibility
- Analyze rollback behavior

---

## Lab 40: Multi-Partition Transaction Test

### Objective
Test transactions spanning multiple partitions.

### Tasks
- Create multi-partition topic
- Start transaction
- Write to multiple partitions
- Commit transaction
- Verify atomicity
- Validate consistency

---

## Lab 41: Multi-Topic Transaction Test

### Objective
Test transactions spanning multiple topics.

### Tasks
- Create multiple topics
- Begin transaction
- Write to multiple topics
- Commit transaction
- Verify atomic visibility
- Validate exactly-once behavior

---

## Lab 42: Transaction Failure Recovery

### Objective
Test transaction recovery after producer failure.

### Tasks
- Start transaction
- Produce messages
- Simulate producer failure
- Observe recovery process
- Validate consistency guarantees

---

## Lab 43: Producer Metrics Monitoring

### Objective
Monitor producer metrics using JMX.

### Tasks
- Enable JMX metrics
- Generate producer traffic
- Monitor request metrics
- Monitor error metrics
- Generate monitoring report

---

## Lab 44: Request Rate and Error Rate Analysis

### Objective
Analyze producer request and error rates.

### Tasks
- Generate producer workload
- Measure request rate
- Simulate failures
- Measure error rate
- Configure alerts

---

## Lab 45: Retry Metrics Analysis

### Objective
Monitor and analyze producer retry behavior.

### Tasks
- Trigger producer retries
- Monitor record-retry-rate
- Monitor record-send-total
- Analyze reliability trends
- Prepare findings report

---

## Lab 46: Batch Size and Compression Monitoring

### Objective
Monitor batching and compression effectiveness.

### Tasks
- Generate producer traffic
- Monitor batch-size-avg
- Monitor compression-rate
- Analyze throughput
- Identify optimization opportunities

---

## Lab 47: End-to-End Producer Performance Tuning

### Objective
Perform comprehensive producer performance tuning.

### Tasks
- Tune acknowledgments
- Tune batching parameters
- Tune compression settings
- Tune retry settings
- Measure throughput
- Measure latency
- Create performance report

---

## Lab 48: Production Producer Readiness Assessment

### Objective
Validate producer configuration for production.

### Tasks
- Validate producer configuration
- Validate reliability settings
- Validate compression strategy
- Validate monitoring setup
- Validate transaction configuration
- Prepare production readiness checklist












# CONSUMERS LABS

## Lab 1: Create and Run a Basic Consumer

### Objective
Create a basic consumer and verify message consumption.

### Tasks
- Create a topic and consume messages from it
- Verify consumer receives records
- Verify offsets increase sequentially
- Verify consumer successfully reads from assigned partitions

---

## Lab 2: Consumer Group Load Balancing

### Objective
Understand how consumers in the same group share partitions.

### Tasks
- Create a topic with multiple partitions
- Start 3 consumers in the same consumer group
- Verify partition assignment distribution
- Verify load balancing across consumers
- Verify no duplicate processing inside the group

---

## Lab 3: Multiple Consumer Groups

### Objective
Demonstrate publish-subscribe behavior across consumer groups.

### Tasks
- Create consumer group A
- Create consumer group B
- Consume the same topic from both groups
- Verify both groups receive all messages
- Verify independent offset management
- Verify independent lag tracking

---

## Lab 4: Observe Group Coordinator Activity

### Objective
Identify and monitor group coordinator behavior.

### Tasks
- Create a consumer group
- Add and remove consumers
- Verify coordinator assignment
- Verify group leader election
- Verify partition reassignment process

---

## Lab 5: Offset Tracking Validation

### Objective
Monitor consumer offset movement during consumption.

### Tasks
- Consume records from a topic
- Monitor current offset
- Monitor committed offset
- Verify offset movement
- Verify difference between current and committed offsets

---

## Lab 6: Inspect __consumer_offsets Topic

### Objective
Explore the internal offset storage topic.

### Tasks
- Create consumer groups and consume messages
- Verify offset storage in __consumer_offsets
- Verify offset commit records
- Verify consumer metadata storage

---

## Lab 7: Auto Commit Testing

### Objective
Test automatic offset commit behavior.

### Tasks
- Enable enable.auto.commit=true
- Consume messages
- Force consumer shutdown
- Verify offset commit behavior
- Verify message loss possibilities
- Verify recovery position

---

## Lab 8: Manual Sync Commit Testing

### Objective
Implement and test synchronous offset commits.

### Tasks
- Disable auto commit
- Use commitSync()
- Verify offset commits occur after processing
- Verify recovery resumes correctly
- Verify at-least-once behavior

---

## Lab 9: Manual Async Commit Testing

### Objective
Implement and test asynchronous offset commits.

### Tasks
- Disable auto commit
- Use commitAsync()
- Verify higher throughput
- Verify offset commit behavior
- Verify commit failure handling

---

## Lab 10: Commit Callback Validation

### Objective
Implement callbacks for async commit monitoring.

### Tasks
- Implement commit callbacks
- Verify successful commit logging
- Verify failed commit logging
- Verify callback execution behavior

---

## Lab 11: Consumer Restart Recovery

### Objective
Validate offset recovery after consumer restart.

### Tasks
- Consume messages
- Stop consumer
- Restart consumer
- Verify recovery from committed offsets
- Verify no unexpected message skipping
- Verify offset persistence

---

## Lab 12: Earliest Offset Consumption

### Objective
Configure consumers to read from the beginning of topics.

### Tasks
- Configure auto.offset.reset=earliest
- Start a new consumer group
- Verify consumption starts from beginning
- Verify historical messages are processed

---

## Lab 13: Latest Offset Consumption

### Objective
Configure consumers to read only new messages.

### Tasks
- Configure auto.offset.reset=latest
- Start a new consumer group
- Verify historical messages ignored
- Verify only new messages consumed

---

## Lab 14: Seek to Specific Offset

### Objective
Use seek() for targeted offset repositioning.

### Tasks
- Consume records
- Use seek() to move to a specific offset
- Verify replay functionality
- Verify offset repositioning
- Verify controlled reprocessing

---

## Lab 15: Seek to Beginning and End

### Objective
Use seekToBeginning() and seekToEnd() APIs.

### Tasks
- Use seekToBeginning()
- Use seekToEnd()
- Verify full replay capability
- Verify skip-to-latest functionality

---

## Lab 16: Position Monitoring

### Objective
Monitor consumer position during processing.

### Tasks
- Monitor consumer position during processing
- Verify current processing location
- Verify offset advancement
- Verify progress tracking

---

## Lab 17: Pause and Resume Consumption

### Objective
Control message flow using pause() and resume().

### Tasks
- Pause assigned partitions
- Continue producer activity
- Resume consumption
- Verify message accumulation
- Verify lag increase
- Verify lag recovery after resume

---

## Lab 18: Poll Loop Tuning

### Objective
Optimize poll loop behavior for performance.

### Tasks
- Modify max.poll.records
- Modify poll duration
- Verify processing batch sizes
- Verify throughput impact
- Verify consumer responsiveness

---

## Lab 19: max.poll.interval.ms Failure Scenario

### Objective
Trigger consumer eviction through slow processing.

### Tasks
- Create slow processing logic
- Set small max.poll.interval.ms
- Verify consumer eviction
- Verify rebalance occurrence
- Verify processing interruption

---

## Lab 20: fetch.min.bytes Optimization

### Objective
Optimize fetch request batch sizes.

### Tasks
- Modify fetch.min.bytes
- Modify fetch.max.wait.ms
- Verify batch size changes
- Verify throughput changes
- Verify latency changes

---

## Lab 21: fetch.max.wait.ms Optimization

### Objective
Balance latency and batch formation.

### Tasks
- Modify fetch.max.wait.ms
- Verify broker waiting behavior
- Verify latency impact
- Verify batch formation

---

## Lab 22: Eager Rebalancing Demonstration

### Objective
Observe eager rebalancing behavior.

### Tasks
- Use eager rebalance strategy
- Add a new consumer
- Verify stop-the-world rebalance
- Verify partition revocation
- Verify partition reassignment

---

## Lab 23: Cooperative Rebalancing Demonstration

### Objective
Implement incremental cooperative rebalancing.

### Tasks
- Enable cooperative sticky assignor
- Add and remove consumers
- Verify incremental reassignment
- Verify reduced downtime
- Verify fewer revoked partitions

---

## Lab 24: Rebalance Listener Testing

### Objective
Implement and test rebalance callbacks.

### Tasks
- Implement onPartitionsRevoked()
- Implement onPartitionsAssigned()
- Verify callback execution
- Verify offset handling during rebalance
- Verify assignment events

---

## Lab 25: Static Membership Testing

### Objective
Use static membership for stable assignments.

### Tasks
- Configure group.instance.id
- Restart consumers
- Verify reduced rebalances
- Verify stable assignments
- Verify faster recovery

---

## Lab 26: Session Timeout Failure Simulation

### Objective
Test failure detection through heartbeat timeouts.

### Tasks
- Stop heartbeats
- Verify consumer removal
- Verify failure detection
- Verify rebalance trigger

---

## Lab 27: Heartbeat Configuration Testing

### Objective
Tune heartbeat and session timeout settings.

### Tasks
- Modify heartbeat.interval.ms
- Modify session.timeout.ms
- Verify failure detection speed
- Verify rebalance frequency
- Verify group stability

---

## Lab 28: Consumer Lag Generation

### Objective
Create and observe consumer lag.

### Tasks
- Generate messages faster than consumers process them
- Verify lag growth
- Verify lag metrics
- Verify processing bottlenecks

---

## Lab 29: Consumer Lag Reduction

### Objective
Scale consumers to reduce lag.

### Tasks
- Increase consumers
- Increase partitions
- Verify lag reduction
- Verify throughput improvement
- Verify recovery speed

---

## Lab 30: Consumer Group Monitoring

### Objective
Monitor consumer groups using CLI tools.

### Tasks
- Use kafka-consumer-groups.sh
- Verify group status
- Verify current offsets
- Verify end offsets
- Verify consumer lag

---

## Lab 31: Multi-Threaded Consumer Design

### Objective
Implement one consumer per thread architecture.

### Tasks
- Implement one consumer per thread
- Verify parallel processing
- Verify partition ownership
- Verify thread isolation

---

## Lab 32: Worker Thread Processing Model

### Objective
Implement single consumer with worker threads.

### Tasks
- Implement single consumer
- Implement multiple worker threads
- Verify work distribution
- Verify processing parallelism
- Verify offset commit challenges

---

## Lab 33: Consumer Failure Recovery

### Objective
Test recovery after consumer crash.

### Tasks
- Kill an active consumer
- Verify partition reassignment
- Verify recovery behavior
- Verify message processing continuity

---

## Lab 34: Production Consumer Validation

### Objective
Validate consumer configuration for production.

### Tasks
- Build a production-ready consumer
- Validate manual commits
- Validate rebalance handling
- Validate lag monitoring
- Validate error handling
- Validate recovery mechanisms
- Prepare production readiness report












# Kafka Administration & Operations - Hands-On Labs

## Lab 1: Topic Lifecycle Management

Create, list, describe, alter, and delete topics using `kafka-topics.sh`.

Verify:

- Topic creation
- Topic metadata
- Topic deletion behavior

---

## Lab 2: Production Topic Administration Audit

Review all topics in the cluster.

Identify:

- Incorrect configurations
- Missing replication settings
- Unused topics

Prepare an audit report.

---

## Lab 3: Increase Topic Partitions

Create a topic and increase its partition count.

Verify:

- New partition creation
- Existing data remains unchanged
- Consumer group rebalance

---

## Lab 4: Partition Expansion Impact Analysis

Produce keyed messages before and after partition increase.

Verify:

- Hashing changes
- Ordering implications
- Message distribution

---

## Lab 5: Update Topic Retention Policies

Modify:

- retention.ms
- retention.bytes

Verify:

- New configuration applied
- Data cleanup behavior

---

## Lab 6: Configure min.insync.replicas

Create a replicated topic.

Modify `min.insync.replicas`.

Verify:

- Producer behavior
- Write success and failure scenarios

---

## Lab 7: Topic Configuration Governance

Review topic-level configurations.

Validate:

- Replication settings
- Retention policies
- Cleanup policies

Prepare recommendations.

---

## Lab 8: Partition Reassignment Between Brokers

Move partitions from one broker to another using `kafka-reassign-partitions.sh`.

Verify:

- Data movement
- Replica reassignment
- Cluster health

---

## Lab 9: Broker Expansion and Rebalancing

Add a new broker to the cluster.

Perform partition reassignment.

Verify:

- Load distribution
- Resource utilization
- Replica placement

---

## Lab 10: Reassignment Throttling Validation

Configure reassignment throttling.

Perform partition movement.

Verify:

- Network utilization
- Replication speed
- Cluster stability

---

## Lab 11: Preferred Leader Election

Trigger preferred leader election using `kafka-leader-election.sh`.

Verify:

- Leader redistribution
- Broker load balancing
- Client connectivity

---

## Lab 12: Leader Imbalance Recovery

Create an intentionally unbalanced cluster.

Perform leader rebalance.

Verify:

- Even leader distribution
- Improved broker utilization

---

## Lab 13: Dump Kafka Log Segments

Use `kafka-dump-log.sh`.

Inspect:

- Messages
- Offsets
- Segment metadata

---

## Lab 14: Analyze Index and Time Index Files

Inspect:

- .index files
- .timeindex files

Verify:

- Offset lookups
- Timestamp mappings

---

## Lab 15: Log Corruption Investigation

Simulate log issues.

Use dump tools to investigate.

Verify:

- Corrupted segments
- Recovery information

---

## Lab 16: Consumer Group Administration

Use `kafka-consumer-groups.sh`.

Perform:

- List groups
- Describe groups
- View assignments

Verify group health.

---

## Lab 17: Consumer Lag Analysis

Create lag intentionally.

Monitor using:

- kafka-consumer-groups.sh

Verify:

- Lag calculation
- Lag recovery

---

## Lab 18: Offset Reset Operations

Reset offsets to:

- Earliest
- Latest
- Specific offset

Verify:

- Replay behavior
- Recovery scenarios

---

## Lab 19: Consumer Group Cleanup

Delete inactive consumer groups.

Verify:

- Metadata removal
- Offset cleanup

---

## Lab 20: Producer Quota Enforcement

Configure producer quotas.

Generate high producer traffic.

Verify:

- Throttling behavior
- Throughput limits

---

## Lab 21: Consumer Fetch Quota Enforcement

Configure fetch quotas.

Generate heavy consumer load.

Verify:

- Consumer throttling
- Resource protection

---

## Lab 22: Client-Based Quota Management

Apply quotas using client IDs.

Verify:

- Individual client throttling
- Fair resource allocation

---

## Lab 23: SSL Encryption Setup

Configure SSL communication between:

- Producers
- Consumers
- Brokers

Verify:

- Secure communication
- Certificate validation

---

## Lab 24: Mutual TLS (mTLS) Authentication

Enable mTLS.

Verify:

- Client certificate authentication
- Unauthorized access rejection

---

## Lab 25: SASL Authentication Setup

Configure:

- SASL/PLAIN
- SASL/SCRAM

Verify:

- Successful authentication
- Authentication failures

---

## Lab 26: ACL-Based Authorization

Create ACLs for:

- Producers
- Consumers

Verify:

- Allowed operations
- Denied operations

---

## Lab 27: Wildcard ACL Management

Create wildcard ACLs.

Verify:

- Multi-topic access
- Security boundaries

---

## Lab 28: Super User Administration

Configure Kafka super users.

Verify:

- Administrative access
- ACL bypass behavior

---

## Lab 29: Transactional ID Authorization

Secure transactional producers.

Verify:

- Transactional ID permissions
- Unauthorized transaction failures

---

## Lab 30: JMX Metrics Monitoring

Enable JMX metrics.

Collect:

- Broker metrics
- JVM metrics
- Request metrics

---

## Lab 31: Prometheus and Grafana Integration

Deploy:

- Prometheus
- Grafana

Create Kafka dashboards.

Monitor cluster health.

---

## Lab 32: Under-Replicated Partition Investigation

Create replication issues.

Verify:

- Under-replicated partition metrics
- Recovery procedures

---

## Lab 33: Offline Partition Recovery

Simulate broker failures.

Verify:

- Offline partition detection
- Recovery process

---

## Lab 34: ISR Shrink and Expansion Monitoring

Force replicas out of sync.

Verify:

- ISR shrink events
- ISR expansion events
- Cluster recovery

---

## Lab 35: Request Handler Capacity Analysis

Generate high traffic.

Monitor:

- Request Handler Avg Idle %

Determine cluster bottlenecks.

---

## Lab 36: Network Processor Capacity Analysis

Generate network-intensive workloads.

Monitor:

- Network Processor Avg Idle %

Analyze capacity limits.

---

## Lab 37: Consumer Lag Dashboard Creation

Build monitoring dashboards.

Track:

- Consumer lag by topic
- Consumer lag by group

Configure alerts.

---

## Lab 38: Dynamic Broker Log Level Changes

Modify broker log levels dynamically.

Verify:

- INFO logs
- DEBUG logs
- TRACE logs

Observe operational impact.

---

## Lab 39: Slow Consumer Investigation

Create intentionally slow consumers.

Verify:

- Lag growth
- Throughput degradation
- Recovery procedures

---

## Lab 40: Fetcher Lag Troubleshooting

Create follower replication delays.

Verify:

- Fetcher lag metrics
- Replication recovery
- Cluster stability

---

## Lab 41: Production Operations Runbook Exercise

Perform a full operational review.

Validate:

- Topic health
- Consumer groups
- Replication status
- Quotas
- Authentication
- Authorization
- Monitoring dashboards
- Alerting rules

Prepare an operations readiness report.















# PERFORMANCE TUNING LABS

## LAB 1: Producer Batching Performance Tuning

### Objective
Analyze the impact of `batch.size` and `linger.ms` on producer throughput and latency.

### Tasks
- Create a test topic.
- Produce messages using default settings.
- Measure throughput and latency.
- Increase `batch.size`.
- Increase `linger.ms`.
- Compare performance results.
- Identify optimal batching configuration.

---

## LAB 2: Producer Reliability vs Performance

### Objective
Compare producer performance using different acknowledgment settings.

### Tasks
- Create a topic with replication.
- Test producer using `acks=0`.
- Test producer using `acks=1`.
- Test producer using `acks=all`.
- Measure throughput and latency.
- Compare reliability and performance trade-offs.

---

## LAB 3: Producer Retry Configuration Tuning

### Objective
Understand producer retry behavior during broker failures.

### Tasks
- Configure producer retries.
- Stop broker during message production.
- Observe retry attempts.
- Modify `retry.backoff.ms`.
- Modify `delivery.timeout.ms`.
- Analyze recovery behavior.

---

## LAB 4: Producer Memory Buffer Tuning

### Objective
Analyze producer memory utilization under heavy load.

### Tasks
- Generate high-volume producer traffic.
- Monitor buffer utilization.
- Modify `buffer.memory`.
- Configure `max.block.ms`.
- Observe blocking behavior.
- Compare throughput results.

---

## LAB 5: Producer Compression Benchmark

### Objective
Evaluate compression algorithms and their performance impact.

### Tasks
- Produce messages without compression.
- Enable GZIP compression.
- Enable Snappy compression.
- Enable LZ4 compression.
- Enable ZSTD compression.
- Compare CPU, throughput, and storage utilization.

---

## LAB 6: Consumer Fetch Tuning

### Objective
Optimize consumer throughput using fetch configurations.

### Tasks
- Consume messages using default settings.
- Modify `fetch.min.bytes`.
- Modify `fetch.max.wait.ms`.
- Measure fetch performance.
- Compare latency and throughput.

---

## LAB 7: Consumer Poll Loop Optimization

### Objective
Analyze polling behavior and processing efficiency.

### Tasks
- Configure default polling.
- Modify `max.poll.records`.
- Observe batch processing behavior.
- Modify `max.poll.interval.ms`.
- Simulate slow processing.
- Analyze rebalance impact.

---

## LAB 8: Consumer Group Scaling

### Objective
Study consumer scaling using partitions and consumer groups.

### Tasks
- Create topic with multiple partitions.
- Start one consumer.
- Measure throughput.
- Add additional consumers.
- Observe partition assignments.
- Compare scaling efficiency.

---

## LAB 9: Offset Commit Performance Analysis

### Objective
Compare auto and manual offset commit strategies.

### Tasks
- Enable auto commit.
- Measure performance.
- Implement manual sync commit.
- Implement manual async commit.
- Compare throughput and reliability.
- Analyze offset commit overhead.

---

## LAB 10: Consumer Lag Investigation and Tuning

### Objective
Identify consumer lag causes and optimize consumption.

### Tasks
- Generate high producer load.
- Monitor consumer lag.
- Analyze bottlenecks.
- Tune consumer configurations.
- Scale consumers.
- Validate lag reduction.

---

## LAB 11: Broker Page Cache Analysis

### Objective
Understand Kafka's dependency on OS page cache.

### Tasks
- Produce large volumes of data.
- Consume historical data.
- Monitor OS memory utilization.
- Analyze page cache behavior.
- Compare cache hit and miss scenarios.

---

## LAB 12: Broker Heap Size Tuning

### Objective
Evaluate JVM heap sizing impact on Kafka performance.

### Tasks
- Start broker with small heap.
- Measure GC activity.
- Increase heap size.
- Compare GC performance.
- Analyze page cache availability.
- Determine optimal heap allocation.

---

## LAB 13: Log Flush Performance Analysis

### Objective
Compare Kafka default flushing with forced flush policies.

### Tasks
- Produce messages using default configuration.
- Enable forced log flushing.
- Measure throughput.
- Measure latency.
- Compare disk utilization.
- Analyze durability implications.

---

## LAB 14: Disk and Network Bandwidth Monitoring

### Objective
Identify infrastructure bottlenecks affecting Kafka performance.

### Tasks
- Generate producer load.
- Monitor network utilization.
- Monitor disk throughput.
- Analyze broker resource usage.
- Identify bottlenecks.
- Document findings.

---

## LAB 15: Partition Count vs Throughput Benchmark

### Objective
Analyze the relationship between partition count and throughput.

### Tasks
- Create topic with 1 partition.
- Benchmark producer throughput.
- Increase partitions.
- Repeat benchmark tests.
- Compare consumer parallelism.
- Determine optimal partition count.

---

## LAB 16: Compression Algorithm Comparison

### Objective
Compare Kafka compression algorithms using practical workloads.

### Tasks
- Benchmark GZIP.
- Benchmark Snappy.
- Benchmark LZ4.
- Benchmark ZSTD.
- Compare storage savings.
- Compare throughput and CPU utilization.
- Recommend suitable algorithm.

---

## LAB 17: Replication Throttling During Migration

### Objective
Control replication traffic during partition reassignment.

### Tasks
- Create replicated topics.
- Add a new broker.
- Initiate partition reassignment.
- Configure replication throttling.
- Monitor replication traffic.
- Verify production workload stability.
- Remove throttles after migration.

---

## LAB 18: End-to-End Kafka Performance Benchmark

### Objective
Perform complete producer, broker, and consumer performance tuning.

### Tasks
- Establish baseline metrics.
- Tune producer batching.
- Tune compression.
- Tune consumer fetch settings.
- Tune consumer scaling.
- Analyze broker resource utilization.
- Compare before and after results.
- Document tuning recommendations.





