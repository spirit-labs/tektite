# Partitioning data

## Processors

Tektite processes data using **processors**. A Tektite server has a fixed number of processors, typically chosen to correspond
to the number of available cores on the machine.

Essentially, a processor is an event loop, which loops around consuming submitted tasks, executing them and then processing
the next task when it's available. Internally, each processor uses a single goroutine to execute all work.

## Partitions

All Tektite streams are partitioned. The number of partitions is determined by the `partitions` parameter on the most recent upstream
`bridge from`, `topic`, `kafka in` or `partition by` operator.

Tektite pins each partition with a particular processor. Any work for a particular partition will always be processed by the same
processor.

Partitioning a stream into many partitions therefore allows us to parallelise processing of data on that stream.

## The `partition by` operator

The `partition by` operator re-partitions the stream with the specified key and number of partitions. Why would you need 
to re-partition? Let's consider some examples:

### Re-partitioning for an `aggregate` or `store table`

Let's say we have a Kafka topic which has 20 partitions.

```
sales := (topic partitions = 20)
```

It's the Kafka client that chooses the partition for the message, not Tektite. Let's say the Kafka client used a random policy.

We wish to create a windowed aggregation calculating sales totals by `country`. As the sales topic is not partitioned on the 
`country` column, messages for a particular country could be in any partition.

When calculating an aggregation, the aggregation is maintained per partition, so for a correct value, all messages for a
particular country must be aggregated on the same partition.

To ensure this, we re-partition the stream on the `country` field:

```
sales-tots := sales ->
    (project json_string("country", val) as country,
        to_decimal(json_string("value", val), 10, 2) as value) ->
    (partition by country partitions = 10) ->
    (aggregate count(value), sum(value) by country size = 1h hop = 10m)
```

Similarly, when using a `store table` operator, you will need to partition by the key of the table before the `store table` operator.

The partition by operator can also partition by multiple key columns. You would commonly partition by multiple columns when
you are grouping by multiple columns in an aggregation:

```
sales-tots := sales ->
    (project json_string("country", val) as country,
             json_string("city", val) as city,
             to_decimal(json_string("value", val), 10, 2) as value) ->
    (partition by country, city partitions = 10) ->
    (aggregate count(value), sum(value) by country, city size = 1h hop = 10m)
```

#### Partiton by const

Sometimes you want all rows to go to a single partition. This would be the case where you have an aggregatioon with no group
by expressions - you want to maintain overall aggregations, or you want to just maintain the latest value seen in a `store table` operator.

In these case you can use the special value `const` when defining the partition key.

For example:

```
overall-sales-tots := sales ->
    (project json_string("country", val) as country,
             json_string("city", val) as city,
             to_decimal(json_string("value", val), 10, 2) as value) ->
    (partition by const partitions = 1) ->
    (aggregate count(value), sum(value) size = 1h hop = 10m)
```

Or for a table:

```
last-sale := sales ->
  (partition by const partitions = 1)
  (store table)
```

### Re-partitioning for a `bridge to`

If you are bridging from a Tektite stream to an external Kafka topic and the external topic has a different number of
partitions then you will need to re-partition the stream first:

```
out-stream := sales ->
   (partition by customer_id partitions = 50) ->
   (bridge to external-sales
       props = ("bootstrap.servers" = "mykafka1.foo.com:9092"))
```

### Re-partitioning an existing topic

Take an existing Tektite topic, repartition it with a different key and number of partitions and expose the repartitioned
data as a new (read only) topic:

```
repartitioned := my-topic ->
    (partition by product_id partitions = 100) ->
    (kafka out)
```

### Re-partitioning an external topic

Bridge an external topic into Tektite, filter out some data, repartition it and send it to another external topic.

```
repartition-stream :=
    (bridge from sales-by-customer partitions = 16
        props = ("bootstrap.servers" = "mykafka1.foo.com:9092")) ->
    (filter by json_string("country", val) != "USA") ->
    (partition by product_id partitions = 50) ->
    (bridge to sales-by-product
        props = ("bootstrap.servers" = "mykafka1.foo.com:9092"))
```
