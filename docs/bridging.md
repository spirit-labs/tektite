# Bridging to and from external topics

You can connect a stream to an external topic that lives in any Kafka compatible server - it could an existing Apache Kafka or RedPanda instance, or another Tektite
cluster. We call this *bridging* to an external topic. Bridges can be set up to consume messages from an external topic into the stream
or to produce messages from the stream to the external topic.

Bridges are resilient; they cope with temporary unavailability of the external topic, reconnecting automatically when the
server is available again.

## Bridging from an external topic

To bridge from an external topic and consume messages into your stream you use the `bridge from` operator. This operator
will internally maintain a set of Kafka message consumers which consume messages and make them available in your stream.

Here's an example of creating a stream that consumes from an external topic called `external-topic`.

The messages contain JSON
which have a string field called `country`. The stream [filters](filtering.md) out any messages which don't have country == `UK` then
exposes the stream as a Tektite (read-only) topic which can be consumed by any Kafka consumer.

```
uk_sales :=
    (bridge from external-sales partitions = 16
        props = ("bootstrap.servers" =
                     "mykafka1.foo.com:9092, mykafka2.foo.com:9092")) ->
    (filter by json_string("country", val) == "UK") ->
    (kafka out)
```

Here's another example, where we consume from an external topic and maintain a [windowed aggregation](aggregating.md#windowed-aggregations) which captures
total number and value of sales per country in a one-hour window with a 10-minute hop.

```
uk_sales_totals :=
    (bridge from external-sales partitions = 16
        props = ("bootstrap.servers" =
                     "mykafka1.foo.com:9092, mykafka2.foo.com:9092")) ->
    (project json_string("country", val) as country,
             to_decimal(json_string("value", val), 10, 2) as value) ->
    (partition by country partitions = 10) ->
    (aggregate count(value), sum(value) by country size = 1h hop = 10m)
```

Note that `partitions` must be specified and must correspond to the number of [partitions](partitions.md) in the external topic.

The `props` parameter is used to provide properties to the Kafka client. The `bootstrap.servers` property, at minimum
must be provided. This must contain the addresses of one or more Kafka servers that host the external topic in a comma
separated list.

By default, only messages arriving at the external topic *after* the `bridge from` was created will be consumed. If you want
to consume all existing messages in the external topic you can set the `auto.offset.reset` property to `earliest`. The default
value is `latest`.

The `bridge from` operator has some parameters that can be set:

* `max_poll_messages`: Maximum number of messages to consume (if available) and pass to the stream in a single batch. Defaults to `1000`
* `poll_timeout`: Maximum time to wait polling for messages to arrive before polling again. Defaults to `50ms`

The `bridge from` also generates [watermarks](watermarks.md).

## Bridging to an external topic

To send messages from your stream to an external topic you use the `bridge to` operator.

The `bridge to` will automatically
reconnect to the external server after unavailability. If the server is unavailable pending messages to send will be stored
in Tektite so the local stream can continue operating. When the server becomes available again, pending messages will be
sent.

Here's an example of bridging from a local stream to an external topic called `external-topic`

```
out-stream := cust-updates -> (filter by area == "USA") ->
    (bridge to external-topic props =
        ("bootstrap.servers" = "mykafka1.foo.com:9092, mykafka2.foo.com:9092"))
```

Here's an example of a local write-only Tektite topic which immediately forwards its messages on to an external topic:

```
sales-send-proxy := (topic partitions = 16) ->
    (bridge to sales props =
        ("bootstrap.servers" = "mykafka1.foo.com:9092, mykafka2.foo.com:9092"))
```

Please note, that the partition number is maintained when sending to the external topic.

I.e. if a message is in partition 12 in the
local stream it will be sent to partition 12 in the external topic, so you must ensure that the number of partitions in your
local stream matches the number of partitions in the external topic. If it doesn't you should repartition your stream before
sending.

For example, if the external topic has 50 partitions:

```
out-stream := cust-updates -> (filter by area == "USA") ->
    (partition by customer_id partitions = 50) ->
    (bridge to external-topic props =
        ("bootstrap.servers" = "mykafka1.foo.com:9092, mykafka2.foo.com:9092"))
```

The `bridge to` operator has some parameters that can be set:

* `retention`: Maximum time to keep locally stored messages. Default is `24h`
* `initial_retry_delay`: When target server is unavailable how long to wait before initially retrying to connect. Default is `5s`
* `max_retry_delay`: When reconnecting, retry delay automatically increases up to a maximum of this value. Default is `30s`
* `connect_timeout`: How long to wait for a connection before considering it failed. Default is `5s`
* `send_timeout`: How long to wait for sending a batch to complete before considering it failed. Default is `2s`