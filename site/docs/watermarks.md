# Watermarks

Watermarks are used in Tektite for determining when [aggregation windows](aggregating.md#watermarks) can be closed.

Watermarks are generated at the points where data enters the system which are the [`kafka in`](topics.md) and [`topic`](topics.md) operators for
Kafka topics hosted in Tektite or the [`bridge from`](bridging.md) operator for data entering from an external topic.

A watermark carries a timestamp and flows with the rest of the data through the network of streams along all the routes that data can flow. 
When an operator receives a watermark with a particular timestamp, the watermark tells the operator that it shouldn't
expect to see any more data with a timestamp less than this value.

As watermarks flow through partitions, joins and unions - all of which can receive data from different processors, the watermark is
delayed until a watermark has been received from each sending processor, and then a single watermark with a value of the minimum of the 
incoming watermarks is forwarded.

Internally, watermarks piggyback on a closely related Tektite concept: barriers. Barriers are periodically injected into streams at sources
and flow through the network of streams. They're used to guarantee snapshot isolation.

When a watermark reaches a windowed aggregation, the `aggregate` operator makes a decision about which open windows (if any)
should be closed and have results emitted for them.

Tektite supports two strategies for generating watermarks.

## Event time watermark

With an event time strategy, the value given to the watermark when injected at the source is the maximum value of any data seen so far
at the source for the processor minus the value of `watermark_lateness`. The watermark is based on the actual timestamp of the message
which often does not correspond to when the message is processed.

As an example, let's say the maximum timestamp a `kafka in` operator has seen for incoming produced messages is 
`2024-05-16 09:00:00` and `watermark_lateness` is set to `5s`. Then the next watermark to be generated will have a timestamp
value of `2024-05-16 08:59:55`.

This watermark will then flow through the system, and if it reaches a windowed aggregation
the aggregation may decide to close all open windows with a window end <= `2024-05-16 08:59:55` (depending on configuration)

Event time watermarks are the default type and will be injected in any `kafka in`, `topic` or `bridge from` operators automatically
if not explicitly configured.

Here's an example of explicitly configuring a `topic` with event time watermarks:

```
my_topic :=
    (topic partitions = 16
     watermark_type = event_time
     watermark_lateness = 10s
     watermark_idle_timeout = 30s)
```

The same parameters can be used on `kafka in` and `bridge from`.

With an event time strategy, if no data is received on a processor then watermark value can get stuck, and this could 
prevent downstream windowed aggregations from closing.

To unstick stuck watermarks a special watermark with a value of `-1`
is generated if no data is received for `watermark_idle_timeout`. If a windowed aggregation receives a watermark of `-1` its
upstreams are all idle, and can emit results.

The default value of `watermark_lateness` is `1s`. The default value of `watermark_idle_timeout` is `1m`

## Processing time watermark

With a processing time strategy, the value given to the watermark when injected at the source is based on the processing time (i.e.
the current system time on the server) minus the value of `watermark_lateness`.

Here's an example of configuring a `bridge from` with processing time watermarks:

```
sales-imported :=
    (bridge from sales partitions = 16 props = ("bootstrap.servers" = "mykafka.foo.com:9092")
        watermark_type = processing_time
        watermark_lateness = 10s) ->
    (store stream)            
```