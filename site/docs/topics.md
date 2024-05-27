# Topics

Tektite allows you to create topics and access them from any Kafka-compatible client. The topics have the same
characteristics as they would in other Kafka compatible event streaming platforms - they are persistent and partitioned.

With Tektite a topic is just a type of stream. It's a stream that takes input data from external Kafka producers and outputs
data to external Kafka consumers.

## Creating topics

The simplest way to create a topic is by using the `topic` operator.

```
my-topic := (topic partitions = 16)
```

This creates a topic called `my-topic` with 16 partitions. It's just a simple stream composed of a single
`topic` operator.

The number of partitions is mandatory when creating a topic.

## Deleting topics

Topics are deleted just like any stream:

```
delete(my-topic)
```

## Topic magic

The simple topic stream above is actually equivalent to the following stream:

```
my-stream := (kafka in partitions = 16) -> (kafka out)
```

The `kafka in` operator exposes a Kafka compatible producer endpoint. I.e. it accepts messages from any Kafka compatible producer.
The produced data is sent to a `kafka out` which stores it persistently and exposes a Kafka compatible consumer endpoint so the data
can be consumed from any Kafka compatible consumer.

The `topic` operator, under the hood, basically instantiates a `kafka in` followed by a `kafka out` as above. We provide a 
distinct `topic` operator as creating a simple vanilla topic is a very common thing to do in Tektite.

However, sometimes you want to do interesting things with topics that you can't do in existing event streaming platforms, and that's
where a separate `kafka in` and `kafka out` operator become useful.

### Filtered topics

How about a server side filter?

```
filtered-topic :=
    (kafka in) -> (filter by matches(to_string(val), "^hello")) -> (kafka out)
```

This creates a Kafka topic called `filtered-topic` which accepts any messages produced to it but only stores and exposes
for consumption those where the message body starts with the string `"hello"`.

### Exposing an existing stream to Kafka consumers

Let's say you have an existing stream in Tektite called `my-stream`. You can expose it to Kafka consumers using a `kafka out`
operator.

```
exposed-stream := my-stream -> (kafka out)
```

This will create a Kafka topic called `exposed-stream` which any Kafka compatible consumer can consume. Note that this 
topic has no `kafka in` so you cannot produce to it. It's a read-only topic!

Perhaps you have an existing topic living in your existing Apache Kafka installation - and you want to expose that to Kafka consumers after transforming it
in some way. No problem!

```
upper-cased-topic :=
    (bridge from my-kafka-topic props = (bootstrap.servers="...")) ->
    (project key, hdrs, to_bytes(to_upper(to_string(val)))) ->
    (kafka out)
```

The possibilities are endless.

### Write only topics

Sometimes you want only to expose the Kafka producer endpoint, not a consumer endpoint.

```
latest_sensor_readings :=
    (kafka in partitions = 32) ->
    (to table key = to_string(key))
```

This creates a topic called `latest-sensor-readings` that can only be produced to, as there is no `kafka out` operator.

As data arrives it is stored in a table which is keyed on the bytes in the Kafka message key converted to a string. A table
only stores the latest value for the key, so if the key was the sensor id of a IoT sensor this would store the latest message
from each sensor.

It could then be queried or used as input to other streams.

Perhaps you just want to count the number of readings in the last 5 minutes grouped by sensor

```
readings_last_5_mins :=
    (kafka in partitions = 16) ->
    (aggregate count(val) by to_string(key) size 5m hop 10s)
```

This would create a topic called `readings_last_5_mins` that can only be produced to. As messages arrive a windowed aggregation
is maintained that counts the number of readings in the last 5 minutes, grouped by sensor.

The results of the aggregation can be queried or used as input to another stream.

The building blocks can be put together in many ways to do things with topics you never thought were possible.

## The operator schema

Operators typically have an input and an output schema and the `topic` and `kafka in` and `kafka out` operators are no
exception.

The input and output schema are the same, and they model the structure of a Kafka message

Here's the schema. Each column is written as `column_name:column_type`

```
offset: int
event_time: timestamp
key: bytes
val: bytes
hdrs: bytes
```

The `offset` column corresponds to the Kafka message offset in the partition. It's assigned by Tektite on receipt of the message
before it is output from the `kafka out` operator.

The `event_time` column corresponds to the timestamp of the Kafka message. This can be assigned on the client or on the server
depending on configuration (see `kafka-use-server-timestamp` configuration property). In Tektite *all* streams (not just topics) have an `event_time`.

The `key` field corresponds to the key of the Kafka message. It is an arbitrary byte string. It is optional and can be `null`

The `val` field corresponds to the body of the Kafka message. It is also a byte string.

The `hdrs` field corresponds to the raw headers of the Kafka message. It is a byte string and can be `null`

An operator that takes input from a `kafka in` operator will receive data with this schema. Similarly, the input to a 
`kafka out` operator must have this schema, or the Kafka consumer won't be able to understand it.

## Using expressions to extract typed data

Commonly, your Kafka messages will contain structured data, e.g. in JSON format, and you want to perform filters or
projections based on fields in that data.

Let's say your messages are from IoT sensors. The message key is the `sensor_id` encoded as a UTF-8 string. The message
body is JSON with the following format:

```json
{
  "country": "UK",
  "area_code": 1234,
  "temperature": 23.56
}
```

The message also contains a header called `model_version` containing the version of the sensor.

We want a topic that only stores messages from the UK from sensors with version = "12.23". We can use a filter with an
expression that extracts values from the message.

The function library contains functions such as `json_string` and `json_int` which extract the named JSON field from the
named column in the schema. There's also a function `kafka_header` which extracts the named Kafka header from the raw headers.

```
filtered_readings :=
    (kafka in partitions = 16) -> 
    (filter by json_string("country", val) == "UK" &&
        to_string(kafka_header("model_version", hdrs)) == "12.23") ->
    (kafka out)
```

Or perhaps we have a simple topic, and we want to hang an [aggregation](aggregating.md) of it:

```
my_readings := (topic partitions = 16)

sensor_readings_by_country :=
   my_readings -> 
       (project json_string("country", val) as country,
                json_int("area", val) as area,
                json_float("temperature") as temp) ->
       (aggregate max(temp), min(temp), avg(temp) by country, area)        
```

Other times when you have an existing stream that you want to expose as a Kafka consumer endpoint, you want to convert
the schema of the existing stream into the schema that the `kafka out` operator requires. You can use a projection to do
this.

For example, let's say you have an existing stream `sales` with the schema

```
tx_id: string
cust_id: string
product_id: string
amount: int
price: decimal(10, 2)
```

And you want to convert this into an output topic where the key of the Kafka message is the `tx_id` and the message body
is JSON of the form:

```json
{
  "id": "tx12345",
  "customer_id": "cust46464",
  "product_id": "prod67676",
  "amount": 23,
  "price": "99.99"
}
```

Then you can create a new stream that exposes the data to Kafka consumers by using the built-in `sprintf` function to 
format the JSON string

```
transactions := ... // existing stream

transactions_out := transactions ->
   (project to_bytes(tx_id), 
            to_bytes(sprintf(("{\"id\": %s,
              \"customer_id\": %s,
              \"product_id\": %s,
              \"amount\": %d,
              \"price\": %s}",
                tx_id,
                cust_id,
                product_id,
                amount,
                to_string(price))),
            null) ->
   (kafka out)         
```

The input schema to the `kafka out` operator requires `[key:bytes, val:bytes, hdrs:bytes]` and that's what the `project`
operator will output as it has three expressions each of which return a value of type `bytes`.

## Data retention

If you don't want to keep the data in your topic forever, you can set a maximum retention time on it. Data will be deleted
asynchronously from the topic once that time has been exceeded.

This will create a topic that keeps data up to 7 days:

```
topic-with-retention := (topic partitions = 16 retention = 7d)
```

The duration string is of the form of a positive integer followed by a symbol `ms` = milliseconds, `s` = seconds,
`m` = minutes, `h` = hours, `d` = days.

Retention can also be specified on a `kafka out` operator

```
filtered :=
    (kafka in partitions = 16) ->
    (filter by len(val) > 1000) ->
    (kafka out retention = 2h)
```

## Generating watermarks

Watermarks are automatically generated at a `kafka in` operator. Please see the section on [generating watermarks](watermarks.md) for 
more information.