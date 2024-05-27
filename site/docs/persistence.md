# Persistence

## Persistent streams

Let's take an example of creating a new stream by filtering an existing stream. This one takes the `sales` stream and only lets UK sales through:

```
my-stream := sales -> (filter by by country = "UK")
```

This is a perfectly valid new stream, and can have child streams, but by itself it is not persistent.

If you want to make a stream persistent, such that the data is durably stored, and it can be queried, then you use
a `store stream` operator.

```
my-stream := sales ->
    (filter by by country = "UK") -->
    (store stream)
```

A persistent stream is given an extra column `offset`. This is an automatically generated monotonically increasing sequence
per partition (there can be gaps in the sequence, but it always increases)

The operators `topic` and `kafka out` have a "built-in" store stream, so you don't need to add one yourself. So the following
streams are also persistent, i.e. they are queryable:

```
my-topic := (topic partitions=16)

exposed-sales := sales -> (kafka out)
```
The persisted data for a stream is never overwritten, the `offset` column always increases.

## Tables

The other way that operators can persist queryable data is as *tables*.

A table differs from a persistent stream in that it has a *key*. There can be multiple columns in the key, and the columns
can be of any valid Tektite data type.

Entries for the same key are updated as new data arrives with the same key.

The operators that persist data as tables are the [`aggregate`](aggregating.md) and `store table` operators.

Here's an example aggregate:

```
my-agg := sales ->
    (aggregate count(amount), max(amount) by country, city)
```

This would store aggregate results in a table with key made of columns `country` and `city`.

You can also use a `store table` operator to persist stream data with a key.

For example, let's say we have a stream which contains IoT sensor readings and has the schema:

```
event_time: timestamp
sensor_id: string
temperature: float
country: string
area: string
```

We could use a `store table` operator to create a new stream which takes the stream of sensor readings and persists the latest 
reading for a particular sensor. This table would be queryable.

```
latest_sensor_readings := sensor_readings ->
    (store table by sensor_id)
```

The `store table` operator takes a list of columns.  The syntax is `store table by <column list>`. The column list defines
which columns form the key of the table.

The table can then be [queried](queries.md), e.g. to list all latest UK sensor readings:

```
(scan all from latest_sensor_readings) -> (filter by country = "UK")
```

And to lookup latest reading for a specific sensor (key lookup):

```
(get "sensor34567" from latest_sensor_readings)
```

The column list can be omitted altogether in a `store table`. In this case only a single row per partition will be retained.
This is often used in conjunction with a `partition by const`, to store the last received value:

```
overall_latest_sensor_reading := sensor_readings ->
    (partition by const partitions = 1) ->
    (store table)
```
