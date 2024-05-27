# Aggregations

Unlike stateless projections, aggregations apply aggregate functions to the incoming data and the current state
to create new state. The state can optionally be grouped by a list of expressions.

Tektite supports both non-windowed aggregations and windowed aggregations. 

Aggregations are defined using the `aggregate` operator and support the following aggregate functions:

* `count` - compute the count of rows seen
* `min` - compute the minimum of values seen
* `max` - compute the maximum of values seen
* `sum` - compute the sum of values seen
* `avg` - compute the average (mean) of values seen

The operand to the aggregate function can be any valid Tektite expression which evaluates to a type that that is
compatible with the aggregate function:

* `count` can be used with any type
* `min` requires operand type `int`, `float`, `decimal`, `string`, `bytes`, `timestamp`. For `string` and `bytes`, a lexicographical comparison is performed
* `max` requires operand type `int`, `float`, `decimal`, `string`, `bytes`, `timestamp`. For `string` and `bytes`, a lexicographical comparison is performed
* `sum` requires operand type `int`, `float`, `decimal`
* `avg` requires operand type `int`, `float`, `decimal`, `timestamp`

## Non-windowed aggregations

A non-windowed aggregation computes the aggregate functions over all incoming data received.

Here's an example that computes the `min`, `max`, `count` and `sum` of all sales received so far.

```
sales_totals := sales_topic ->
    (partition by const partitions = 1) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount))
```

The `partition` operator is necessary as the incoming data is already partitioned into multiple partitions, but we want to compute
a single row over all data, so we need to repartition the incoming data into a single partition before computing the
aggregation. If we didn't do this, we'd end up with a totals row per partition.

This can then be queried:

```
tektite> (scan all from sales_totals);
+---------------------------------------------------------------------------------------------------------------------+
| event_time                 | min(amount)         | max(amount)         | count(amount)        | sum(amount)         |
+---------------------------------------------------------------------------------------------------------------------+
| 2024-05-14 07:21:07.851000 | 85.23               | 123.99              | 16                   | 1751.28             |
+---------------------------------------------------------------------------------------------------------------------+
```

You could also store the updates as a new stream - the stream would have a new entry every time the totals were updated:

```
sales_totals_updates := sales_totals -> (store stream);
```

```
tektite> (scan all from sales_totals_updates);
+--------------------------------------------------------------------------------------------------------------------+
| offset               | event_time                 | min(amount) | max(amount) | count(amount)        | sum(amount) |
+--------------------------------------------------------------------------------------------------------------------+
| 0                    | 2024-05-14 07:36:02.928000 | 85.23       | 123.99      | 17                   | 1836.51     |
| 1                    | 2024-05-14 07:36:03.926000 | 85.23       | 123.99      | 18                   | 1921.74     |
| 2                    | 2024-05-14 07:36:04.878000 | 85.23       | 123.99      | 19                   | 2006.97     |
| 3                    | 2024-05-14 07:36:05.801000 | 85.23       | 123.99      | 20                   | 2092.20     |
| 4                    | 2024-05-14 07:36:06.624000 | 85.23       | 123.99      | 21                   | 2177.43     |
| 5                    | 2024-05-14 07:36:07.651000 | 85.23       | 123.99      | 22                   | 2262.66     |
| 6                    | 2024-05-14 07:36:08.521000 | 85.23       | 123.99      | 23                   | 2347.89     |
| 7                    | 2024-05-14 07:36:09.377000 | 85.23       | 123.99      | 24                   | 2433.12     |
+--------------------------------------------------------------------------------------------------------------------+
8 rows returned
```

Or you could expose the update as a Kafka topic, so they can be consumed by any Kafka consumer:

```
sales_totals_updates := sales_totals -> (kafka out);
```

## Grouping data in aggregations

Often, instead of computing a single row in an aggregation, you want to calculate the aggregate functions over subsets
of the data.

For example, you might want to calculate `min`, `max`, `count` and `sum` of sales keyed by country. To do this
you provide one or more key expressions in the `aggregate` as follows:

```
sales_totals := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount) by country)
```

```
tektite> (scan all from sales_totals);
+-------------------------------------------------------------------------------------------------------------------+
| event_time                 | country       | min(amount)   | max(amount)   | count(amount)        | sum(amount)   |
+-------------------------------------------------------------------------------------------------------------------+
| 2024-05-14 07:53:54.146000 | uk            | 56.23         | 85.23         | 6                    | 415.38        |
| 2024-05-14 07:54:35.669000 | usa           | 23.23         | 56.23         | 5                    | 215.15        |
+-------------------------------------------------------------------------------------------------------------------+
```

Note that we repartition the incoming data by the `country` field as that is what we are grouping by. This ensures all incoming
data for the same value of `country` ends up on the same partition so the aggregation can be calculated correctly.

The partition step won't be necessary if the incoming data is already partitioned on the `country` column.

You can also key by multiple expressions:

```
city_totals := sales_topic ->
    (partition by country, city partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount) by country, city)
```

```
tektite> (scan all from city_totals);
+------------------------------------------------------------------------------------------------------------------------------+
| event_time                 | country      | city         | min(amount)  | max(amount)  | count(amount)        | sum(amount)  |
+------------------------------------------------------------------------------------------------------------------------------+
| 2024-05-14 07:53:54.146000 | uk           | london       | 76.23        | 85.23        | 3                    | 246.69       |
| 2024-05-14 07:53:44.438000 | uk           | manchester   | 56.23        | 56.23        | 3                    | 168.69       |
| 2024-05-14 07:54:24.980000 | usa          | austin       | 23.23        | 23.23        | 2                    | 46.46        |
| 2024-05-14 07:54:35.669000 | usa          | miami        | 56.23        | 56.23        | 3                    | 168.69       |
+------------------------------------------------------------------------------------------------------------------------------+
```

## Windowed aggregations

Very commonly, you don't want to compute aggregations over *all* data but over some window, e.g. in a window of a day or
an hour. You accomplish this with a windowed aggregation. 

A windowed aggregation is similar to a non-windowed aggregation but has extra parameters to define the window `size` and
`hop`.

Window `size` is a duration that represents how long a window stays open, for example, if you wanted to aggregate
sales by hour, the window size would be 1 hour.

Window `hop` is also a duration that represents the time between opening new windows. If `hop` is equal to `size` then at
any particular time we only have one window open. This is known in other systems as a hopping window.

If `hop` is less than size, then we can have multiple open windows at any one time. For example, we might have `size` set 
to 1 hour, and `hop` set to 10 minutes. This gives us more timely results as we don't have to wait an hour to get the most
recent sales results, we will get updated results every 10 minutes.

Here's an example of the sales aggregation from before, but this time using a windowed aggregation:

```
sales_by_country_by_hour := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m)
```

Note that durations are expressed as a positive integer followed by one of `ms` for milliseconds, `s` for seconds,
`m` for minutes, `h` for hours or `d` for days.

Tektite maintains aggregate values for each open window, and results are emitted and visible when the window is closed.
How do we determinate that?

## Watermarks

Tektite uses [watermarks](watermarks.md) to determine when aggregate windows can be closed.

At any one time, there can be multiple windows open for an aggregation depending on the values of `size` and `hop`.

As data arrives at the `aggregate` operator it first determines which window(s) the event intersects with.
Each window is defined by the start time of the window `ws` and the end time of the window `we`.

A row intersects with a window if `ws` < `event_time` <= `we`. Each incoming row has an `event_time` column.

For each intersecting window, the aggregate expressions are evaluated for the incoming row.

It's not only rows that flow through the network of interconnecting streams in Tektite. Tektite also injects watermarks
at the places where data enters the system - `bridge from` and `kafka in` operators, and these flow through the streams
along all the routes that data can flow.

A watermark carries a timestamp with it. This timestamp means that no more data with an `event_time` less than this timestamp
is expected to flow through the operator. 

When the `aggregate` operator receives a watermark it can close all open windows whose `we` timestamp is <= the watermark timestamp.

In some systems, events can arrive late, and we might not want to close the window until some time after the watermark, this 
is done by specifying the `lateness` parameter.

Here we don't close open windows until 10 seconds after the timestamp carried by the watermark.

```
sales_by_country_by_hour := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m lateness = 10s)
```

By default, watermarks are injected in `bridge from` and `kafka in` operators approximately 1 second after the `event_time`
of the highest `event_time` seen on incoming data. This can be configured on those operators. For more information on
watermarks see the section on [generating watermarks](watermarks.md)

When a window is closed the aggregate values for the last closed window becomes visible and can be queried:

```
tektite> (scan all from sales_by_country_by_hour);
+-----------------------------------------------------------------------------------------------------------------------------------------------+
| event_time                 | country              | min(amount)          | max(amount)          | count(amount)        | sum(amount)          |
+-----------------------------------------------------------------------------------------------------------------------------------------------+
| 2024-05-14 10:51:17.962000 | uk                   | 56.23                | 85.23                | 5                    | 339.15               |
| 2024-05-14 10:51:29.687000 | usa                  | 23.23                | 23.23                | 2                    | 46.46                |
+-----------------------------------------------------------------------------------------------------------------------------------------------+
```

Note that the aggregate results are keyed on `country` so we only store the most recent aggregate results per country. The
`event_time` column represents the highest `event_time` for any row which contributed towards that row of the aggregate results.

We could also create a new stream that receives updates from the aggregation or expose it as a Kafka consumer endpoint or siphon the results
off to an external topic.

Sometimes you may want to retain aggregation results from all closed windows, not just the most recent one. You do this by
specifying the `window_cols` parameter set to `true`. 

```
sales_by_country_by_hour := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m window_cols = true)
```

Then closed windows will be output with additional columns `ws` and `we` and the key of the data will be `[ws, we, country]`

By default, a windowed aggregation stores results persistently in a table with the same name as the stream, when windows
are closed. Sometimes you don't want results to be stored, you just want to emit results somewhere else. For example you
might want to send results to an external Kafka topic or expose results as a Kafka topic in Tektite.

You set the `store` parameter to `false` to prevent the aggregate storing results.

Here's an example of creating a new (read-only) topic that exposes the latest aggregate sales figures from the existing topic
`sales_topic`:
```
sales_by_country_by_hour_topic := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m store = false) ->
    (kafka out)
```

And here's an example of computing latest aggregate sales figures and outputting them directly to an external topic in Apache Kafka

```
sales_by_country_by_hour_out := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m store = false) ->
    (bridge to external_sales_figures props = ("bootstrap.servers" = "foo.com:9092")))
```

Here's an example of computing latest aggregate sales figures without persisting them and using a `store table` operator
to explicitly persist the results in a table. 

```
sales_by_country_by_hour_with_table := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m store = false) ->
    (store table by country)
```

## Data retention

If you don't want to keep the results of your aggregation forever, you can set a maximum retention time on it. Data will be deleted
asynchronously from the aggregation once that time has been exceeded.

The following will delete the aggregate results after 1 day:

```
sales_by_country_by_hour := sales_topic ->
    (partition by country partitions = 16) ->
    (aggregate min(amount), max(amount), count(amount), sum(amount)
        by country size = 1h hop = 10m retention = 1d)
```



