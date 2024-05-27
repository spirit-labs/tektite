# Queries

Any data in a [persistent stream or table](persistence.md) can be queried.

Tektite implements queries as transient streams that only exist as long as the query. Raw data from the query source
is fed into one end of the stream, and results are obtained from the other end.

Tektite two ways of reading data into the query stream - key lookups and range scans.

## Key lookups

A key lookup retrieves all matching rows given a key value.

For example, let's say we have a table that contains customer information and has a key `customer_id`

```
cust_info := cust_stream ->
    (partition by customer_id partitions = 16) ->
    (store table by customer_id)
```

And we want to look up a specific customer with `customer_id` = `cust54323`.

We use a `get` operator to do the lookup:

```
tektite> (get "cust54323" from cust_info);
+---------------------------------------------------------------------------------------------------------------------+
| event_time                 | customer_id         | name                | age                  | city                |
+---------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 09:10:07.375000 | cust54323           | joe bloggs          | 43                   | miami               |
+---------------------------------------------------------------------------------------------------------------------+
1 row returned
```

We can also look up in tables with composite keys. Let's say we have an aggregation with a composite key of `[country, city]`

```
sales_by_country_city := sales ->
   (partition by country partitions=16) ->
   (aggregate min(amount), max(amount), count(amount), sum(amount)
       by country, city);
```

We can use the `get` operator to look up a single row by specifying values for both `country` and `city`:

```
tektite> (get "usa", "austin" from sales_by_country_city);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 09:18:25.576000 | usa        | austin     | 23.23      | 23.23      | 1                    | 23.23      |
+--------------------------------------------------------------------------------------------------------------------+
1 row returned
```

We don't need to specify all the values for the key, we can omit ones on the right. This lets us look up all rows for a
particular country:

```
tektite> (get "usa" from sales_by_country_city);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 09:18:25.576000 | usa        | austin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 09:18:25.957000 | usa        | miami      | 56.23      | 56.23      | 1                    | 56.23      |
+--------------------------------------------------------------------------------------------------------------------+
2 rows returned
```

You can use a `project` operator to transform the data received:

```
tektite> (get "usa" from sales_by_country_city) ->
    (project country, to_upper(city) as ucity);
+---------------------------------------------------------------------------------------------------------------------+
| country                                                  | ucity                                                    |
+---------------------------------------------------------------------------------------------------------------------+
| usa                                                      | MIAMI                                                    |
| usa                                                      | AUSTIN                                                   |
+---------------------------------------------------------------------------------------------------------------------+
```

You might want to sort the results if you are returning more than one row:

```
tektite> (get "usa" from sales_by_country_city) ->
    (project country, to_upper(city) as ucity) ->
    (sort by ucity);
+---------------------------------------------------------------------------------------------------------------------+
| country                                                  | ucity                                                    |
+---------------------------------------------------------------------------------------------------------------------+
| usa                                                      | AUSTIN                                                   |
| usa                                                      | MIAMI                                                    |
+---------------------------------------------------------------------------------------------------------------------+
2 rows returned
```

## Table scans

Sometimes you may want to scan all the data in a table. You do this with the `scan all` operator:

```
tektite> (scan all from sales_by_country_city);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 09:18:25.535000 | uk         | manchester | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 09:18:25.576000 | usa        | austin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 09:18:25.957000 | usa        | miami      | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 09:18:25.557000 | uk         | london     | 76.23      | 85.23      | 2                    | 161.46     |
+--------------------------------------------------------------------------------------------------------------------+
4 rows returned
```

As with any query you can use `filter`, `project` or `sort` in the query stream:

```
tektite> (scan all from sales_by_country_city) ->
    (filter by city != "manchester") ->
    (project country, to_upper(city) as ucity) ->
    (sort by ucity);
+---------------------------------------------------------------------------------------------------------------------+
| country                                                  | ucity                                                    |
+---------------------------------------------------------------------------------------------------------------------+
| usa                                                      | AUSTIN                                                   |
| uk                                                       | LONDON                                                   |
| usa                                                      | MIAMI                                                    |
+---------------------------------------------------------------------------------------------------------------------+
3 rows returned
```

## Range scans

A range scan matches all key values within a range.

The key of this table is `[country, city]` so the following scan will match any countries with a country that is 
lexographically greater than or equal to `f` and less than `h`

```
tektite> (scan "f" to "h" from sales_by_country_city) -> (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.676000 | france     | paris      | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 10:27:24.699000 | france     | lyon       | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.720000 | germany    | berlin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 10:27:25.164000 | germany    | hamburg    | 56.23      | 56.23      | 1                    | 56.23      |
+--------------------------------------------------------------------------------------------------------------------+
4 rows returned
```

The range can include all components of a composite key, so the following query will just return the row for `lyon`

```
tektite> (scan "france", "lyon" to "france", "m" from
            sales_by_country_city) ->
         (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.699000 | france     | lyon       | 56.23      | 56.23      | 1                    | 56.23      |
+--------------------------------------------------------------------------------------------------------------------+
1 row returned
```

By default, the lower bound of the range is *inclusive* and the upper bound is *exclusive*. So the following query also
doesn't return the row for `paris`.

```
tektite> (scan "france", "lyon" to "france", "paris" from
             sales_by_country_city) ->
         (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.699000 | france     | lyon       | 56.23      | 56.23      | 1                    | 56.23      |
+--------------------------------------------------------------------------------------------------------------------+
1 row returned
```

The upper bound can be made inclusive with `incl`:

```
tektite> (scan "france", "lyon" to "france", "paris" incl from
             sales_by_country_city) ->
         (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.699000 | france     | lyon       | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.676000 | france     | paris      | 23.23      | 23.23      | 1                    | 23.23      |
+--------------------------------------------------------------------------------------------------------------------+
2 rows returned
```

The lower bound can be made exclusive with `excl`:

```
tektite> (scan "france", "lyon" excl to "france", "paris" incl from
             sales_by_country_city) ->
         (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.676000 | france     | paris      | 23.23      | 23.23      | 1                    | 23.23      |
+--------------------------------------------------------------------------------------------------------------------+
1 row returned
```

If you want to scan from a lower bound but don't want an upper bound you can use the special upper bound `end`

```
tektite> (scan "germany" to end from sales_by_country_city) ->
    (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:25.164000 | germany    | hamburg    | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.720000 | germany    | berlin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 09:18:25.535000 | uk         | manchester | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 09:18:25.557000 | uk         | london     | 76.23      | 85.23      | 2                    | 161.46     |
| 2024-05-15 09:18:25.576000 | usa        | austin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 09:18:25.957000 | usa        | miami      | 56.23      | 56.23      | 1                    | 56.23      |
+--------------------------------------------------------------------------------------------------------------------+
6 rows returned
```

You can also use the special lower bound `start` to scan from the first key up to some upper bound:

```
tektite> (scan start to "uk" incl from sales_by_country_city) -> (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.699000 | france     | lyon       | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.676000 | france     | paris      | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 10:27:25.164000 | germany    | hamburg    | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.720000 | germany    | berlin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 09:18:25.535000 | uk         | manchester | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 09:18:25.557000 | uk         | london     | 76.23      | 85.23      | 2                    | 161.46     |
+--------------------------------------------------------------------------------------------------------------------+
6 rows returned
```

You can also scan `start` to `end`. This identical to using a `scan all` operator.

```
tektite> (scan start to end from sales_by_country_city) -> (sort by country);
+--------------------------------------------------------------------------------------------------------------------+
| event_time                 | country    | city       | min(amou.. | max(amou.. | count(amount)        | sum(amou.. |
+--------------------------------------------------------------------------------------------------------------------+
| 2024-05-15 10:27:24.699000 | france     | lyon       | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.676000 | france     | paris      | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 10:27:25.164000 | germany    | hamburg    | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 10:27:24.720000 | germany    | berlin     | 23.23      | 23.23      | 1                    | 23.23      |
| 2024-05-15 09:18:25.557000 | uk         | london     | 76.23      | 85.23      | 2                    | 161.46     |
| 2024-05-15 09:18:25.535000 | uk         | manchester | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 09:18:25.957000 | usa        | miami      | 56.23      | 56.23      | 1                    | 56.23      |
| 2024-05-15 09:18:25.576000 | usa        | austin     | 23.23      | 23.23      | 1                    | 23.23      |
+--------------------------------------------------------------------------------------------------------------------+
8 rows returned
```

## The sort operator

This operator can only be used in queries, and is used to sort the data by a list of expressions. Each sort expression is
applied from left to right.

The expressions are standard Tektite expressions and support all the operators and functions, or custom WebAssembly functions
that you can use in any expression.

Some examples:

```
(scan all from latest_sensor_readings) ->
    (sort by country, to_upper(city))

(scan all from cust_data) ->
    (sort by my_custom_wasm_function(cust_name, cust_address, cust_age))
```

## Legal operators in queries

Queries don't support all the operators that you can use in static streams. The following operators are supported in queries:

* `get`
* `scan all`
* `scan`
* `project`
* `filter`
* `sort`

Notably we do not support aggregations or joins for queries. The Tektite approach is to maintain joins and aggregations
on the write path as static streams, and keep the read path (queries) lightweight.

## Using the Tektite HTTP API for queries

Queries can be executed at the [CLI](cli.md) for ad-hoc purposes, but to execute queries from your application, you use the [Tektite HTTP API](http_api.md), this also allows you to prepare statements to avoid
the cost of setting up the query each time.

If you're using golang in your application we also provide a simple golang client which allows you to execute and prepare statements and
queries. This is a simple convenience wrapper around the HTTP API which you can use directly if you prefer.