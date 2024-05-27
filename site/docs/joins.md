# Joins

Tektite allows you to join streams with other streams to create new streams, or to join streams with table to create new streams.

The streams can be any Tektite stream, so you can join two Tektite topics, a Tektite topic with an imported external
topic, two external topics (e.g. an Apache Kafka topic with a RedPanda topic), or any other stream with any other stream!

## Stream-stream joins

A stream-stream join, joins a stream with another stream to create a new stream. Rows are joined by matching one or more
key columns on the incoming streams and specifying a window.

For example, the following join creates an output row when there's a sale and a payment for the same transaction id in
a window of 5 minutes.

```
matched_sales :=
    (join sales with payments on tx_id=transaction_id within = 5m) ->
    (store stream);
```

In the above the left stream is `sales` and has a key column `tx_id` which contains the transaction id. The right stream
is `payments` and contains a column `transaction_id` which contains the transaction id.

A stream-stream join *must* have a `within` argument that specifies the size of the window. The window size is a duration
like `5m`, `12h` or `10s`.

A join creates output columns for each input column of the left and right streams. Columns that come from the left stream
are prefixed with `l_` and columns that come from the right stream are prefixed with `r_`.

If sales has the following schema:

```
event_time: timestamp
tx_id: int
product_id: string
price: decimal(10, 2)
customer_id: string
```

And payments has the following schema:

```
event_time: timestamp
transaction_id: int
payment_type: string
payment_provider: string
```

Then the output of the join will have the following schema:

```
event_time: timestamp
l_event_time: timestamp
l_tx_id: int
l_product_id: string
l_price: decimal(10, 2)
l_customer_id: string
r_event_time: timestamp
r_payment_type: string
r_payment_provider: string
```

Note that the join columns (`l_tx_id`) is only included once as it's the same for left and right.

Often you will only want a subset of the columns or want to change their names or order. A `project` operator is used to do that:

```
matched_sales :=
    (join sales with payments on tx_id = transaction_id within = 5m) ->
    (project l_tx_id as tx_id,
             l_customer_id as cust_id,
             l_price as price, 
             r_payment_type as payment_type) ->
    (store stream);
```

You can also join on multiple columns. In this case the join columns are separated by a comma.

```
matched_sales :=
    (join sales with payments
        on tx_id = transaction_id, country = country_id
        within = 5m) ->
    (project l_tx_id as tx_id,
             l_customer_id as cust_id,
             l_price as price, 
             r_payment_type as payment_type) ->
    (store stream);
```

## Stream-table joins

Tables are persisted by `aggregate` and `to table` operators. They differ from stream data in that they have a key so later rows with the
same key overwrite earlier rows, and they have no `offset` column.

With Tektite, you can join a stream with a table. This is often used to 'enrich' stream data with value looked up from the table.
The stream-table join can be an inner join, or an outer join.

With an inner join, a row is output from the join if the incoming stream data finds a match in the table.

With an outer join a row is output from the join whether or not a match is found in the table. If no match is found the columns in the output that
come from the table will be `null`.

Let's say we have a table `cust_data` which contains latest customer data.

```
cust_data := cust_updates_stream -> (to table key = "cust_id")
```

And a stream topic sales which contains sales events:

```
sales := (topic partitions = 16)
```

We can create a new stream that enriches the sales stream with customer data. We will use a projection to extract the
columns of interest:

```
enriched_sales := 
    (join sales with cust_data on c_id = customer_id) ->
    (project l_c_id as customer_id,
             r_cust_name as customer_name
             r_cust_address as customer_address
             l_tx_id as tx_id,
             l_amount as amount,
             l_price as price) ->
    (store stream)         
```

In the above we define the join columns with a single `=` (`c_id = customer_id`). This means it's an inner join and a row
will only be output if a matching customer row is found for an incoming sale.

If we want a row to be output irrespective of whether a matching customer row was found we use `=*` to define the join 
columns:

```
enriched_sales := 
    (join sales with cust_data on c_id *= customer_id) ->
    (project l_c_id as customer_id,
             r_cust_name as customer_name
             r_cust_address as customer_address
             l_tx_id as tx_id,
             l_amount as amount,
             l_price as price) ->
    (store stream)         
```

This means it's an outer join. The `*` must be on the stream side of the join.
