# Transforming data with projections

A projection evaluates a list of expressions on the incoming rows in order to produce output rows. There's one expression
for each output column. Projections are implemented using the `project` operator.

By evaluating expressions to create new rows, data is transformed from one form to another.

Expressions are composed of constants, column identifiers, [operators](expressions.md#operators) and functions. Any [built-in function](functions.md) can be used, and you
can also create your own functions using [WebAssembly](wasm.md).

For an in-depth explanation of the
expression language please see the section on [expressions](expressions.md). To learn more about creating custom WebAssembly 
functions please see the section on [WebAssembly](wasm.md)

To change the name of the column, use the `as` operator. Otherwise, the name of the expression will be used for the
new column name.

Here are some examples:

Extracting fields from an incoming JSON object:
```
cust_updates := (bridge from ext_cust_updates ...) ->
    (project to_string(key) as cust_id,
             json_string("name", val) as cust_name,
             json_int("age", val) as cust_age,
             json_string("address", val) as cust_address) ->
    (store stream)          
```

Just changing the order of the columns / changing column names:
```
cust_updates2 := cust_updates ->
    (project cust_name, cust_age as cage, cust_id, cust_address) ->
    (store stream)    
```

Transforming data using a custom WebAssembly function:
```
adjusted_sales := sales ->
    (project id, my_wasm_func(id, amount, price, event_time)) ->
    (store stream)
```

Projections can also be used in queries, for example:

Omit all columns in the results apart from `cust_id` and `name`:
```
(scan all from cust_data) -> (project cust_id, name)
```

Get a specific customer and upper case the name column and rename it
```
(get "cust1234" from cust_data) -> (project cust_id, to_upper(name) as uname)
```

