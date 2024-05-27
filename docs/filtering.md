# Filters

Rows are filtered using the `filter` operator. In the operator definition you write `filter by` followed by a single expression.
The expression is evaluated on incoming rows, and rows are passed through if the expression evaluates to `true`, otherwise they are ignored.

Expressions are composed of constants, column identifiers, [operators](expressions.md#operators) and functions. Any [built-in function](functions.md) can be used, and you
can also create your own functions using [WebAssembly](wasm.md).

For an in-depth explanation of the
expression language please see the section on [expressions](expressions.md). To learn more about creating custom WebAssembly
functions please see the section on [WebAssembly](wasm.md)

Here are some examples:

Extract UK sales into a new stream
```
uk_sales := sales -> (filter by country == "UK") -> (store stream)
```

Filter using a custom WebAssembly function `fraud_score`
```
fraudulent_tx := tx -> (filter by fraud_score(name, age, price, product_id) > 75) -> (store stream)
```

