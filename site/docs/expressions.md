# Expressions

Expressions are used in Tektite [projections](projections.md), [filters](filtering.md), [aggregations](aggregating.md) and [sorts](queries.md#the-sort-operator).

Expressions are composed of constants, column identifiers, operators and [function](functions.md) calls.

Expressions have a return type which is any of the Tektite data types: `int`, `float`, `bool`, `decimal`, `string`, `bytes` or `timestamp`.

When evaluated, an expression will return a value with a type equal to the return type, or `null`.

If any operand to an expression is null, the expression evaluates to `null`.

## Constants

These can be 

* String literals. Enclosed in double quotes, e.g. `"aardvarks"`. The quotes are not part of the string literal.
If your string literal contains quotes, they must be escaped with backslash e.g. `"\"hello\""`
* Integer literals. E.g. `23`, `134353`, `-45`, `0`
* Float literals. These are suffixed with `f`. E.g. `23.23f`, `-2.3e22f`
* Bool literals. `true` or `false`

## Column identifiers

A column identifier references a column in the schema that the expression applies to. It's just the name of the column:

## Operators

Tektite supports the following operators:

Arithmetic operators

* `+` - addition: supported for types `int`, `float`, `decimal`, `timestamp`
* `-` - subtraction: supported for types `int`, `float`, `decimal`, `timestamp`
* `*` - multiplication: supported for types `int`, `float`, `decimal`, `timestamp`
* `/` - division: supported for types `int`, `float`, `decimal`, `timestamp`
* `%` - modulus: supported for type `int`

Comparison operators

* `==` - equality: supported for `int`, `float`, `bool`, `decimal`, `string`, `bytes`, `timestamp`
* `!=` - inequality: supported for `int`, `float`, `bool`, `decimal`, `string`, `bytes`, `timestamp`
* `>`  - greater than: supported for `int`, `float`, `decimal`, `string`, `bytes`, `timestamp`
* `>=` - greater than or equal: supported for `int`, `float`, `decimal`, `string`, `bytes`, `timestamp`
* `<`  - less than: supported for `int`, `float`, `decimal`, `string`, `bytes`, `timestamp`
* `<=` - less than or equal: supported for `int`, ``float``, `decimal`, `string`, `bytes`, `timestamp`

`bool`ean operators - operands must be of type `bool`

* `&&` - and
* `||` - or
* `!` - not

## Function calls

Tektite comes with a library of built-in [functions](functions.md) - and you can also write your own custom functions
using WebAssembly and use them the same way as the built-in functions.

Functions are used in expressions by writing the function name followed by left parenthesis followed by expression list
followed by right parenthesis.

## Examples

Here are some example expressions:

```
my-stream := parent-stream ->
    (project col3 + 10, sub_str(col7, 10 * col2, 12), col9 == "uk") ->
    (store stream)
    
my-stream := parent-stream ->
    (filter by my_wasm_func(price * 1.23f) && !my_other_wasm_func(name)) ->
    (store stream)    
    
sales_tots := sales ->
    (aggregate count(price), sum(price + adjust) by 
        to_lower(country), if(is_capital, city, "other"))    
    
(scan all from sales_tots)
    -> (sort by sub_str(col2, 3, 10), to_lower(col2))     
    
```

