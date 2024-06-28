// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestShunting(t *testing.T) {
	testShunting(t, "3", "3")
	testShunting(t, "f1", "f1")
	testShunting(t, "3 * (4 + 5)", "3", "(", "4", "5", "+", ")", "*")
	testShunting(t, "(3 * 4) + 5", "(", "3", "4", "*", ")", "5", "+")
	testShunting(t, "3 + 4 * 5", "3", "4", "5", "*", "+")
	testShunting(t, "(3 + 4) * 5", "(", "3", "4", "+", ")", "5", "*")
}

func TestShuntingFunction(t *testing.T) {
	testShunting(t, `1 + to_int("3", to_string("x", 12), "y")`, "1", "(", `"3"`, "(", `"x"`, "12", ")", "to_string", `"y"`, ")", "to_int", "+")
}

func testShunting(t *testing.T, input string, expected ...string) {
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	parser := NewParser(nil)
	ctx := NewParseContext(parser, input, tokens)
	output, err := parser.shuntExpression(ctx)
	require.NoError(t, err)
	checkExpectedTokens(t, output, expected...)
}

func checkExpectedTokens(t *testing.T, tokens []lexer.Token, expected ...string) {
	require.Equal(t, len(expected), len(tokens))
	for i, s := range expected {
		tok := tokens[i]
		require.Equal(t, s, tok.Value)
	}
}

func TestParseConstantExpression(t *testing.T) {
	testParseExpression(t, "23", &IntegerConstExprDesc{Value: 23})
	testParseExpression(t, "0", &IntegerConstExprDesc{Value: 0})
	testParseExpression(t, "123456", &IntegerConstExprDesc{Value: 123456})

	testParseExpression(t, "+23", &IntegerConstExprDesc{Value: 23})
	testParseExpression(t, "+0", &IntegerConstExprDesc{Value: 0})
	testParseExpression(t, "+123456", &IntegerConstExprDesc{Value: 123456})

	testParseExpression(t, "-23", &IntegerConstExprDesc{Value: -23})
	testParseExpression(t, "-0", &IntegerConstExprDesc{Value: 0})
	testParseExpression(t, "-123456", &IntegerConstExprDesc{Value: -123456})

	testParseExpression(t, "1.23f", &FloatConstExprDesc{Value: 1.23})
	testParseExpression(t, "0.00001f", &FloatConstExprDesc{Value: 0.00001})
	testParseExpression(t, "123456.32f", &FloatConstExprDesc{Value: 123456.32})
	testParseExpression(t, "3.23e8f", &FloatConstExprDesc{Value: 3.23e8})

	testParseExpression(t, "+1.23f", &FloatConstExprDesc{Value: 1.23})
	testParseExpression(t, "+0.00001f", &FloatConstExprDesc{Value: 0.00001})
	testParseExpression(t, "+123456.32f", &FloatConstExprDesc{Value: 123456.32})
	testParseExpression(t, "+3.23e8f", &FloatConstExprDesc{Value: 3.23e8})

	testParseExpression(t, "-1.23f", &FloatConstExprDesc{Value: -1.23})
	testParseExpression(t, "-0.00001f", &FloatConstExprDesc{Value: -0.00001})
	testParseExpression(t, "-123456.32f", &FloatConstExprDesc{Value: -123456.32})
	testParseExpression(t, "-3.23e8f", &FloatConstExprDesc{Value: -3.23e8})

	testParseExpression(t, `"foo"`, &StringConstExprDesc{Value: `foo`})
	testParseExpression(t, `""`, &StringConstExprDesc{Value: ``})
	testParseExpression(t, `"\"quoted\""`, &StringConstExprDesc{Value: `"quoted"`})
	testParseExpression(t, `"apples \"quoted\" pears"`, &StringConstExprDesc{Value: `apples "quoted" pears`})

	testParseExpression(t, "1 + -3", &BinaryOperatorExprDesc{
		Left:  &IntegerConstExprDesc{Value: 1},
		Right: &IntegerConstExprDesc{Value: -3},
		Op:    "+",
	})
	testParseExpression(t, "1+-3", &BinaryOperatorExprDesc{
		Left:  &IntegerConstExprDesc{Value: 1},
		Right: &IntegerConstExprDesc{Value: -3},
		Op:    "+",
	})
	testParseExpression(t, "1+-0.23e7f", &BinaryOperatorExprDesc{
		Left:  &IntegerConstExprDesc{Value: 1},
		Right: &FloatConstExprDesc{Value: -0.23e7},
		Op:    "+",
	})
	testParseExpression(t, "1-+3", &BinaryOperatorExprDesc{
		Left:  &IntegerConstExprDesc{Value: 1},
		Right: &IntegerConstExprDesc{Value: 3},
		Op:    "-",
	})
	testParseExpression(t, "1-+0.23e7f", &BinaryOperatorExprDesc{
		Left:  &IntegerConstExprDesc{Value: 1},
		Right: &FloatConstExprDesc{Value: 0.23e7},
		Op:    "-",
	})
	// Make sure func args with -ve number constants are parsed correctly
	testParseExpression(t, "case(f1,-1.23f,-3,-1)",
		&FunctionExprDesc{
			FunctionName: "case",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&FloatConstExprDesc{Value: -1.23},
				&IntegerConstExprDesc{Value: -3},
				&IntegerConstExprDesc{Value: -1},
			},
		},
	)

	testParseExpression(t, "true", &BoolConstExprDesc{Value: true})
	testParseExpression(t, "false", &BoolConstExprDesc{Value: false})

	testParseExpression(t, "f1 == true", &BinaryOperatorExprDesc{
		Left:  &IdentifierExprDesc{IdentifierName: "f1"},
		Right: &BoolConstExprDesc{Value: true},
		Op:    "==",
	})
}

func TestParseColumnExpression(t *testing.T) {
	testParseExpression(t, "x", &IdentifierExprDesc{IdentifierName: "x"})
	testParseExpression(t, "f1", &IdentifierExprDesc{IdentifierName: "f1"})
	testParseExpression(t, "someCol", &IdentifierExprDesc{IdentifierName: "someCol"})
	testParseExpression(t, "_someCol", &IdentifierExprDesc{IdentifierName: "_someCol"})
	testParseExpression(t, "some_col", &IdentifierExprDesc{IdentifierName: "some_col"})
	testParseExpression(t, "some_col23", &IdentifierExprDesc{IdentifierName: "some_col23"})
	testParseExpression(t, "some.col", &IdentifierExprDesc{IdentifierName: "some.col"})
}

func TestParseSimpleBinaryExpression(t *testing.T) {
	expected := &BinaryOperatorExprDesc{
		Left:  &IntegerConstExprDesc{Value: 1},
		Right: &IntegerConstExprDesc{Value: 2},
		Op:    "+",
	}
	testParseExpression(t, "1 + 2", expected)
}

func TestParseSimpleBinaryExpressionWithIdentifierAndFloat(t *testing.T) {
	expected := &BinaryOperatorExprDesc{
		Left:  &IdentifierExprDesc{IdentifierName: "$x"},
		Right: &FloatConstExprDesc{Value: 1.1},
		Op:    "+",
	}
	testParseExpression(t, "$x + 1.1f", expected)
}

func TestParseSimpleBinaryExpressions(t *testing.T) {
	expected := &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 1},
		Right: &BinaryOperatorExprDesc{
			Left: &IntegerConstExprDesc{Value: 2},
			Right: &BinaryOperatorExprDesc{
				Left:  &IntegerConstExprDesc{Value: 3},
				Right: &IntegerConstExprDesc{Value: 4},
				Op:    "+",
			},
			Op: "+",
		},
		Op: "+",
	}
	testParseExpression(t, "1 + 2 + 3 + 4", expected)
}

func TestParseWithPrecedence(t *testing.T) {

	expected := &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 1},
		Right: &BinaryOperatorExprDesc{
			Left:  &IntegerConstExprDesc{Value: 2},
			Right: &IntegerConstExprDesc{Value: 3},
			Op:    "*",
		},
		Op: "+",
	}

	testParseExpression(t, "1 + 2 * 3", expected)

	expected = &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 1},
		Right: &BinaryOperatorExprDesc{
			Left: &IntegerConstExprDesc{Value: 2},
			Right: &BinaryOperatorExprDesc{
				Left:  &IntegerConstExprDesc{Value: 3},
				Right: &IntegerConstExprDesc{Value: 4},
				Op:    "*",
			},
			Op: "*",
		},
		Op: "+",
	}

	testParseExpression(t, "1 + 2 * 3 * 4", expected)
}

func TestParseWithParens(t *testing.T) {

	expected := &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 1},
		Right: &BinaryOperatorExprDesc{
			Left:  &IntegerConstExprDesc{Value: 2},
			Right: &IntegerConstExprDesc{Value: 3},
			Op:    "+",
		},
		Op: "+",
	}

	testParseExpression(t, "1 + (2 + 3)", expected)

	expected = &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 1},
		Right: &BinaryOperatorExprDesc{
			Left: &BinaryOperatorExprDesc{
				Left:  &IntegerConstExprDesc{Value: 2},
				Right: &IntegerConstExprDesc{Value: 3},
				Op:    "+",
			},
			Right: &BinaryOperatorExprDesc{
				Left: &BinaryOperatorExprDesc{
					Left:  &IntegerConstExprDesc{Value: 4},
					Right: &IntegerConstExprDesc{Value: 5},
					Op:    "+",
				},
				Right: &BinaryOperatorExprDesc{
					Left:  &IntegerConstExprDesc{Value: 6},
					Right: &IntegerConstExprDesc{Value: 7},
					Op:    "+",
				},
				Op: "+",
			},
			Op: "+",
		},
		Op: "+",
	}
	testParseExpression(t, "1 + (2 + 3) + ((4 + 5) + (6 + 7))", expected)
}

func TestParseWithParensAndPrecedence(t *testing.T) {
	expected := &BinaryOperatorExprDesc{
		Left: &BinaryOperatorExprDesc{
			Left:  &IntegerConstExprDesc{Value: 2},
			Right: &IntegerConstExprDesc{Value: 3},
			Op:    "+",
		},
		Right: &IntegerConstExprDesc{Value: 4},
		Op:    "*",
	}
	testParseExpression(t, "(2 + 3) * 4", expected)

	expected = &BinaryOperatorExprDesc{
		Left: &BinaryOperatorExprDesc{
			Left:  &IntegerConstExprDesc{Value: 2},
			Right: &IntegerConstExprDesc{Value: 3},
			Op:    "*",
		},
		Right: &IntegerConstExprDesc{Value: 4},
		Op:    "+",
	}
	testParseExpression(t, "(2 * 3) + 4", expected)
}

func TestParseUnaryOperator(t *testing.T) {
	var expected ExprDesc
	expected = &UnaryOperatorExprDesc{
		Operand: &BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "x"},
			Right: &IdentifierExprDesc{IdentifierName: "y"},
			Op:    "==",
		},
		Op: "!",
	}
	testParseExpression(t, "!(x == y)", expected)

	expected = &UnaryOperatorExprDesc{
		Operand: &IdentifierExprDesc{IdentifierName: "x"},
		Op:      "!",
	}
	testParseExpression(t, "!x", expected)

	expected = &BinaryOperatorExprDesc{
		Left: &UnaryOperatorExprDesc{
			Operand: &IdentifierExprDesc{IdentifierName: "x"},
			Op:      "!",
		},
		Right: &IntegerConstExprDesc{Value: 5},
		Op:    "*",
	}
	testParseExpression(t, "!x * 5", expected)

	expected = &UnaryOperatorExprDesc{
		Operand: &BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "x"},
			Right: &IntegerConstExprDesc{Value: 5},
			Op:    "*",
		},
		Op: "!",
	}
	testParseExpression(t, "!(x * 5)", expected)

	expected = &UnaryOperatorExprDesc{
		Operand: &UnaryOperatorExprDesc{
			Operand: &UnaryOperatorExprDesc{
				Operand: &IdentifierExprDesc{IdentifierName: "x"},
				Op:      "!",
			},
			Op: "!",
		},
		Op: "!",
	}
	testParseExpression(t, "!!!x", expected)

	expected = &UnaryOperatorExprDesc{
		Operand: &UnaryOperatorExprDesc{
			Operand: &UnaryOperatorExprDesc{
				Operand: &IdentifierExprDesc{IdentifierName: "x"},
				Op:      "!",
			},
			Op: "!",
		},
		Op: "!",
	}
	testParseExpression(t, "!(!(!x))", expected)

	expected = &UnaryOperatorExprDesc{
		Operand: &IdentifierExprDesc{IdentifierName: "$z"},
		Op:      "!",
	}
	testParseExpression(t, "!$z", expected)
}

func TestParseUnaryPostfixOperator(t *testing.T) {
	var expected ExprDesc
	expected = &UnaryPostfixOperatorExprDesc{
		Operand: &BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "x"},
			Right: &IdentifierExprDesc{IdentifierName: "y"},
			Op:    "+",
		},
		Op: "asc",
	}
	testParseExpression(t, "x + y asc", expected)
}

func TestParseFunction(t *testing.T) {
	expected := &FunctionExprDesc{
		FunctionName: "sub_str",
		ArgExprs: []ExprDesc{
			&IdentifierExprDesc{IdentifierName: "s"},
			&IntegerConstExprDesc{Value: 3},
			&IntegerConstExprDesc{Value: 10},
		},
	}
	testParseExpression(t, `sub_str(s, 3, 10)`, expected)
}

func TestParseFunctionNested(t *testing.T) {
	expected := &FunctionExprDesc{
		FunctionName: "sub_str",
		ArgExprs: []ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sub_str",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "s"},
					&IntegerConstExprDesc{Value: 3},
					&IntegerConstExprDesc{Value: 10},
				},
			},
			&IntegerConstExprDesc{Value: 5},
			&FunctionExprDesc{
				FunctionName: "to_int",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f"},
				},
			},
		},
	}
	testParseExpression(t, `sub_str(sub_str(s, 3, 10), 5, to_int(f))`, expected)
}

func TestParseFunctionParamsWithOperators(t *testing.T) {
	expected := &FunctionExprDesc{
		FunctionName: "sub_str",
		ArgExprs: []ExprDesc{
			&IdentifierExprDesc{IdentifierName: "s"},
			&BinaryOperatorExprDesc{
				Left:  &IntegerConstExprDesc{Value: 3},
				Right: &IntegerConstExprDesc{Value: 5},
				Op:    "+",
			},
			&BinaryOperatorExprDesc{
				Left:  &IntegerConstExprDesc{Value: 10},
				Right: &IntegerConstExprDesc{Value: 1},
				Op:    "-",
			},
		},
	}
	testParseExpression(t, `sub_str(s, 3 + 5, 10 - 1)`, expected)
}

func TestParseFunctionParamsWithOperatorsAndNested(t *testing.T) {
	expected := &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 5},
		Right: &FunctionExprDesc{
			FunctionName: "sub_str",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "s"},
				&BinaryOperatorExprDesc{
					Left: &IntegerConstExprDesc{Value: 3},
					Right: &FunctionExprDesc{
						FunctionName: "json_int",
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{IdentifierName: "f2"},
							&StringConstExprDesc{Value: "foo"},
						},
					},
					Op: "+",
				},
				&FunctionExprDesc{
					FunctionName: "to_int",
					ArgExprs: []ExprDesc{
						&FunctionExprDesc{
							FunctionName: "now",
						},
					},
				},
			},
		},
		Op: "+",
	}
	testParseExpression(t, `5 + sub_str(s, 3 + json_int(f2, "foo"), to_int(now()))`, expected)
}

func TestParseFunctionZeroParams(t *testing.T) {
	expected := &FunctionExprDesc{
		FunctionName: "now",
	}
	testParseExpression(t, `now()`, expected)
}

func TestParseExpressionWithExtraParens(t *testing.T) {
	expected := &BinaryOperatorExprDesc{
		Left: &IntegerConstExprDesc{Value: 23},
		Right: &FunctionExprDesc{
			FunctionName: "sub_str",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "s"},
				&BinaryOperatorExprDesc{
					Left:  &IntegerConstExprDesc{Value: 3},
					Right: &IntegerConstExprDesc{Value: 5},
					Op:    "+",
				},
				&BinaryOperatorExprDesc{
					Left:  &IntegerConstExprDesc{Value: 10},
					Right: &IntegerConstExprDesc{Value: 1},
					Op:    "-",
				},
			},
		},
		Op: "+",
	}
	testParseExpression(t, `(23 + (sub_str((s), (3 + ((5))), ((10) - 1))))`, expected)
}

func TestExtractExpressions(t *testing.T) {
	testExtractExpressions(t, "x)", "x")
	testExtractExpressions(t, "23)", "23")
	testExtractExpressions(t, "sum(x))", "sum(x)")
	testExtractExpressions(t, "sum(foo(x)+10))", "sum(foo(x)+10)")

	testExtractExpressions(t, "x by", "x")
	testExtractExpressions(t, "23 by", "23")
	testExtractExpressions(t, "sum(x) by", "sum(x)")
	testExtractExpressions(t, "sum(foo(x)+10) by", "sum(foo(x)+10)")

	testExtractExpressions(t, "to_string(f1) as foo)", "to_string(f1) as foo")

	testExtractExpressions(t, "10,23+10,23+foo(x),bar(x,y,z),(10))",
		"10", "23+10", "23+foo(x)", "bar(x,y,z)", "(10)")
}

func testExtractExpressions(t *testing.T, input string, expectedS ...string) {
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	ctx := NewParseContext(NewParser(nil), input, tokens)
	for i, expected := range expectedS {
		toks, more, err := ExtractExpressionTokens(ctx)
		require.NoError(t, err)
		if i == len(expectedS)-1 {
			require.False(t, more)
		} else {
			require.True(t, more)
		}
		var sb strings.Builder
		for i, tok := range toks {
			isAs := tok.Value == "as"
			if isAs {
				sb.WriteRune(' ')
			}
			sb.WriteString(tok.Value)
			if isAs && i != len(toks)-1 {
				sb.WriteRune(' ')
			}
		}
		require.Equal(t, expected, sb.String())
	}
}

func TestParseExpressionList(t *testing.T) {
	input := "10,23+10,23+to_int(x),sub_str(x,y,z),(10),!$z,v0==1004 as eq)"
	expected := []ExprDesc{
		&IntegerConstExprDesc{Value: 10},
		&BinaryOperatorExprDesc{
			Left:  &IntegerConstExprDesc{Value: 23},
			Right: &IntegerConstExprDesc{Value: 10},
			Op:    "+",
		},
		&BinaryOperatorExprDesc{
			Left: &IntegerConstExprDesc{Value: 23},
			Right: &FunctionExprDesc{
				FunctionName: "to_int",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "x"},
				},
			},
			Op: "+",
		},
		&FunctionExprDesc{
			FunctionName: "sub_str",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "x"},
				&IdentifierExprDesc{IdentifierName: "y"},
				&IdentifierExprDesc{IdentifierName: "z"},
			},
		},
		&IntegerConstExprDesc{Value: 10},
		&UnaryOperatorExprDesc{
			Operand: &IdentifierExprDesc{IdentifierName: "$z"},
			Op:      "!",
		},
		&BinaryOperatorExprDesc{
			Left: &BinaryOperatorExprDesc{
				Left:  &IdentifierExprDesc{IdentifierName: "v0"},
				Right: &IntegerConstExprDesc{Value: 1004},
				Op:    "==",
			},
			Right: &IdentifierExprDesc{IdentifierName: "eq"},
			Op:    "as",
		},
	}
	testParseExpressionList(t, input, expected)

	input = "x, 10 by"
	expected = []ExprDesc{
		&IdentifierExprDesc{IdentifierName: "x"},
		&IntegerConstExprDesc{Value: 10},
	}
	testParseExpressionList(t, input, expected)

	input = "sub_str(f, 1, 10))"
	expected = []ExprDesc{
		&FunctionExprDesc{
			FunctionName: "sub_str",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f"},
				&IntegerConstExprDesc{Value: 1},
				&IntegerConstExprDesc{Value: 10},
			},
		},
	}
	testParseExpressionList(t, input, expected)

	input = `y,sub_str(f2, 10, if(f4, 1 - -23, 17)) as x,z,true,-1.23e7f,"bar")`
	expected = []ExprDesc{
		&IdentifierExprDesc{IdentifierName: "y"},
		&BinaryOperatorExprDesc{
			Left: &FunctionExprDesc{
				FunctionName: "sub_str",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f2"},
					&IntegerConstExprDesc{Value: 10},
					&FunctionExprDesc{
						FunctionName: "if",
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{IdentifierName: "f4"},
							&BinaryOperatorExprDesc{
								Left:  &IntegerConstExprDesc{Value: 1},
								Right: &IntegerConstExprDesc{Value: -23},
								Op:    "-",
							},
							&IntegerConstExprDesc{Value: 17},
						},
					},
				},
			},
			Right: &IdentifierExprDesc{IdentifierName: "x"},
			Op:    "as",
		},
		&IdentifierExprDesc{IdentifierName: "z"},
		&BoolConstExprDesc{Value: true},
		&FloatConstExprDesc{Value: -1.23e7},
		&StringConstExprDesc{Value: "bar"},
	}
	testParseExpressionList(t, input, expected)

	input = "$x + 1.1f from"
	expected = []ExprDesc{
		&BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "$x"},
			Right: &FloatConstExprDesc{Value: 1.1},
			Op:    "+",
		},
	}
	testParseExpressionList(t, input, expected)
}

func testParseExpressionList(t *testing.T, input string, expected []ExprDesc) {
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	parser := NewParser(nil)
	ctx := NewParseContext(parser, input, tokens)
	exprs, _, err := parser.ParseExpressionList(ctx)
	require.NoError(t, err)
	for _, e := range exprs {
		ce, ok := e.(tokenClearable)
		if ok {
			ce.clearTokenState()
		}
	}
	require.Equal(t, expected, exprs)
}

func TestParseOperators(t *testing.T) {
	testParseExpression(t, "23 + 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "+"})
	testParseExpression(t, "23 - 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "-"})
	testParseExpression(t, "23 * 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "*"})
	testParseExpression(t, "23 / 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "/"})
	testParseExpression(t, "23 % 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "%"})
	testParseExpression(t, "23 < 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "<"})
	testParseExpression(t, "23 > 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: ">"})
	testParseExpression(t, "23 == 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "=="})
	testParseExpression(t, "23 <= 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "<="})
	testParseExpression(t, "23 >= 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: ">="})
	testParseExpression(t, "23 != 23",
		&BinaryOperatorExprDesc{Left: &IntegerConstExprDesc{Value: 23}, Right: &IntegerConstExprDesc{Value: 23}, Op: "!="})
	testParseExpression(t, "f1 as foo",
		&BinaryOperatorExprDesc{Left: &IdentifierExprDesc{IdentifierName: "f1"}, Right: &IdentifierExprDesc{IdentifierName: "foo"}, Op: "as"})
}

func TestParseFunctions(t *testing.T) {
	testParseExpression(t, "if(f1 == 10, f2, f3 - 7)",
		&FunctionExprDesc{
			FunctionName: "if",
			ArgExprs: []ExprDesc{
				&BinaryOperatorExprDesc{
					Left:  &IdentifierExprDesc{IdentifierName: "f1"},
					Right: &IntegerConstExprDesc{Value: 10},
					Op:    "==",
				},
				&IdentifierExprDesc{IdentifierName: "f2"},
				&BinaryOperatorExprDesc{
					Left:  &IdentifierExprDesc{IdentifierName: "f3"},
					Right: &IntegerConstExprDesc{Value: 7},
					Op:    "-",
				},
			},
		})

	testParseExpression(t, "is_null(f1)",
		&FunctionExprDesc{
			FunctionName: "is_null",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})

	testParseExpression(t, "is_not_null(f1)",
		&FunctionExprDesc{
			FunctionName: "is_not_null",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})

	testParseExpression(t, "in(f1, 10, 11, 12, f7 - to_int(f3), 14)",
		&FunctionExprDesc{
			FunctionName: "in",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IntegerConstExprDesc{Value: 10},
				&IntegerConstExprDesc{Value: 11},
				&IntegerConstExprDesc{Value: 12},
				&BinaryOperatorExprDesc{
					Left: &IdentifierExprDesc{IdentifierName: "f7"},
					Right: &FunctionExprDesc{
						FunctionName: "to_int",
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{IdentifierName: "f3"},
						},
					},
					Op: "-",
				},
				&IntegerConstExprDesc{Value: 14},
			},
		})

	testParseExpression(t, "case(f1, 10, 1, 20, 2, 30, f5 + 1, -1)",
		&FunctionExprDesc{
			FunctionName: "case",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IntegerConstExprDesc{Value: 10},
				&IntegerConstExprDesc{Value: 1},
				&IntegerConstExprDesc{Value: 20},
				&IntegerConstExprDesc{Value: 2},
				&IntegerConstExprDesc{Value: 30},
				&BinaryOperatorExprDesc{
					Left:  &IdentifierExprDesc{IdentifierName: "f5"},
					Right: &IntegerConstExprDesc{Value: 1},
					Op:    "+",
				},
				&IntegerConstExprDesc{Value: -1},
			},
		})

	testParseExpression(t, `decimal_shift(f1, 3, false)`,
		&FunctionExprDesc{
			FunctionName: "decimal_shift",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IntegerConstExprDesc{Value: 3},
				&BoolConstExprDesc{Value: false},
			},
		})

	testParseExpression(t, `starts_with(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "starts_with",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `ends_with(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "ends_with",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `trim(f1, "x")`,
		&FunctionExprDesc{
			FunctionName: "trim",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "x"},
			},
		},
	)
	testParseExpression(t, `ltrim(f1, "x")`,
		&FunctionExprDesc{
			FunctionName: "ltrim",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "x"},
			},
		},
	)
	testParseExpression(t, `rtrim(f1, "x")`,
		&FunctionExprDesc{
			FunctionName: "rtrim",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "x"},
			},
		},
	)
	testParseExpression(t, `to_lower(f1)`,
		&FunctionExprDesc{
			FunctionName: "to_lower",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
	)
	testParseExpression(t, `to_upper(f1)`,
		&FunctionExprDesc{
			FunctionName: "to_upper",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
	)
	testParseExpression(t, `sub_str(f1, 3, 10)`,
		&FunctionExprDesc{
			FunctionName: "sub_str",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IntegerConstExprDesc{Value: 3},
				&IntegerConstExprDesc{Value: 10},
			},
		},
	)
	testParseExpression(t, `replace(f1, "foo", "bar")`,
		&FunctionExprDesc{
			FunctionName: "replace",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
				&StringConstExprDesc{Value: "bar"},
			},
		},
	)
	testParseExpression(t, `sprintf("x:%d y:%s z:%f", f1, f2, f3)`,
		&FunctionExprDesc{
			FunctionName: "sprintf",
			ArgExprs: []ExprDesc{
				&StringConstExprDesc{Value: "x:%d y:%s z:%f"},
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IdentifierExprDesc{IdentifierName: "f2"},
				&IdentifierExprDesc{IdentifierName: "f3"},
			},
		},
	)

	testParseExpression(t, "to_int(f1)",
		&FunctionExprDesc{
			FunctionName: "to_int",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "to_float(f1)",
		&FunctionExprDesc{
			FunctionName: "to_float",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "to_string(f1)",
		&FunctionExprDesc{
			FunctionName: "to_string",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "to_decimal(f1, 30, 2)",
		&FunctionExprDesc{
			FunctionName: "to_decimal",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IntegerConstExprDesc{Value: 30},
				&IntegerConstExprDesc{Value: 2},
			},
		})
	testParseExpression(t, "to_bytes(f1)",
		&FunctionExprDesc{
			FunctionName: "to_bytes",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "to_timestamp(f1)",
		&FunctionExprDesc{
			FunctionName: "to_timestamp",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})

	testParseExpression(t, `format_date(f1, "2006-01-02T15:04:05Z07:00")`,
		&FunctionExprDesc{
			FunctionName: "format_date",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "2006-01-02T15:04:05Z07:00"},
			},
		})
	testParseExpression(t, `parse_date(f1, "2006-01-02T15:04:05Z07:00")`,
		&FunctionExprDesc{
			FunctionName: "parse_date",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "2006-01-02T15:04:05Z07:00"},
			},
		})
	testParseExpression(t, "year(f1)",
		&FunctionExprDesc{
			FunctionName: "year",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "month(f1)",
		&FunctionExprDesc{
			FunctionName: "month",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "day(f1)",
		&FunctionExprDesc{
			FunctionName: "day",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "hour(f1)",
		&FunctionExprDesc{
			FunctionName: "hour",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "minute(f1)",
		&FunctionExprDesc{
			FunctionName: "minute",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "second(f1)",
		&FunctionExprDesc{
			FunctionName: "second",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "millis(f1)",
		&FunctionExprDesc{
			FunctionName: "millis",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "now()",
		&FunctionExprDesc{
			FunctionName: "now",
		})

	testParseExpression(t, `json_int(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_int",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `json_float(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_float",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `json_bool(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_bool",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `json_string(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_string",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `json_raw(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_raw",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `json_is_null(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_is_null",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)
	testParseExpression(t, `json_type(f1, "foo")`,
		&FunctionExprDesc{
			FunctionName: "json_type",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "foo"},
			},
		},
	)

	testParseExpression(t, `kafka_build_headers("h1", f1, "h2", f2, "h3", f3)`,
		&FunctionExprDesc{
			FunctionName: "kafka_build_headers",
			ArgExprs: []ExprDesc{
				&StringConstExprDesc{Value: "h1"},
				&IdentifierExprDesc{IdentifierName: "f1"},
				&StringConstExprDesc{Value: "h2"},
				&IdentifierExprDesc{IdentifierName: "f2"},
				&StringConstExprDesc{Value: "h3"},
				&IdentifierExprDesc{IdentifierName: "f3"},
			},
		},
	)

	testParseExpression(t, `kafka_header("h1", f1)`,
		&FunctionExprDesc{
			FunctionName: "kafka_header",
			ArgExprs: []ExprDesc{
				&StringConstExprDesc{Value: "h1"},
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
	)

	testParseExpression(t, "bytes_slice(f1, 3, 10)",
		&FunctionExprDesc{
			FunctionName: "bytes_slice",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IntegerConstExprDesc{Value: 3},
				&IntegerConstExprDesc{Value: 10},
			},
		},
	)
	testParseExpression(t, "uint64_be(f1)",
		&FunctionExprDesc{
			FunctionName: "uint64_be",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
	)
	testParseExpression(t, "uint64_le(f1)",
		&FunctionExprDesc{
			FunctionName: "uint64_le",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
	)
}

func TestParseAggregateFunctions(t *testing.T) {
	testParseExpression(t, "sum(f1)",
		&FunctionExprDesc{
			FunctionName: "sum",
			Aggregate:    true,
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "count(f1)",
		&FunctionExprDesc{
			FunctionName: "count",
			Aggregate:    true,
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "max(f1)",
		&FunctionExprDesc{
			FunctionName: "max",
			Aggregate:    true,

			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "min(f1)",
		&FunctionExprDesc{
			FunctionName: "min",
			Aggregate:    true,
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
	testParseExpression(t, "avg(f1)",
		&FunctionExprDesc{
			FunctionName: "avg",
			Aggregate:    true,
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		})
}

func TestFailToParseUnmatchedParens(t *testing.T) {
	input := "1 + 2) + 3 + 4"
	expectedMsg := `unmatched ')' (line 1 column 6):
1 + 2) + 3 + 4
     ^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "((1 + 2) + 3"
	expectedMsg = `unmatched '(' (line 1 column 1):
((1 + 2) + 3
^`
	testFailToParseExpression(t, input, expectedMsg)
}

func TestFailToParseIncompleteBinaryOp(t *testing.T) {
	input := "3 +"
	expectedMsg := `incomplete expression (line 1 column 1):
3 +
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "3 + 4 + 5 + 6 +"
	expectedMsg = `incomplete expression (line 1 column 1):
3 + 4 + 5 + 6 +
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "3 + (4 +) + 5"
	expectedMsg = `incomplete expression (line 1 column 5):
3 + (4 +) + 5
    ^`
	testFailToParseExpression(t, input, expectedMsg)
}

func TestFailToParseIncompleteFunction(t *testing.T) {
	input := "sprintf(f1, 10, 20"
	expectedMsg := `unmatched '(' (line 1 column 8):
sprintf(f1, 10, 20
       ^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "sprintf"
	expectedMsg = `incomplete expression (line 1 column 1):
sprintf
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "23 + to_int"
	expectedMsg = `missing parentheses (line 1 column 6):
23 + to_int
     ^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "1 + * 2"
	expectedMsg = `incomplete expression (line 1 column 1):
1 + * 2
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "1 * * 2"
	expectedMsg = `incomplete expression (line 1 column 1):
1 * * 2
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "+"
	expectedMsg = `incomplete expression (line 1 column 1):
+
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "+ +"
	expectedMsg = `incomplete expression (line 1 column 3):
+ +
  ^`
	testFailToParseExpression(t, input, expectedMsg)
}

func TestFailToParseIncompleteUnaryOperator(t *testing.T) {
	input := "!"
	expectedMsg := `incomplete expression (line 1 column 1):
!
^`
	testFailToParseExpression(t, input, expectedMsg)

	input = "(f1 + !) * 2"
	expectedMsg = `incomplete expression (line 1 column 7):
(f1 + !) * 2
      ^`
	testFailToParseExpression(t, input, expectedMsg)
}

func testFailToParseExpression(t *testing.T, input string, expectedMsg string) {
	_, err := doParseExpression(input)
	require.Error(t, err)
	require.Equal(t, expectedMsg, err.Error())
}

func doParseExpression(input string) (ExprDesc, error) {
	tokens, err := Lex(input, true)
	if err != nil {
		return nil, err
	}
	parser := NewParser(nil)
	return parser.ParseExpression(NewParseContext(parser, input, tokens))
}

func testParseExpression(t *testing.T, input string, expected ExprDesc) {
	e, err := doParseExpression(input)
	require.NoError(t, err)
	ce, ok := e.(tokenClearable)
	if ok {
		ce.clearTokenState()
	}
	require.Equal(t, expected, e)
}
