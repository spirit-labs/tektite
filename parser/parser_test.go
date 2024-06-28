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
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLexInvalidInput(t *testing.T) {
	input := "stream1 := (filter by f2 ;)"
	expected := `invalid statement (line 1 column 26):
stream1 := (filter by f2 ;)
                         ^`
	testLexInvalidInput(t, input, expected)

	input = `stream1 := (filter by f2) ->
(project f3, f7, @f9, f12, f17) ->
(store stream retention = 5h)`
	expected = `invalid statement (line 2 column 18):
(project f3, f7, @f9, f12, f17) ->
                 ^`
	testLexInvalidInput(t, input, expected)

	input = `@:@:@:@:@`
	expected = `invalid statement (line 1 column 1):
@:@:@:@:@
^`
	testLexInvalidInput(t, input, expected)
}

func TestLexInvalidFloat(t *testing.T) {
	input := `foo 0.23 bar`
	expected := `invalid float. please suffix float literals with 'f' (line 1 column 6):
foo 0.23 bar
     ^`
	testLexInvalidInput(t, input, expected)
}

func testLexInvalidInput(t *testing.T, input string, expectedMsg string) {
	tokens, err := Lex(input, true)
	require.Nil(t, tokens)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.ParseError))
	require.Equal(t, expectedMsg, err.Error())
}

func TestLexStreamAssignment(t *testing.T) {
	input := ":="
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, StreamAssignmentTokenType, tok.Type)
	require.Equal(t, ":=", tok.Value)
}

func TestLexDuration(t *testing.T) {
	input := "13456ms 4645s 23m 45h 76d"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"13456ms", "4645s", "23m", "45h", "76d"}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, DurationTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexPipe(t *testing.T) {
	input := "->"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, PipeTokenType, tok.Type)
	require.Equal(t, "->", tok.Value)
}

func TestLexBinaryOperators(t *testing.T) {
	input := "as != <= >= && || - + * / % < >"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"as", "!=", "<=", ">=", "&&", "||", "-", "+", "*", "/", "%", "<", ">"}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, BinaryOpTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
	// note == has ambiguity with join types
}

func TestArgAssignment(t *testing.T) {
	input := "="
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, ArgAssignmentTokenType, tok.Type)
	require.Equal(t, "=", tok.Value)
}

func TestLexIdentifier(t *testing.T) {
	input := "mystream MyStream myStream MYSTREAM my-stream my_stream _mystream mystream0 mystream1234 my_stream_4321 my.stream.uk $p1 x"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"mystream", "MyStream", "myStream", "MYSTREAM", "my-stream", "my_stream", "_mystream", "mystream0",
		"mystream1234", "my_stream_4321", "my.stream.uk", "$p1", "x"}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, IdentTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexIdentifierWithType(t *testing.T) {
	input := "$p1:int $p2:string $p3:decimal(10,2) $p4:decimal(12, 4)"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"$p1:int", "$p2:string", "$p3:decimal(10,2)", "$p4:decimal(12, 4)"}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, IdentTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexListSeparator(t *testing.T) {
	input := ","
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, ListSeparatorTokenType, tok.Type)
	require.Equal(t, ",", tok.Value)
}

func TestLexLParens(t *testing.T) {
	input := "("
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, LParensTokenType, tok.Type)
	require.Equal(t, "(", tok.Value)
}

func TestLexRParens(t *testing.T) {
	input := ")"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, RParensTokenType, tok.Type)
	require.Equal(t, ")", tok.Value)
}

func TestLexJoinType(t *testing.T) {
	input := "*= *="
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	// Note we omit == as there is ambiguity with binary op ==
	expected := []string{"*=", "*="}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, JoinTypeTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexUnaryOperator(t *testing.T) {
	input := "!"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	require.Equal(t, 1, len(tokens))
	tok := tokens[0]
	require.Equal(t, UnaryOpTokenType, tok.Type)
	require.Equal(t, "!", tok.Value)
}

func TestLexFloat(t *testing.T) {
	input := "0f 1f 123f 2323f 1.23f 1234.4321f 0.432f 1.3e7f"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"0f", "1f", "123f", "2323f", "1.23f", "1234.4321f", "0.432f", "1.3e7f"}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, FloatTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexInteger(t *testing.T) {
	input := "0 1 123 1233456 234566"
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"0", "1", "123", "1233456", "234566"}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, IntegerTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexStringLiteral(t *testing.T) {
	input := `"apples" "a pp \t ll \n \r es" "s1 := (filter by x + y > z) -> (store stream retention = 1h)" "\"in quotes\""`
	tokens, err := Lex(input, true)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{`"apples"`, `"a pp \t ll \n \r es"`,
		`"s1 := (filter by x + y > z) -> (store stream retention = 1h)"`, `"\"in quotes\""`}
	require.Equal(t, len(expected), len(tokens))
	for i, token := range tokens {
		require.Equal(t, StringLiteralTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestLexWhitespace(t *testing.T) {
	input := "  qsuhqsuhqsh sijis   ijijqsiqjs\tqsuhqs\nwudhwudh\riwjdijwd\t\tuhwduwhdhwu\n\nuhqwsuhqs\r\ruihqsuhq\"  \""
	tokens, err := Lex(input, false)
	require.NoError(t, err)
	require.NotNil(t, tokens)
	expected := []string{"  ", " ", "   ", "\t", "\n", "\r", "\t\t", "\n\n", "\r\r"}
	var out []lexer.Token
	for _, token := range tokens {
		if token.Type == WhitespaceTokenType {
			out = append(out, token)
		}
	}
	require.Equal(t, len(expected), len(out))
	for i, token := range out {
		require.Equal(t, WhitespaceTokenType, token.Type)
		require.Equal(t, expected[i], token.Value)
	}
}

func TestParseEmptyStatement(t *testing.T) {
	cs := NewCreateStreamDesc()
	err := NewParser(nil).Parse("", cs)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.ParseError))
	require.Equal(t, "statement is empty", err.Error())
}
