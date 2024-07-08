package parser

import (
	"fmt"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/spirit-labs/tektite/errors"
	"strings"
)

var lex = lexer.MustSimple([]lexer.SimpleRule{
	{"StreamAssignment", `:=`},
	{"Duration", `(?:[1-9][0-9]*(?:ms|s|m|h|d))`},
	{"Pipe", `->`},
	// Note - there is ambiguity for "==" as this appears is a valid expr, so we omit it from JoinType
	{"JoinType", `(?:\*=|=\*)`},
	{"UnaryPostfixOp", `(?:ascending|asc|descending|desc)`},
	{"BinaryOp", `(?:as|==|!=|<=|>=|&&|\|\||[-+\*/%<>])`},
	{"UnaryOp", `!`},
	{"ArgAssignment", `=`},
	{"BoolLiteral", `(?:true|false)`},
	{"Ident", `\$?[a-zA-Z_](?:[a-zA-Z0-9_.\-]*[a-zA-Z0-9])?(\:(?:int|float|bool|decimal\(\d+,\s*\d+\)|string|bytes|timestamp))?`},
	{"ListSeparator", `,`},
	{"LParens", `\(`},
	{"RParens", `\)`},
	{"Float", `[-+]?(\d+(\.\d*)?|\.\d+)(e[-+]?\d+)?f`},
	{"Integer", `[+-]?([0-9][0-9]*)`},
	{"InvalidFloat", `[-+]?(\d+(\.\d*)?|\.\d+)(e[-+]?\d+)?`},
	{"StringLiteral", `"(?:\\"|[^"])*"`},
	{"Whitespace", `[ \t\n\r]+`},
})

var StreamAssignmentTokenType lexer.TokenType
var DurationTokenType lexer.TokenType
var FloatTokenType lexer.TokenType
var InvalidFloatTokenType lexer.TokenType
var IntegerTokenType lexer.TokenType
var IdentTokenType lexer.TokenType
var ListSeparatorTokenType lexer.TokenType
var LParensTokenType lexer.TokenType
var RParensTokenType lexer.TokenType
var PipeTokenType lexer.TokenType
var JoinTypeTokenType lexer.TokenType
var BinaryOpTokenType lexer.TokenType
var StringLiteralTokenType lexer.TokenType
var WhitespaceTokenType lexer.TokenType
var ArgAssignmentTokenType lexer.TokenType
var UnaryOpTokenType lexer.TokenType
var UnaryPostfixOpTokenType lexer.TokenType
var BoolLiteralTokenType lexer.TokenType

func init() {
	StreamAssignmentTokenType = lex.Symbols()["StreamAssignment"]
	DurationTokenType = lex.Symbols()["Duration"]
	FloatTokenType = lex.Symbols()["Float"]
	InvalidFloatTokenType = lex.Symbols()["InvalidFloat"]
	IntegerTokenType = lex.Symbols()["Integer"]
	IdentTokenType = lex.Symbols()["Ident"]
	ListSeparatorTokenType = lex.Symbols()["ListSeparator"]
	LParensTokenType = lex.Symbols()["LParens"]
	RParensTokenType = lex.Symbols()["RParens"]
	PipeTokenType = lex.Symbols()["Pipe"]
	JoinTypeTokenType = lex.Symbols()["JoinType"]
	BinaryOpTokenType = lex.Symbols()["BinaryOp"]
	StringLiteralTokenType = lex.Symbols()["StringLiteral"]
	WhitespaceTokenType = lex.Symbols()["Whitespace"]
	ArgAssignmentTokenType = lex.Symbols()["ArgAssignment"]
	UnaryOpTokenType = lex.Symbols()["UnaryOp"]
	UnaryPostfixOpTokenType = lex.Symbols()["UnaryPostfixOp"]
	BoolLiteralTokenType = lex.Symbols()["BoolLiteral"]
}

func NewParser(externalFunctionChecker ExternalFunctionChecker) *Parser {
	return &Parser{externalFunctionChecker: externalFunctionChecker}
}

type ExternalFunctionChecker interface {
	FunctionExists(functionName string) bool
}

type Parser struct {
	externalFunctionChecker ExternalFunctionChecker
}

func (p *Parser) Parse(input string, parseable Parseable) error {
	if input == "" {
		return errors.NewTektiteErrorf(errors.ParseError, "statement is empty")
	}
	tokens, err := Lex(input, true)
	if err != nil {
		return err
	}
	return parseable.Parse(NewParseContext(p, input, tokens))
}

func (p *Parser) ParseTSL(input string) (*TSLDesc, error) {
	tsl := NewTSLDesc()
	err := p.Parse(input, tsl)
	return tsl, err
}

func (p *Parser) ParseQuery(input string) (*QueryDesc, error) {
	tsl := NewQueryDesc()
	err := p.Parse(input, tsl)
	return tsl, err
}

func Lex(input string, removeWhitespace bool) ([]lexer.Token, error) {
	// We Lex all tokens up-front. We will want them all anyway, and this makes the logic in parsing simpler as we
	// know how many tokens there are before parsing.
	l, err := lex.Lex("", strings.NewReader(input))
	if err != nil {
		return nil, err
	}
	tokens := make([]lexer.Token, 0, 20)
	for {
		token, err := l.Next()
		if err != nil {
			var le *lexer.Error
			ok := errors.As(err, &le)
			if !ok {
				return nil, err
			}
			if strings.Contains(le.Error(), "invalid input text") {
				return nil, errorAtPosition("invalid statement", le.Pos, input)
			}
			return nil, err
		}
		if token.Type == lexer.EOF {
			break
		}
		if token.Type == InvalidFloatTokenType {
			// We have a new token type for an invalid float (a float without a trailing 'f') so that we can catch
			// users specifying floats in this way and give them a nicer error.
			return nil, errorAtPosition("invalid float. please suffix float literals with 'f'", token.Pos, input)
		}
		if !removeWhitespace || token.Type != WhitespaceTokenType {
			tokens = append(tokens, token)
		}
	}
	return tokens, nil
}

func lineWithPosHighlight(input string, pos lexer.Position) string {
	if input == "" {
		return ""
	}
	lines := strings.Split(input, "\n")
	line := lines[pos.Line-1]
	line = strings.ReplaceAll(line, "\t", " ")
	line = strings.ReplaceAll(line, "\r", " ")
	sb := strings.Builder{}
	for i := 0; i < pos.Column-1; i++ {
		sb.WriteRune(' ')
	}
	sb.WriteRune('^')
	return fmt.Sprintf("%s\n%s", line, sb.String())
}

type Parseable interface {
	Parse(context *ParseContext) error
}

type ParseContext struct {
	parser  *Parser
	input   string
	tokens  []lexer.Token
	pos     int
	lastPos *lexer.Position
}

func NewParseContext(parser *Parser, input string, tokens []lexer.Token) *ParseContext {
	return &ParseContext{
		parser: parser,
		input:  input,
		tokens: tokens,
	}
}

func (pc *ParseContext) HasNext() bool {
	return pc.pos != len(pc.tokens)
}

func (pc *ParseContext) NextToken() (lexer.Token, bool) {
	if pc.pos == len(pc.tokens) {
		return lexer.Token{}, false
	}
	tok := pc.tokens[pc.pos]
	pc.pos++
	pc.lastPos = &tok.Pos
	return tok, true
}

func (pc *ParseContext) PeekToken() (lexer.Token, bool) {
	if pc.pos == len(pc.tokens) {
		return lexer.Token{}, false
	}
	return pc.tokens[pc.pos], true
}

func (pc *ParseContext) LastPos() *lexer.Position {
	return pc.lastPos
}

func (pc *ParseContext) MoveCursor(val int) {
	pc.pos += val
}

func (pc *ParseContext) CursorPos() int {
	return pc.pos
}

func (pc *ParseContext) TokenAt(cursorPos int) lexer.Token {
	return pc.tokens[cursorPos]
}
