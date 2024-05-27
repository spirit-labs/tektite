package parser

import (
	"fmt"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"strconv"
)

type ExprDesc interface {
	ErrorAtPosition(msg string, args ...interface{}) error
}

type BaseExprDesc struct {
	tokenInfo tokenInfo
}

func (b *BaseExprDesc) clearTokenState() {
	b.tokenInfo.token = lexer.Token{}
	b.tokenInfo.input = ""
}

func (b *BaseExprDesc) ErrorAtPosition(msg string, args ...interface{}) error {
	msg = fmt.Sprintf(msg, args...)
	return errors.NewStatementError(MessageWithPosition(msg, b.tokenInfo.token.Pos, b.tokenInfo.input))
}

type tokenInfo struct {
	token lexer.Token
	input string
}

type BinaryOperatorExprDesc struct {
	BaseExprDesc
	Left  ExprDesc
	Right ExprDesc
	Op    string
}

func (f *BinaryOperatorExprDesc) clearTokenState() {
	f.BaseExprDesc.clearTokenState()
	ce, ok := f.Left.(tokenClearable)
	if ok {
		ce.clearTokenState()
	}
	ce, ok = f.Right.(tokenClearable)
	if ok {
		ce.clearTokenState()
	}
}

type UnaryOperatorExprDesc struct {
	BaseExprDesc
	Operand ExprDesc
	Op      string
}

func (u *UnaryOperatorExprDesc) clearTokenState() {
	u.BaseExprDesc.clearTokenState()
	ce, ok := u.Operand.(tokenClearable)
	if ok {
		ce.clearTokenState()
	}
}

type UnaryPostfixOperatorExprDesc struct {
	BaseExprDesc
	Operand ExprDesc
	Op      string
}

func (u *UnaryPostfixOperatorExprDesc) clearTokenState() {
	u.BaseExprDesc.clearTokenState()
	ce, ok := u.Operand.(tokenClearable)
	if ok {
		ce.clearTokenState()
	}
}

type IntegerConstExprDesc struct {
	BaseExprDesc
	Value int
}

type BoolConstExprDesc struct {
	BaseExprDesc
	Value bool
}

type FloatConstExprDesc struct {
	BaseExprDesc
	Value float64
}

type StringConstExprDesc struct {
	BaseExprDesc
	Value string
}

type IdentifierExprDesc struct {
	BaseExprDesc
	IdentifierName string
}

type FunctionExprDesc struct {
	BaseExprDesc
	FunctionName string
	Aggregate    bool
	ArgExprs     []ExprDesc
}

func (f *FunctionExprDesc) clearTokenState() {
	f.BaseExprDesc.clearTokenState()
	for _, expr := range f.ArgExprs {
		ce, ok := expr.(tokenClearable)
		if ok {
			ce.clearTokenState()
		}
	}
}

func (p *Parser) ParseExpressionList(context *ParseContext) ([]ExprDesc, [][]lexer.Token, error) {
	var exprs []ExprDesc
	var allTokens [][]lexer.Token
	for {
		// Extract the tokens that form an expression
		tokens, more, err := ExtractExpressionTokens(context)
		if err != nil {
			return nil, nil, err
		}
		if tokens == nil {
			return nil, nil, nil
		}
		allTokens = append(allTokens, tokens)
		// Shunt them into post-fix
		shunted, err := p.shuntExpression(NewParseContext(p, context.input, tokens))
		if err != nil {
			return nil, nil, err
		}
		expr, _, err := p.parseExpression(shunted, len(shunted)-1, context.input)
		if err != nil {
			return nil, nil, err
		}
		exprs = append(exprs, expr)
		if !more {
			break
		}
	}
	return exprs, allTokens, nil
}

func ExtractExpressionTokens(context *ParseContext) ([]lexer.Token, bool, error) {
	// expressions appear in expression lists in TSL, an expression is a list is terminated by:
	// a ')': e.g. (store table by = 3 + to_int(f2) )
	// a ',': e.g. (project f3, f1 + 10, f7) - the first two expressions are terminated by ','
	// an identifier that's not preceded by an operator: e.g. (aggregate sum(f1), count(to_lower(f2)) by f3) - the second agggregate expression is terminated by 'by'
	parensCount := 0
	var prevToken *lexer.Token
	var tokens []lexer.Token
	more := false
loop:
	for {
		token, ok := context.PeekToken()
		if !ok {
			break
		}
		switch token.Type {
		case LParensTokenType:
			parensCount++
		case RParensTokenType:
			parensCount--
			if parensCount < 0 {
				break loop
			}
		case IdentTokenType:
			if prevToken != nil && prevToken.Type != BinaryOpTokenType && prevToken.Type != LParensTokenType &&
				prevToken.Type != ListSeparatorTokenType && prevToken.Type != UnaryOpTokenType {
				break loop
			}
		case ListSeparatorTokenType:
			if parensCount == 0 {
				more = true
				context.NextToken()
				break loop
			}
		}
		prevToken = &token
		tokens = append(tokens, token)
		context.NextToken()
	}
	return tokens, more, nil
}

func (p *Parser) ParseExpression(context *ParseContext) (ExprDesc, error) {
	tokens, err := p.shuntExpression(context)
	if err != nil {
		return nil, err
	}
	expr, _, err := p.parseExpression(tokens, len(tokens)-1, context.input)
	return expr, err
}

func checkUnaryOperand(prevToken *lexer.Token, token *lexer.Token, input string) error {
	// We detect absence of unary operand and return an error if so. This won't otherwise be detected in expression
	// parsing
	if prevToken != nil && prevToken.Type == UnaryOpTokenType {
		if token == nil || token.Type == RParensTokenType {
			return incompleteExpressionError(*prevToken, input)
		}
	}
	return nil
}

func (p *Parser) checkValidFunction(prevToken *lexer.Token, token *lexer.Token, input string) error {
	if prevToken != nil && prevToken.Type == IdentTokenType && token.Type == LParensTokenType {
		// an identifier followed by an l parens must be a function call. we validate whether it is a valid
		// function. we do the check here before creating expressions, otherwise the identifier will get shunted into
		// a different place, and we'll end up with a hard-to-understand error message. this provides a better error
		// message to the user
		if !p.isFunction(prevToken.Value) {
			msg := fmt.Sprintf("'%s' is not a known function", prevToken.Value)
			return errorAtPosition(msg, prevToken.Pos, input)
		}
	}
	return nil
}

// shuntExpression uses the shunting yard algorithm to convert the expression into post-fix notation - it then
// maps directly to the expression tree structure.
// the original algorithm has been adapted, so it can cope with functions with variable number of arguments
// this is done by including the left and right parentheses in the shunted output tokens. this allows us to
// see where function parameter lists start and end, so we know how many params there are.
func (p *Parser) shuntExpression(context *ParseContext) ([]lexer.Token, error) {
	var output []lexer.Token
	var tokens tokenStack
	var prevToken *lexer.Token
	for {
		tok, ok := context.NextToken()
		if !ok {
			break
		}
		if err := checkUnaryOperand(prevToken, &tok, context.input); err != nil {
			return nil, err
		}
		if err := p.checkValidFunction(prevToken, &tok, context.input); err != nil {
			return nil, err
		}
		switch tok.Type {
		case BinaryOpTokenType, UnaryOpTokenType:
			if (tok.Value == "-" || tok.Value == "+") && (prevToken == nil || prevToken.Type == BinaryOpTokenType ||
				prevToken.Type == UnaryOpTokenType || prevToken.Type == ListSeparatorTokenType) {
				// The lexer lexes all '-' or '+' as binary operators including ones that are meant to represent number constants
				// e.g. "-3" or "4 + -3", "4 - +3", so if we have a "-" or "+" followed by a number we treat it as a number constant not
				// a binary op if it's the first token, or it's preceded by an operator.
				next, ok := context.PeekToken()
				if ok && (next.Type == IntegerTokenType || next.Type == FloatTokenType) {
					context.NextToken()
					if tok.Value == "-" {
						numBytes := make([]byte, len(next.Value)+1)
						numBytes[0] = '-'
						copy(numBytes[1:], next.Value)
						tok := lexer.Token{
							Type:  next.Type,
							Value: common.ByteSliceToStringZeroCopy(numBytes),
							Pos:   tok.Pos,
						}
						output = append(output, tok)
					} else {
						output = append(output, next)
					}
					break
				}
			}

			// while there is an operator with greater precedence at top of stack, pop it and append it to the output
			for {
				top, ok := tokens.peek()
				if !ok || top.Value == "(" {
					break
				}
				if (top.Type == BinaryOpTokenType || top.Type == UnaryOpTokenType) && getPrecedence(top.Value) > getPrecedence(tok.Value) {
					popped, _ := tokens.pop()
					output = append(output, popped)
				} else {
					break
				}
			}
			tokens.push(tok)
		case UnaryPostfixOpTokenType:
			// This needs to go at the bottom of the stack
			tokens.tokens = append([]lexer.Token{tok}, tokens.tokens...)
		case LParensTokenType:
			tokens.push(tok)
			output = append(output, tok)
		case RParensTokenType:
			// pop tokens until we reach matching "("
			for {
				top, ok := tokens.pop()
				if !ok {
					return nil, errorAtPosition("unmatched ')'", tok.Pos, context.input)
				}
				if top.Value == "(" {
					break
				}
				output = append(output, top)
			}
			output = append(output, tok)
			top, ok := tokens.peek()
			if ok {
				if p.isFunction(top.Value) {
					tokens.pop()
					output = append(output, top)
				}
			}
		case IdentTokenType:
			if p.isFunction(tok.Value) {
				tokens.push(tok)
			} else {
				output = append(output, tok)
			}
		case ListSeparatorTokenType:
			// while operator at top of stack is not a left parentheses, pop it to the output queue
			for {
				top, ok := tokens.peek()
				if !ok || top.Value == "(" {
					break
				}
				popped, _ := tokens.pop()
				output = append(output, popped)
			}
		default:
			output = append(output, tok)
		}
		prevToken = &tok
	}
	// We do some validity checking here, before the tokens are shunted
	if err := checkUnaryOperand(prevToken, nil, context.input); err != nil {
		return nil, err
	}
	for {
		popped, ok := tokens.pop()
		if !ok {
			break
		}
		if popped.Type == LParensTokenType {
			return nil, errorAtPosition("unmatched '('", popped.Pos, context.input)
		}
		output = append(output, popped)
	}
	return output, nil
}

func checkPos(pos int, tokens []lexer.Token, input string) error {
	if pos >= 0 {
		return nil
	}
	return incompleteExpressionError(tokens[pos+1], input)
}

// parseExpression takes shunted tokens and creates an expression tree from it. The shunted tokens are in post-fix
// notation and map directly to the expression tree structure.
// it works in reverse as the shunted tokens are in reverse.
func (p *Parser) parseExpression(tokens []lexer.Token, pos int, input string) (ExprDesc, int, error) {
	//log.Debugf("parsing expr from tokens: %v", tokens[:pos])
	// Remove any leading rparens (remember, it's reversed, so rparens come before lparens)
	if err := checkPos(pos, tokens, input); err != nil {
		return nil, 0, err
	}
	leadingParensCount := 0
	for tokens[pos].Type == RParensTokenType {
		pos--
		leadingParensCount++
		if err := checkPos(pos, tokens, input); err != nil {
			return nil, 0, err
		}
	}
	tok := tokens[pos]
	var expr ExprDesc
	switch tok.Type {
	case IdentTokenType:
		if p.isFunction(tok.Value) {
			var err error
			expr, pos, err = p.createFunctionExpression(tokens, pos, input)
			if err != nil {
				return nil, 0, err
			}
		} else {
			ce := &IdentifierExprDesc{IdentifierName: tok.Value}
			ce.tokenInfo.token = tok
			ce.tokenInfo.input = input
			expr = ce
			pos--
		}
	case BinaryOpTokenType:
		var err error
		expr, pos, err = p.createBinaryExpression(tokens, pos, input)
		if err != nil {
			return nil, 0, err
		}
	case UnaryOpTokenType:
		var err error
		expr, pos, err = p.createUnaryExpression(tokens, pos, input)
		if err != nil {
			return nil, 0, err
		}
	case UnaryPostfixOpTokenType:
		var err error
		expr, pos, err = p.createUnaryPostfixExpression(tokens, pos, input)
		if err != nil {
			return nil, 0, err
		}
	case IntegerTokenType:
		i, err := strconv.Atoi(tok.Value)
		if err != nil {
			return nil, 0, err
		}
		ie := &IntegerConstExprDesc{Value: i}
		ie.tokenInfo.token = tok
		ie.tokenInfo.input = input
		expr = ie
		pos--
	case FloatTokenType:
		f, err := strconv.ParseFloat(tok.Value[:len(tok.Value)-1], 64)
		if err != nil {
			return nil, 0, err
		}
		fe := &FloatConstExprDesc{Value: f}
		fe.tokenInfo.token = tok
		fe.tokenInfo.input = input
		expr = fe
		pos--
	case StringLiteralTokenType:
		unquoted, err := strconv.Unquote(tok.Value)
		if err != nil {
			return nil, 0, errorAtPosition("invalid quoted string literal", tok.Pos, input)
		}
		se := &StringConstExprDesc{Value: unquoted}
		se.tokenInfo.token = tok
		se.tokenInfo.input = input
		expr = se
		pos--
	case BoolLiteralTokenType:
		var b bool
		if tok.Value == "true" {
			b = true
		} else if tok.Value == "false" {
			b = false
		} else {
			panic("invalid bool value")
		}
		be := &BoolConstExprDesc{Value: b}
		be.tokenInfo.token = tok
		be.tokenInfo.input = input
		expr = be
		pos--
	case LParensTokenType:
		return nil, 0, incompleteExpressionError(tok, input)
	default:
		panic(fmt.Sprintf("unexpected token: %s", tok.Value))
	}
	// remove any trailing rparens
	for i := 0; i < leadingParensCount; i++ {
		if tokens[pos].Type != LParensTokenType {
			return nil, 0, foundUnexpectedTokenError("(", tokens[pos], input)
		}
		pos--
	}
	if expr == nil {
		panic("no expression created")
	}
	return expr, pos, nil
}

func (p *Parser) isFunction(functionName string) bool {
	_, ok := BuiltinFunctions[functionName]
	if ok {
		return true
	}
	_, ok = AggregateFunctions[functionName]
	if ok {
		return true
	}
	if p.externalFunctionChecker != nil {
		return p.externalFunctionChecker.FunctionExists(functionName)
	}
	return false
}

func (p *Parser) createFunctionExpression(tokens []lexer.Token, pos int, input string) (ExprDesc, int, error) {
	tok := tokens[pos]
	aggregate := false
	funcName := tok.Value
	_, isNonAggFunction := BuiltinFunctions[funcName]
	if !isNonAggFunction && p.externalFunctionChecker != nil {
		isNonAggFunction = p.externalFunctionChecker.FunctionExists(funcName)
	}
	if !isNonAggFunction {
		_, ok := AggregateFunctions[funcName]
		if !ok {
			msg := fmt.Sprintf("unknown function '%s'", funcName)
			return nil, 0, errorAtPosition(msg, tok.Pos, input)
		}
		aggregate = true
	}
	pos--
	if err := checkPos(pos, tokens, input); err != nil {
		return nil, 0, err
	}
	tok = tokens[pos]
	if tok.Type != RParensTokenType {
		return nil, 0, errorAtPosition("missing parentheses", tokens[pos+1].Pos, input)
	}
	pos--
	var argExprs []ExprDesc
	for {
		if tokens[pos].Type == LParensTokenType {
			// end of parameters
			pos--
			break
		}
		var err error
		var expr ExprDesc
		// recursively call parseExpression to parse the function argument
		expr, pos, err = p.parseExpression(tokens, pos, input)
		if err != nil {
			return nil, 0, err
		}
		argExprs = append(argExprs, expr)
	}
	// reverse them
	re := len(argExprs) - 1
	for i, j := 0, re; i < j; i, j = i+1, j-1 {
		argExprs[i], argExprs[j] = argExprs[j], argExprs[i]
	}
	fe := &FunctionExprDesc{FunctionName: funcName}
	fe.tokenInfo.token = tok
	fe.tokenInfo.input = input
	fe.ArgExprs = argExprs
	fe.Aggregate = aggregate
	return fe, pos, nil
}

func incompleteExpressionError(tok lexer.Token, input string) error {
	return errorAtPosition("incomplete expression", tok.Pos, input)
}

func (p *Parser) createBinaryExpression(tokens []lexer.Token, pos int, input string) (ExprDesc, int, error) {
	tok := tokens[pos]
	be := &BinaryOperatorExprDesc{Op: tok.Value}
	be.tokenInfo.token = tok
	be.tokenInfo.input = input
	// invoke recursively to get right and left expressions
	right, pos, err := p.parseExpression(tokens, pos-1, input)
	if err != nil {
		return nil, 0, err
	}
	if right == nil {
		panic("right nil")
	}
	left, pos, err := p.parseExpression(tokens, pos, input)
	if err != nil {
		return nil, 0, err
	}
	if left == nil {
		panic("left nil")
	}
	be.Left = left
	be.Right = right
	return be, pos, nil
}

func (p *Parser) createUnaryExpression(tokens []lexer.Token, pos int, input string) (ExprDesc, int, error) {
	tok := tokens[pos]
	ue := &UnaryOperatorExprDesc{Op: tok.Value}
	ue.tokenInfo.token = tok
	ue.tokenInfo.input = input
	// invoke recursively to get operand
	operand, pos, err := p.parseExpression(tokens, pos-1, input)
	if err != nil {
		return nil, 0, err
	}
	ue.Operand = operand
	return ue, pos, nil
}

func (p *Parser) createUnaryPostfixExpression(tokens []lexer.Token, pos int, input string) (ExprDesc, int, error) {
	tok := tokens[pos]
	ue := &UnaryPostfixOperatorExprDesc{Op: tok.Value}
	ue.tokenInfo.token = tok
	ue.tokenInfo.input = input
	operand, pos, err := p.parseExpression(tokens, pos-1, input)
	if err != nil {
		return nil, 0, err
	}
	if operand == nil {
		panic("left nil")
	}
	ue.Operand = operand
	return ue, pos, nil
}

type tokenStack struct {
	tokens []lexer.Token
}

func (t *tokenStack) push(token lexer.Token) {
	t.tokens = append(t.tokens, token)
}

func (t *tokenStack) pop() (lexer.Token, bool) {
	if len(t.tokens) == 0 {
		return lexer.Token{}, false
	}
	last := len(t.tokens) - 1
	r := t.tokens[last]
	t.tokens = t.tokens[:last]
	return r, true
}

func (t *tokenStack) peek() (lexer.Token, bool) {
	if len(t.tokens) == 0 {
		return lexer.Token{}, false
	}
	return t.tokens[len(t.tokens)-1], true
}

func getPrecedence(op string) int {
	prec, ok := Operators[op]
	if !ok {
		panic("unknown operator")
	}
	return prec
}

func ExtractAlias(exprDesc ExprDesc) (bool, ExprDesc, string, ExprDesc) {
	binary, isBinary := exprDesc.(*BinaryOperatorExprDesc)
	if !isBinary || binary.Op != "as" {
		return true, exprDesc, "", nil
	}
	identExpr, isConst := binary.Right.(*IdentifierExprDesc)
	if !isConst {
		return false, nil, "", nil
	}
	return true, binary.Left, identExpr.IdentifierName, identExpr
}

func ExtractAscDesc(exprDesc ExprDesc) (ExprDesc, string) {
	op, unaryPostfix := exprDesc.(*UnaryPostfixOperatorExprDesc)
	if !unaryPostfix {
		return exprDesc, ""
	}
	return op.Operand, op.Op
}

/*

Adapted from golang Language Specification:

=========
We follow the precedence and associativity rules as defined in the golang language specification (copied here):

Unary operators have the highest precedence. [snip...]>

There are five precedence levels for binary operators. Multiplication operators bind strongest, followed by addition
operators, comparison operators, && (logical AND), and finally || (logical OR):

Precedence    Operator
    5             *  /  %
    4             +  -
    3             ==  !=  <  <=  >  >=
    2             &&
    1             ||

Binary operators of the same precedence associate from left to right. For instance, x / y * z is the same as (x / y) * z.
==========

*/

var Operators = map[string]int{
	"asc":        8,
	"ascending":  8,
	"desc":       8,
	"descending": 8,
	"!":          7,
	"/":          5,
	"%":          5,
	"*":          5,
	"+":          4,
	"-":          4,
	"==":         3,
	"!=":         3,
	"<=":         3,
	">=":         3,
	"<":          3,
	">":          3,
	"&&":         2,
	"||":         1,
	"as":         0,
}

var AggregateFunctions = map[string]struct{}{
	"count": {},
	"sum":   {},
	"min":   {},
	"max":   {},
	"avg":   {},
}

var BuiltinFunctions = map[string]struct{}{

	"if":          {},
	"is_null":     {},
	"is_not_null": {},
	"in":          {},
	"case":        {},

	"decimal_shift": {},

	"len":         {},
	"concat":      {},
	"starts_with": {},
	"ends_with":   {},
	"matches":     {},
	"trim":        {},
	"ltrim":       {},
	"rtrim":       {},
	"to_lower":    {},
	"to_upper":    {},
	"sub_str":     {},
	"replace":     {},
	"sprintf":     {},

	"to_int":       {},
	"to_float":     {},
	"to_string":    {},
	"to_decimal":   {},
	"to_bytes":     {},
	"to_timestamp": {},

	"format_date": {},
	"parse_date":  {},
	"year":        {},
	"month":       {},
	"day":         {},
	"hour":        {},
	"minute":      {},
	"second":      {},
	"millis":      {},
	"now":         {},

	"json_int":     {},
	"json_float":   {},
	"json_bool":    {},
	"json_string":  {},
	"json_raw":     {},
	"json_is_null": {},
	"json_type":    {},

	"kafka_build_headers": {},
	"kafka_header":        {},

	"bytes_slice": {},
	"uint64_be":   {},
	"uint64_le":   {},

	"abs": {},
}
