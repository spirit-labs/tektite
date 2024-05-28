package parser

import (
	"fmt"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/types"
	"strconv"
	"strings"
	"time"
)

type BaseDesc struct {
	tokenInfo tokensInfo
	super     parseable
}

type tokensInfo struct {
	tokens []lexer.Token
	input  string
}

type parseable interface {
	parse(context *ParseContext) error
}

type tokenClearable interface {
	clearTokenState()
}

func (b *BaseDesc) clearTokenState() {
	b.tokenInfo.tokens = nil
	b.tokenInfo.input = ""
	b.super = nil
}

func (b *BaseDesc) Parse(context *ParseContext) error {
	tokStart := context.pos
	if err := b.super.parse(context); err != nil {
		return err
	}
	// capture the tokens and input
	b.tokenInfo.tokens = context.tokens[tokStart:context.pos]
	b.tokenInfo.input = context.input
	return nil
}

func (b *BaseDesc) ErrorMsgAtToken(msg string, tokenVal string) string {
	tok := &b.tokenInfo.tokens[0] // default to first token
	if tokenVal != "" {
		for _, token := range b.tokenInfo.tokens {
			if token.Value == tokenVal {
				tok = &token
				break
			}
		}
	}
	return MessageWithPosition(msg, tok.Pos, b.tokenInfo.input)
}

func (b *BaseDesc) ErrorMsgAtPosition(msg string, position lexer.Position) string {
	return MessageWithPosition(msg, position, b.tokenInfo.input)
}

func NewTSLDesc() *TSLDesc {
	super := &TSLDesc{}
	super.BaseDesc.super = super
	return super
}

type TSLDesc struct {
	BaseDesc
	CreateStream *CreateStreamDesc
	DeleteStream *DeleteStreamDesc
	PrepareQuery *PrepareQueryDesc
	ListStreams  *ListStreamsDesc
	ShowStream   *ShowStreamDesc
}

func (t *TSLDesc) parse(context *ParseContext) error {
	token, ok := context.PeekToken()
	if !ok {
		return errors.NewParseError("statement is empty")
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	switch token.Value {
	case "delete":
		deleteStream := NewDeleteStreamDesc()
		if err := deleteStream.Parse(context); err != nil {
			return err
		}
		t.DeleteStream = deleteStream
	case "prepare":
		prepareQuery := NewPrepareQueryDesc()
		if err := prepareQuery.Parse(context); err != nil {
			return err
		}
		t.PrepareQuery = prepareQuery
	case "list":
		listStreams := NewListStreamsDesc()
		if err := listStreams.Parse(context); err != nil {
			return err
		}
		t.ListStreams = listStreams
	case "show":
		showStream := NewShowStreamDesc()
		if err := showStream.Parse(context); err != nil {
			return err
		}
		t.ShowStream = showStream
	default:
		createStreamDesc := NewCreateStreamDesc()
		if err := createStreamDesc.Parse(context); err != nil {
			return err
		}
		t.CreateStream = createStreamDesc
	}
	return nil
}

func (t *TSLDesc) clearTokenState() {
	t.BaseDesc.clearTokenState()
	if t.PrepareQuery != nil {
		t.PrepareQuery.clearTokenState()
	}
	if t.DeleteStream != nil {
		t.DeleteStream.clearTokenState()
	}
	if t.CreateStream != nil {
		t.CreateStream.clearTokenState()
	}
	if t.ListStreams != nil {
		t.ListStreams.clearTokenState()
	}
	if t.ShowStream != nil {
		t.ShowStream.clearTokenState()
	}
}

func NewCreateStreamDesc() *CreateStreamDesc {
	super := &CreateStreamDesc{}
	super.BaseDesc.super = super
	return super
}

type CreateStreamDesc struct {
	BaseDesc
	StreamName    string
	OperatorDescs []Parseable
	TestSource    bool
	TestSink      bool
}

func (cs *CreateStreamDesc) parse(context *ParseContext) error {
	token, err := context.expectToken()
	if err != nil {
		return err
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	cs.StreamName = token.Value
	if _, err := context.expectToken(":="); err != nil {
		return err
	}
	for {
		if err := cs.parseOperatorDesc(context); err != nil {
			return err
		}
		if !context.HasNext() {
			break
		}
		if _, err := context.expectToken("->"); err != nil {
			return err
		}
	}
	return nil
}

func (cs *CreateStreamDesc) parseOperatorDesc(context *ParseContext) error {
	token, ok := context.PeekToken()
	if !ok {
		return endOfInputError()
	}
	if token.Value != "(" {
		// Continuation
		continuation := NewContinuationDesc()
		if err := continuation.Parse(context); err != nil {
			return err
		}
		cs.OperatorDescs = append(cs.OperatorDescs, continuation)
		return nil
	}
	if _, err := context.expectToken("("); err != nil {
		return err
	}
	token, ok = context.NextToken()
	if !ok {
		return endOfInputError()
	}
	var err error
	var operatorDesc Parseable
	switch token.Value {
	// note we rewind tokens before calling Parse on the operator descs as the operators need to capture the operator
	// name so error messages make sense
	case "bridge":
		token, err = context.expectToken("from", "to")
		if err != nil {
			return err
		}
		if token.Value == "from" {
			operatorDesc = NewBridgeFromDesc()
		} else if token.Value == "to" {
			operatorDesc = NewBridgeToDesc()
		} else {
			panic(fmt.Sprintf("invalid bridge type %s", token.Value))
		}
		context.MoveCursor(-2)
	case "filter":
		operatorDesc = NewFilterDesc()
		context.MoveCursor(-1)
	case "store":
		token, err := context.expectToken("stream", "table")
		if err != nil {
			return err
		}
		if token.Value == "stream" {
			operatorDesc = NewStoreStreamDesc()
		} else if token.Value == "table" {
			operatorDesc = NewStoreTableDesc()
		} else {
			panic(fmt.Sprintf("invalid store type %s", token.Value))
		}
		context.MoveCursor(-2)
	case "project":
		operatorDesc = NewProjectDesc()
		context.MoveCursor(-1)
	case "partition":
		operatorDesc = NewPartitionDesc()
		context.MoveCursor(-1)
	case "aggregate":
		operatorDesc = NewAggregateDesc()
		context.MoveCursor(-1)
	case "join":
		operatorDesc = NewJoinDesc()
		context.MoveCursor(-1)
	case "kafka":
		token, err := context.expectToken("in", "out")
		if err != nil {
			return err
		}
		if token.Value == "in" {
			operatorDesc = NewKafkaInDesc()
		} else if token.Value == "out" {
			operatorDesc = NewKafkaOutDesc()
		} else {
			panic(fmt.Sprintf("invalid kafka operator type %s", token.Value))
		}
		context.MoveCursor(-2)
	case "topic":
		operatorDesc = NewTopicDesc()
		context.MoveCursor(-1)
	case "union":
		operatorDesc = NewUnionDesc()
		context.MoveCursor(-1)
	case "backfill":
		operatorDesc = NewBackfillDesc()
		context.MoveCursor(-1)
	default:
		expected := expectedStr("aggregate", "backfill", "bridge", "filter", "join", "kafka", "partition",
			"producer", "project", "store", "topic", "union")
		return errorAtPosition(fmt.Sprintf("expected %s", expected), token.Pos, context.input)
	}
	if err := operatorDesc.Parse(context); err != nil {
		return err
	}
	cs.OperatorDescs = append(cs.OperatorDescs, operatorDesc)
	return nil
}

func (cs *CreateStreamDesc) clearTokenState() {
	cs.BaseDesc.clearTokenState()
	for _, expr := range cs.OperatorDescs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func NewQueryDesc() *QueryDesc {
	super := &QueryDesc{}
	super.BaseDesc.super = super
	return super
}

type QueryDesc struct {
	BaseDesc
	OperatorDescs []Parseable
}

func (q *QueryDesc) parse(context *ParseContext) error {
	for {
		if err := q.parseOperatorDesc(context); err != nil {
			return err
		}
		if !context.HasNext() {
			break
		}
		if _, err := context.expectToken("->"); err != nil {
			return err
		}
	}
	return nil
}

func (q *QueryDesc) parseOperatorDesc(context *ParseContext) error {
	if _, err := context.expectToken("("); err != nil {
		return err
	}
	token, err := context.expectToken("get", "scan", "project", "filter", "sort")
	if err != nil {
		return err
	}
	var operatorDesc Parseable
	// note we rewind tokens before calling Parse on the operator descs as the operators need to capture the operator
	// name so error messages make sense
	switch token.Value {
	case "get":
		operatorDesc = NewGetDesc()
		context.MoveCursor(-1)
	case "scan":
		operatorDesc = NewScanDesc()
		context.MoveCursor(-1)
	case "project":
		operatorDesc = NewProjectDesc()
		context.MoveCursor(-1)
	case "filter":
		operatorDesc = NewFilterDesc()
		context.MoveCursor(-1)
	case "sort":
		operatorDesc = NewSortDesc()
		context.MoveCursor(-1)
	default:
		panic("unexpected operator desc")
	}
	if err := operatorDesc.Parse(context); err != nil {
		return err
	}
	q.OperatorDescs = append(q.OperatorDescs, operatorDesc)
	return nil
}

func (q *QueryDesc) clearTokenState() {
	q.BaseDesc.clearTokenState()
	for _, expr := range q.OperatorDescs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func NewDeleteStreamDesc() *DeleteStreamDesc {
	super := &DeleteStreamDesc{}
	super.BaseDesc.super = super
	return super
}

type DeleteStreamDesc struct {
	BaseDesc
	StreamName string
}

func (d *DeleteStreamDesc) parse(context *ParseContext) error {
	_, err := context.expectToken("delete")
	if err != nil {
		return err
	}
	_, err = context.expectToken("(")
	if err != nil {
		return err
	}
	token, err := context.expectToken()
	if err != nil {
		return err
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	d.StreamName = token.Value
	_, err = context.expectToken(")")
	return err
}

func NewPrepareQueryDesc() *PrepareQueryDesc {
	super := &PrepareQueryDesc{}
	super.BaseDesc.super = super
	super.Query = NewQueryDesc()
	return super
}

type PrepareQueryDesc struct {
	BaseDesc
	QueryName string
	Query     *QueryDesc
	Params    []PreparedStatementParam
}

func (p *PrepareQueryDesc) parse(context *ParseContext) error {
	_, err := context.expectToken("prepare")
	if err != nil {
		return err
	}
	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	p.QueryName = token.Value
	_, err = context.expectToken(":=")
	if err != nil {
		return err
	}
	start := context.CursorPos()
	if err := p.Query.Parse(context); err != nil {
		return err
	}

	// Now we scan back through the tokens to capture any prepared statement parameters
	var params []PreparedStatementParam
	curr := context.CursorPos()
	for i := start; i < curr; i++ {
		tok := context.TokenAt(i)
		if tok.Type == IdentTokenType {
			if tok.Value[0] == '$' {
				parts := strings.Split(tok.Value, ":")
				ok := false
				if len(parts) == 2 {
					ct, err := types.StringToColumnType(parts[1])
					if err == nil {
						params = append(params, PreparedStatementParam{
							ParamName: tok.Value,
							ParamType: ct,
						})
						ok = true
					}
				}
				if !ok {
					return errorAtPosition("invalid prepared statement parameter. must be of form '$name:type' where type is one of int, float, bool, decimal(p, s), string, bytes, timestamp",
						tok.Pos, context.input)
				}
			}
		}
	}
	p.Params = params
	return nil
}

type PreparedStatementParam struct {
	ParamName string
	ParamType types.ColumnType
}

func (p *PrepareQueryDesc) clearTokenState() {
	p.BaseDesc.clearTokenState()
	p.Query.clearTokenState()
}

func NewListStreamsDesc() *ListStreamsDesc {
	super := &ListStreamsDesc{}
	super.BaseDesc.super = super
	return super
}

type ListStreamsDesc struct {
	BaseDesc
	RegEx string
}

func (p *ListStreamsDesc) parse(context *ParseContext) error {
	context.NextToken()
	if _, err := context.expectToken("("); err != nil {
		return err
	}
	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Type == StringLiteralTokenType {
		p.RegEx = token.Value
		token, ok = context.NextToken()
		if !ok {
			return endOfInputError()
		}
	}
	if token.Type != RParensTokenType {
		return foundUnexpectedTokenError("right parenthesis", token, context.input)
	}
	return nil
}

func (p *ListStreamsDesc) clearTokenState() {
	p.BaseDesc.clearTokenState()
}

func NewShowStreamDesc() *ShowStreamDesc {
	super := &ShowStreamDesc{}
	super.BaseDesc.super = super
	return super
}

type ShowStreamDesc struct {
	BaseDesc
	StreamName string
}

func (p *ShowStreamDesc) parse(context *ParseContext) error {
	context.NextToken()
	if _, err := context.expectToken("("); err != nil {
		return err
	}
	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	p.StreamName = token.Value
	if _, err := context.expectToken(")"); err != nil {
		return err
	}
	return nil
}

func (p *ShowStreamDesc) clearTokenState() {
	p.BaseDesc.clearTokenState()
}

func NewContinuationDesc() *ContinuationDesc {
	super := &ContinuationDesc{}
	super.BaseDesc.super = super
	return super
}

type ContinuationDesc struct {
	BaseDesc
	ParentStreamName string
}

func (c *ContinuationDesc) parse(context *ParseContext) error {
	token, ok := context.NextToken()
	if !ok {
		// can't happen as we already checked in caller
		panic("no more tokens")
	}
	c.ParentStreamName = token.Value
	return nil
}

func NewBridgeFromDesc() *BridgeFromDesc {
	super := &BridgeFromDesc{}
	super.BaseDesc.super = super
	return super
}

type BridgeFromDesc struct {
	BaseDesc
	TopicName            string
	Partitions           int
	PollTimeout          *time.Duration
	MaxPollMessages      *int
	WatermarkType        *string
	WatermarkLateness    *time.Duration
	WatermarkIdleTimeout *time.Duration
	Props                map[string]string
}

func (b *BridgeFromDesc) parse(context *ParseContext) error {
	context.MoveCursor(2)

	// topic name is mandatory
	topicName, err := parseTopicName(context)
	if err != nil {
		return err
	}
	b.TopicName = topicName

	// partitions is mandatory
	partitions, err := parsePartitions(context)
	if err != nil {
		return err
	}
	b.Partitions = partitions

	for {
		token, ok := context.NextToken()
		if !ok {
			break
		}
		if token.Value == ")" {
			// End of operator definition
			return nil
		}
		// Must be optional arg
		if token.Type != IdentTokenType {
			return foundUnexpectedTokenError("identifier or closing ')'", token, context.input)
		}
		switch token.Value {
		case "poll_timeout":
			if b.PollTimeout != nil {
				return duplicateArgumentError(token, context)
			}
			pollTimeout, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.PollTimeout = &pollTimeout
		case "max_poll_messages":
			if b.MaxPollMessages != nil {
				return duplicateArgumentError(token, context)
			}
			tok, err := parseNamedArgValue(IntegerTokenType, "number", context)
			if err != nil {
				return err
			}
			maxPollMessages, err := strconv.Atoi(tok.Value)
			if err != nil {
				return err
			}
			b.MaxPollMessages = &maxPollMessages
		case "watermark_type":
			if b.WatermarkType != nil {
				return duplicateArgumentError(token, context)
			}
			wmType, err := parseWatermarkType(context)
			if err != nil {
				return err
			}
			b.WatermarkType = &wmType
		case "watermark_lateness":
			if b.WatermarkLateness != nil {
				return duplicateArgumentError(token, context)
			}
			wmLateness, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.WatermarkLateness = &wmLateness
		case "watermark_idle_timeout":
			if b.WatermarkIdleTimeout != nil {
				return duplicateArgumentError(token, context)
			}
			wmIdleTimeout, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.WatermarkIdleTimeout = &wmIdleTimeout
		case "props":
			if b.Props != nil {
				return duplicateArgumentError(token, context)
			}
			props, err := parseProps(context)
			if err != nil {
				return err
			}
			b.Props = props
		default:
			if token.Value == "partitions" {
				return duplicateArgumentError(token, context)
			}
			return unknownArgumentError(token, context)
		}
	}
	return nil
}

func NewBridgeToDesc() *BridgeToDesc {
	super := &BridgeToDesc{}
	super.BaseDesc.super = super
	return super
}

type BridgeToDesc struct {
	BaseDesc
	TopicName         string
	Retention         *time.Duration
	InitialRetryDelay *time.Duration
	MaxRetryDelay     *time.Duration
	ConnectTimeout    *time.Duration
	SendTimeout       *time.Duration
	Props             map[string]string
}

func (b *BridgeToDesc) parse(context *ParseContext) error {
	context.MoveCursor(2)

	// topic name is mandatory
	topicName, err := parseTopicName(context)
	if err != nil {
		return err
	}
	b.TopicName = topicName

	for {
		token, ok := context.NextToken()
		if !ok {
			break
		}
		if token.Value == ")" {
			// End of operator definition
			return nil
		}
		// Must be optional arg
		if token.Type != IdentTokenType {
			return foundUnexpectedTokenError("identifier", token, context.input)
		}
		switch token.Value {
		case "props":
			if b.Props != nil {
				return duplicateArgumentError(token, context)
			}
			props, err := parseProps(context)
			if err != nil {
				return err
			}
			b.Props = props
		case "retention":
			if b.Retention != nil {
				return duplicateArgumentError(token, context)
			}
			ret, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.Retention = &ret
		case "initial_retry_delay":
			if b.InitialRetryDelay != nil {
				return duplicateArgumentError(token, context)
			}
			ret, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.InitialRetryDelay = &ret
		case "max_retry_delay":
			if b.MaxRetryDelay != nil {
				return duplicateArgumentError(token, context)
			}
			ret, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.MaxRetryDelay = &ret
		case "connect_timeout":
			if b.ConnectTimeout != nil {
				return duplicateArgumentError(token, context)
			}
			ret, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.ConnectTimeout = &ret
		case "send_timeout":
			if b.SendTimeout != nil {
				return duplicateArgumentError(token, context)
			}
			ret, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			b.SendTimeout = &ret
		}
	}
	return nil
}

func NewFilterDesc() *FilterDesc {
	super := &FilterDesc{}
	super.BaseDesc.super = super
	return super
}

type FilterDesc struct {
	BaseDesc
	Expr ExprDesc
}

func (f *FilterDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	_, err := context.expectToken("by")
	if err != nil {
		return err
	}
	_, exprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(exprs) != 1 {
		nextToken, ok := context.NextToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition(`a single filter expression must be specified`, nextToken.Pos, context.input)
	}
	f.Expr = exprs[0]
	if _, err := context.expectToken(")"); err != nil {
		return err
	}
	return nil
}

func (f *FilterDesc) clearTokenState() {
	f.BaseDesc.clearTokenState()
	clearable, ok := f.Expr.(tokenClearable)
	if ok {
		clearable.clearTokenState()
	}
}

func NewStoreStreamDesc() *StoreStreamDesc {
	super := &StoreStreamDesc{}
	super.BaseDesc.super = super
	return super
}

type StoreStreamDesc struct {
	BaseDesc
	Retention *time.Duration
}

func (s *StoreStreamDesc) parse(context *ParseContext) error {
	context.MoveCursor(2)
	retention, err := parseOptionalRetention(context)
	if err != nil {
		return err
	}
	s.Retention = retention
	return nil
}

func NewStoreTableDesc() *StoreTableDesc {
	super := &StoreTableDesc{}
	super.BaseDesc.super = super
	return super
}

type StoreTableDesc struct {
	BaseDesc
	KeyCols   []string
	Retention *time.Duration
}

func (s *StoreTableDesc) parse(context *ParseContext) error {
	context.MoveCursor(2)
	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Value == ")" {
		// No key
		return nil
	}
	if token.Value != "by" {
		return foundUnexpectedTokenError("'by'", token, context.input)
	}
	cols, _, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		nextToken, ok := context.NextToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition(`no key columns specified`, nextToken.Pos, context.input)
	}
	s.KeyCols = cols
	token, ok = context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Value == ")" {
		return nil
	}
	if token.Value != "retention" {
		return foundUnexpectedTokenError("retention", token, context.input)
	}
	retention, err := parseDurationArg(context)
	if err != nil {
		return err
	}
	s.Retention = &retention
	if _, err := context.expectToken(")"); err != nil {
		return err
	}
	return nil
}

func NewProjectDesc() *ProjectDesc {
	super := &ProjectDesc{}
	super.BaseDesc.super = super
	return super
}

type ProjectDesc struct {
	BaseDesc
	Expressions []ExprDesc
}

func (p *ProjectDesc) clearTokenState() {
	p.BaseDesc.clearTokenState()
	for _, expr := range p.Expressions {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func (p *ProjectDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	_, exprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	_, err = context.expectToken(")")
	if err != nil {
		return err
	}
	if len(exprs) == 0 {
		return emptyKeyExpressionsError(*context.LastPos(), context)
	}
	p.Expressions = exprs
	return nil
}

func NewPartitionDesc() *PartitionDesc {
	super := &PartitionDesc{}
	super.BaseDesc.super = super
	return super
}

type PartitionDesc struct {
	BaseDesc
	KeyExprs   []string
	Partitions int
	Mapping    string
}

func (p *PartitionDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	if _, err := context.expectToken("by"); err != nil {
		return err
	}
	keyExprs, _, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(keyExprs) == 0 {
		tok, ok := context.PeekToken()
		if !ok {
			return endOfInputError()
		}
		return emptyKeyExpressionsError(tok.Pos, context)
	}
	p.KeyExprs = keyExprs

	partitions, err := parsePartitions(context)
	if err != nil {
		return err
	}
	p.Partitions = partitions

	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Value == ")" {
		// End of operator definition
		return nil
	}
	if token.Value != "mapping" {
		return foundUnexpectedTokenError("mapping", token, context.input)
	}
	tok, err := parseNamedArgValue(StringLiteralTokenType, "string literal", context)
	if err != nil {
		return err
	}
	if _, err := context.expectToken(")"); err != nil {
		return err
	}
	p.Mapping = tok.Value
	return nil
}

func NewAggregateDesc() *AggregateDesc {
	super := &AggregateDesc{}
	super.BaseDesc.super = super
	return super
}

type AggregateDesc struct {
	BaseDesc
	AggregateExprs       []ExprDesc
	KeyExprs             []ExprDesc
	AggregateExprStrings []string
	KeyExprsStrings      []string
	Size                 *time.Duration
	Hop                  *time.Duration
	Lateness             *time.Duration
	Store                *bool
	IncludeWindowCols    *bool
	Retention            *time.Duration
}

func (a *AggregateDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	aggExprStrings, aggExprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(aggExprs) == 0 {
		nextToken, ok := context.NextToken()
		if !ok {
			return endOfInputError()
		}
		return emptyKeyExpressionsError(nextToken.Pos, context)
	}
	a.AggregateExprs = aggExprs
	a.AggregateExprStrings = aggExprStrings

	//token, ok := context.NextToken()
	//if !ok {
	//	return endOfInputError()
	//}
	//if token.Type == RParensTokenType {
	//	// end of operator definition
	//	return nil
	//}

	//if token.Value != "by" {
	//	return foundUnexpectedTokenError("by", token, context.input)
	//}

	//keyExprStrings, keyExprs, err := parseExpressions(context)
	//if err != nil {
	//	return err
	//}
	//a.KeyExprs = keyExprs
	//a.KeyExprsStrings = keyExprStrings

	//nextToken, ok := context.PeekToken()
	//if !ok {
	//	return endOfInputError()
	//}
	//if nextToken.Type == RParensTokenType {
	//	context.NextToken()
	//	// end of operator definition
	//	return nil
	//}

	for {
		token, ok := context.NextToken()
		if !ok {
			break
		}
		if token.Value == ")" {
			// End of operator definition
			return nil
		}
		// Must be optional arg
		if token.Type != IdentTokenType {
			return foundUnexpectedTokenError("identifier", token, context.input)
		}

		switch token.Value {
		case "by":
			keyExprStrings, keyExprs, err := parseExpressions(context)
			if err != nil {
				return err
			}
			a.KeyExprs = keyExprs
			a.KeyExprsStrings = keyExprStrings
		case "size":
			if a.Size != nil {
				return duplicateArgumentError(token, context)
			}
			size, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			a.Size = &size
		case "hop":
			if a.Hop != nil {
				return duplicateArgumentError(token, context)
			}
			hop, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			a.Hop = &hop
		case "lateness":
			if a.Lateness != nil {
				return duplicateArgumentError(token, context)
			}
			lateness, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			a.Lateness = &lateness
		case "store":
			if a.Store != nil {
				return duplicateArgumentError(token, context)
			}
			store, err := parseBool(context)
			if err != nil {
				return err
			}
			a.Store = &store
		case "window_cols":
			if a.IncludeWindowCols != nil {
				return duplicateArgumentError(token, context)
			}
			includeWindowCols, err := parseBool(context)
			if err != nil {
				return err
			}
			a.IncludeWindowCols = &includeWindowCols
		case "retention":
			if a.Retention != nil {
				return duplicateArgumentError(token, context)
			}
			ret, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			a.Retention = &ret
		}
	}
	return nil
}

func (a *AggregateDesc) clearTokenState() {
	a.BaseDesc.clearTokenState()
	for _, expr := range a.AggregateExprs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
	for _, expr := range a.KeyExprs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func NewJoinDesc() *JoinDesc {
	super := &JoinDesc{}
	super.BaseDesc.super = super
	return super
}

type JoinDesc struct {
	BaseDesc
	LeftIsTable      bool
	RightIsTable     bool
	LeftStream       string
	RightStream      string
	LeftStreamToken  lexer.Token
	RightStreamToken lexer.Token
	JoinElements     []JoinElement
	Within           *time.Duration
	Retention        *time.Duration
}

type JoinElement struct {
	LeftToken     lexer.Token
	RightToken    lexer.Token
	JoinTypeToken lexer.Token
	LeftCol       string
	JoinType      string
	RightCol      string
}

func (j *JoinDesc) clearTokenState() {
	j.BaseDesc.clearTokenState()
	for i := 0; i < len(j.JoinElements); i++ {
		e := &j.JoinElements[i]
		e.JoinTypeToken = lexer.Token{}
		e.LeftToken = lexer.Token{}
		e.RightToken = lexer.Token{}
	}
	j.LeftStreamToken = lexer.Token{}
	j.RightStreamToken = lexer.Token{}
}

func (j *JoinDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	leftIsTable, leftStreamToken, err := parseJoinInput(context)
	if err != nil {
		return err
	}
	j.LeftIsTable = leftIsTable
	j.LeftStream = leftStreamToken.Value
	j.LeftStreamToken = leftStreamToken

	if _, err := context.expectToken("with"); err != nil {
		return err
	}

	rightIsTable, rightStreamToken, err := parseJoinInput(context)
	if err != nil {
		return err
	}
	j.RightIsTable = rightIsTable
	j.RightStream = rightStreamToken.Value
	j.RightStreamToken = rightStreamToken

	if _, err := context.expectToken("by"); err != nil {
		return err
	}
	joinElements, token, err := parseJoinElements(context)
	if err != nil {
		return err
	}
	if len(joinElements) == 0 {
		return errorAtPosition(`there must be at least one join column expression`, token.Pos, context.input)
	}
	j.JoinElements = joinElements

	if token.Value == ")" {
		// end of join definition
		return nil
	}

	if token.Value == "within" {
		within, err := parseDurationArg(context)
		if err != nil {
			return err
		}
		j.Within = &within
		var ok bool
		token, ok = context.NextToken()
		if !ok {
			return endOfInputError()
		}
	}

	if token.Value == ")" {
		// end of join definition
		return nil
	}

	if token.Value == "retention" {
		retention, err := parseDurationArg(context)
		if err != nil {
			return err
		}
		j.Retention = &retention
	}

	if _, err := context.expectToken(")"); err != nil {
		return err
	}

	return nil
}

func parseJoinInput(context *ParseContext) (bool, lexer.Token, error) {
	token, ok := context.NextToken()
	isTable := false
	if !ok || token.Value == ")" {
		return false, lexer.Token{}, endOfInputError()
	}
	if token.Value == "table" {
		isTable = true
		token, ok = context.NextToken()
		if !ok {
			return false, lexer.Token{}, endOfInputError()
		}
	}
	if token.Type != IdentTokenType {
		return false, lexer.Token{}, foundUnexpectedTokenError("identifier", token, context.input)
	}
	return isTable, token, nil
}

func parseJoinElements(context *ParseContext) ([]JoinElement, lexer.Token, error) {
	var joinElements []JoinElement
	first := true
	for {
		token, ok := context.NextToken()
		if !ok {
			return nil, lexer.Token{}, endOfInputError()
		}
		if token.Value == ")" || token.Value == "within" || token.Value == "retention" {
			// End of join elements definition
			return joinElements, token, nil
		}
		if !first {
			if token.Value != "," {
				return nil, lexer.Token{}, foundUnexpectedTokenError("','", token, context.input)
			}
			token, ok = context.NextToken()
			if !ok {
				return nil, lexer.Token{}, endOfInputError()
			}
		}
		first = false
		if token.Type != IdentTokenType {
			return nil, lexer.Token{}, foundUnexpectedTokenError("identifier", token, context.input)
		}
		leftCol := token.Value
		leftToken := token
		token, ok = context.NextToken()
		if !ok {
			return nil, lexer.Token{}, endOfInputError()
		}
		if token.Value != "=" && token.Value != "*=" && token.Value != "=*" {
			return nil, lexer.Token{}, foundUnexpectedTokenError("one of '=', '*=', '=*'", token, context.input)
		}
		joinType := token.Value
		joinTypeToken := token
		token, ok = context.NextToken()
		if !ok {
			return nil, lexer.Token{}, endOfInputError()
		}
		if token.Type != IdentTokenType {
			return nil, lexer.Token{}, foundUnexpectedTokenError("identifier", token, context.input)
		}
		rightCol := token.Value
		rightToken := token
		joinElements = append(joinElements, JoinElement{
			LeftToken:     leftToken,
			RightToken:    rightToken,
			JoinTypeToken: joinTypeToken,
			LeftCol:       leftCol,
			JoinType:      joinType,
			RightCol:      rightCol,
		})
	}
}

func NewKafkaInDesc() *KafkaInDesc {
	super := &KafkaInDesc{}
	super.BaseDesc.super = super
	return super
}

type KafkaInDesc struct {
	BaseDesc
	Partitions           int
	WatermarkType        *string
	WatermarkLateness    *time.Duration
	WatermarkIdleTimeout *time.Duration
}

func (k *KafkaInDesc) parse(context *ParseContext) error {
	context.MoveCursor(2)
	partitions, err := parsePartitions(context)
	if err != nil {
		return err
	}
	k.Partitions = partitions
	for {
		token, ok := context.NextToken()
		if !ok {
			break
		}
		if token.Value == ")" {
			// End of operator definition
			return nil
		}
		// Must be optional arg
		if token.Type != IdentTokenType {
			return foundUnexpectedTokenError("identifier", token, context.input)
		}
		switch token.Value {

		case "watermark_type":
			if k.WatermarkType != nil {
				return duplicateArgumentError(token, context)
			}
			wmType, err := parseWatermarkType(context)
			if err != nil {
				return err
			}
			k.WatermarkType = &wmType
		case "watermark_lateness":
			if k.WatermarkLateness != nil {
				return duplicateArgumentError(token, context)
			}
			wmLateness, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			k.WatermarkLateness = &wmLateness
		case "watermark_idle_timeout":
			if k.WatermarkIdleTimeout != nil {
				return duplicateArgumentError(token, context)
			}
			wmIdleTimeout, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			k.WatermarkIdleTimeout = &wmIdleTimeout
		default:
			if token.Value == "partitions" {
				return duplicateArgumentError(token, context)
			}
			return unknownArgumentError(token, context)
		}
	}
	return nil
}

func NewKafkaOutDesc() *KafkaOutDesc {
	super := &KafkaOutDesc{}
	super.BaseDesc.super = super
	return super
}

type KafkaOutDesc struct {
	BaseDesc
	Retention *time.Duration
}

func (k *KafkaOutDesc) parse(context *ParseContext) error {
	context.MoveCursor(2)
	retention, err := parseOptionalRetention(context)
	if err != nil {
		return err
	}
	k.Retention = retention
	return nil
}

func NewTopicDesc() *TopicDesc {
	super := &TopicDesc{}
	super.BaseDesc.super = super
	return super
}

type TopicDesc struct {
	BaseDesc
	Retention            *time.Duration
	Partitions           int
	WatermarkType        *string
	WatermarkLateness    *time.Duration
	WatermarkIdleTimeout *time.Duration
}

func (t *TopicDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)

	partitions, err := parsePartitions(context)
	if err != nil {
		return err
	}
	t.Partitions = partitions
	for {
		token, ok := context.NextToken()
		if !ok {
			break
		}
		if token.Value == ")" {
			// End of operator definition
			return nil
		}
		// Must be optional arg
		if token.Type != IdentTokenType {
			return foundUnexpectedTokenError("identifier", token, context.input)
		}
		switch token.Value {

		case "watermark_type":
			if t.WatermarkType != nil {
				return duplicateArgumentError(token, context)
			}
			wmType, err := parseWatermarkType(context)
			if err != nil {
				return err
			}
			t.WatermarkType = &wmType
		case "watermark_lateness":
			if t.WatermarkLateness != nil {
				return duplicateArgumentError(token, context)
			}
			wmLateness, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			t.WatermarkLateness = &wmLateness
		case "watermark_idle_timeout":
			if t.WatermarkIdleTimeout != nil {
				return duplicateArgumentError(token, context)
			}
			wmIdleTimeout, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			t.WatermarkIdleTimeout = &wmIdleTimeout
		case "retention":
			if t.Retention != nil {
				return duplicateArgumentError(token, context)
			}
			retention, err := parseDurationArg(context)
			if err != nil {
				return err
			}
			t.Retention = &retention
		default:
			if token.Value == "partitions" {
				return duplicateArgumentError(token, context)
			}
			return unknownArgumentError(token, context)
		}
	}
	return nil
}

func NewUnionDesc() *UnionDesc {
	super := &UnionDesc{}
	super.BaseDesc.super = super
	return super
}

type UnionDesc struct {
	BaseDesc
	StreamNames []string
}

func (u *UnionDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	exprs, _, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(exprs) < 2 {
		nextToken, ok := context.PeekToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition(`at least two input stream names are required`, nextToken.Pos, context.input)
	}
	if _, err := context.expectToken(")"); err != nil {
		return err
	}
	u.StreamNames = exprs
	return nil
}

func NewBackfillDesc() *BackfillDesc {
	super := &BackfillDesc{}
	super.BaseDesc.super = super
	return super
}

type BackfillDesc struct {
	BaseDesc
}

func (b *BackfillDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	_, err := context.expectToken(")")
	return err
}

func NewGetDesc() *GetDesc {
	super := &GetDesc{}
	super.BaseDesc.super = super
	return super
}

type GetDesc struct {
	BaseDesc
	KeyExprs  []ExprDesc
	TableName string
}

func (g *GetDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	_, keyExprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(keyExprs) < 1 {
		nextToken, ok := context.PeekToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition("there must be at least one key expression", nextToken.Pos, context.input)
	}
	g.KeyExprs = keyExprs
	_, err = context.expectToken("from")
	if err != nil {
		return err
	}
	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	g.TableName = token.Value
	_, err = context.expectToken(")")
	return err
}

func (g *GetDesc) clearTokenState() {
	g.BaseDesc.clearTokenState()
	for _, expr := range g.KeyExprs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func NewScanDesc() *ScanDesc {
	super := &ScanDesc{}
	super.BaseDesc.super = super
	return super
}

type ScanDesc struct {
	BaseDesc
	FromKeyExprs []ExprDesc
	ToKeyExprs   []ExprDesc
	ToIncl       bool
	FromIncl     bool
	TableName    string
	All          bool
}

func (s *ScanDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	if err := s.parseRange(context); err != nil {
		return err
	}
	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Type != IdentTokenType {
		return foundUnexpectedTokenError("identifier", token, context.input)
	}
	s.TableName = token.Value
	_, err := context.expectToken(")")
	return err
}

func (s *ScanDesc) parseRange(context *ParseContext) error {
	nextToken, ok := context.PeekToken()
	if !ok {
		return endOfInputError()
	}
	if nextToken.Value == ")" {
		return endOfInputError()
	}
	if nextToken.Value == "all" {
		context.NextToken()
		s.All = true
		_, err := context.expectToken("from")
		return err
	}
	s.FromIncl = true
	s.ToIncl = false
	startKeyStrs, startKeyExprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(startKeyExprs) < 1 {
		nextToken, ok := context.PeekToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition(`there must be at least one expression`, nextToken.Pos, context.input)
	}
	if startKeyStrs[0] != "start" {
		s.FromKeyExprs = startKeyExprs
	}

	token, ok := context.NextToken()
	if !ok {
		return endOfInputError()
	}

	if token.Value != "to" {
		if token.Value == "excl" {
			s.FromIncl = false
		} else if token.Value != "incl" {
			return foundUnexpectedTokenError(expectedStr("to", "excl", "incl"), token, context.input)
		}
		if _, err := context.expectToken("to"); err != nil {
			return err
		}
	}

	toKeyStrs, toKeyExprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(toKeyExprs) < 1 {
		nextToken, ok := context.PeekToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition(`there must be at least one expression`, nextToken.Pos, context.input)
	}
	if toKeyStrs[0] != "end" {
		s.ToKeyExprs = toKeyExprs
	}

	token, ok = context.NextToken()
	if !ok {
		return endOfInputError()
	}
	if token.Value != "from" {
		if token.Value == "incl" {
			s.ToIncl = true
		} else if token.Value != "excl" {
			return foundUnexpectedTokenError(expectedStr("from", "excl", "incl"), token, context.input)
		}
		if _, err := context.expectToken("from"); err != nil {
			return err
		}
	}
	return nil
}

func (s *ScanDesc) clearTokenState() {
	s.BaseDesc.clearTokenState()
	for _, expr := range s.FromKeyExprs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
	for _, expr := range s.ToKeyExprs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func NewSortDesc() *SortDesc {
	super := &SortDesc{}
	super.BaseDesc.super = super
	return super
}

type SortDesc struct {
	BaseDesc
	SortExprs []ExprDesc
}

func (s *SortDesc) parse(context *ParseContext) error {
	context.MoveCursor(1)
	_, err := context.expectToken("by")
	if err != nil {
		return err
	}
	_, exprs, err := parseExpressions(context)
	if err != nil {
		return err
	}
	if len(exprs) < 1 {
		nextToken, ok := context.PeekToken()
		if !ok {
			return endOfInputError()
		}
		return errorAtPosition(`at least one sort expression must be specified`, nextToken.Pos, context.input)
	}
	if _, err := context.expectToken(")"); err != nil {
		return err
	}
	s.SortExprs = exprs
	return nil
}

func (s *SortDesc) clearTokenState() {
	s.BaseDesc.clearTokenState()
	for _, expr := range s.SortExprs {
		clearable, ok := expr.(tokenClearable)
		if ok {
			clearable.clearTokenState()
		}
	}
}

func parseOptionalRetention(context *ParseContext) (*time.Duration, error) {
	token, ok := context.NextToken()
	if !ok {
		return nil, endOfInputError()
	}
	if token.Value == ")" {
		return nil, nil
	}
	if token.Value != "retention" {
		return nil, foundUnexpectedTokenError("'retention'", token, context.input)
	}
	retention, err := parseDurationArg(context)
	if err != nil {
		return nil, err
	}
	if _, err := context.expectToken(")"); err != nil {
		return nil, err
	}
	return &retention, nil
}

func parseBool(context *ParseContext) (bool, error) {
	tok, err := parseNamedArgValue(BoolLiteralTokenType, "bool", context)
	if err != nil {
		return false, err
	}
	bVal := tok.Value
	var b bool
	if bVal == "true" {
		b = true
	} else if bVal == "false" {
		b = false
	} else {
		msg := fmt.Sprintf(`expected 'true' or 'false' but found '%s'`, tok.Value)
		return false, errorAtPosition(msg, tok.Pos, context.input)
	}
	return b, nil
}

func emptyKeyExpressionsError(lastPos lexer.Position, context *ParseContext) error {
	return errorAtPosition("there must be at least one expression", lastPos, context.input)
}

func parseExpressions(context *ParseContext) ([]string, []ExprDesc, error) {
	expressions, allTokens, err := context.parser.ParseExpressionList(context)
	if err != nil {
		return nil, nil, err
	}
	// hacky - convert expressions back to strings - this will disappear once we use our new expressions for evaluation too!
	exprStrs := make([]string, len(allTokens))
	for i, tokens := range allTokens {
		var sb strings.Builder
		for _, tok := range tokens {
			isAs := tok.Value == "as"
			if isAs {
				sb.WriteRune(' ')
			}
			sb.WriteString(tok.Value)
			if isAs {
				sb.WriteRune(' ')
			}
		}
		exprStrs[i] = sb.String()
	}
	return exprStrs, expressions, nil
}

func unknownArgumentError(token lexer.Token, context *ParseContext) error {
	msg := fmt.Sprintf(`unknown argument '%s'`, token.Value)
	return errorAtPosition(msg, token.Pos, context.input)
}

func parseTopicName(context *ParseContext) (string, error) {
	token, err := context.expectToken()
	if err != nil {
		return "", err
	}
	if token.Type != IdentTokenType {
		return "", foundUnexpectedTokenError("identifier", token, context.input)
	}
	return token.Value, nil
}

func parsePartitions(context *ParseContext) (int, error) {
	// partitions is mandatory
	tok, err := parseNamedArg("partitions", IntegerTokenType, "integer", context)
	if err != nil {
		return 0, err
	}
	partitions, err := strconv.Atoi(tok.Value)
	if err != nil {
		return 0, errorAtPosition(fmt.Sprintf("%s is not an integer", tok.Value), tok.Pos,
			context.input)
	}
	return partitions, nil
}

func skipPastOptionalEquals(context *ParseContext) (lexer.Token, bool, bool) {
	var token lexer.Token
	skippedPastEquals := false
	for {
		var ok bool
		token, ok = context.NextToken()
		if !ok {
			return lexer.Token{}, false, false
		}
		// "=" is optional
		if token.Value != "=" {
			return token, skippedPastEquals, true
		}
		skippedPastEquals = true
	}
}

func parseProps(context *ParseContext) (map[string]string, error) {
	token, skippedPastEquals, ok := skipPastOptionalEquals(context)
	if !ok {
		return nil, endOfInputError()
	}
	if token.Value != "(" {
		expected := `'('`
		if !skippedPastEquals {
			expected = `'=' or '('`
		}
		return nil, foundUnexpectedTokenError(expected, token, context.input)
	}
	props := map[string]string{}
	for {
		token, ok := context.NextToken()
		if !ok {
			return nil, endOfInputError()
		}
		if token.Value == ")" {
			// End of props definition
			return props, nil
		}
		if token.Type != StringLiteralTokenType {
			return nil, foundUnexpectedTokenError("string literal", token, context.input)
		}
		tok, err := parseNamedArgValue(StringLiteralTokenType, "string literal", context)
		if err != nil {
			return nil, err
		}
		props[stripQuotes(token.Value)] = stripQuotes(tok.Value)
	}
}

func parseDurationArg(context *ParseContext) (time.Duration, error) {
	tok, err := parseNamedArgValue(DurationTokenType, "duration", context)
	if err != nil {
		return 0, err
	}
	dur, err := time.ParseDuration(tok.Value)
	if err != nil {
		return 0, err
	}
	return dur, nil
}

func parseWatermarkType(context *ParseContext) (string, error) {
	tok, err := parseNamedArgValue(IdentTokenType, "identifier", context)
	if err != nil {
		return "", err
	}
	if tok.Value != "event_time" && tok.Value != "processing_time" {
		return "", foundUnexpectedTokenError(expectedStr("event_time", "processing_time"),
			tok, context.input)
	}
	return tok.Value, nil
}

func parseNamedArg(argName string, argType lexer.TokenType, argTypeStr string, context *ParseContext) (lexer.Token, error) {
	_, err := context.expectToken(argName)
	if err != nil {
		return lexer.Token{}, err
	}
	return parseNamedArgValue(argType, argTypeStr, context)
}

func parseNamedArgValue(argType lexer.TokenType, argTypeStr string, context *ParseContext) (lexer.Token, error) {
	token, skippedPastEquals, ok := skipPastOptionalEquals(context)
	if !ok {
		return lexer.Token{}, endOfInputError()
	}
	if token.Type != argType {
		expected := argTypeStr
		if !skippedPastEquals {
			expected = fmt.Sprintf(`'=' or %s`, argTypeStr)
		}
		return lexer.Token{}, foundUnexpectedTokenError(expected, token, context.input)
	}
	return token, nil
}

func (pc *ParseContext) expectToken(expected ...string) (lexer.Token, error) {
	token, ok := pc.NextToken()
	if !ok {
		return lexer.Token{}, endOfInputError()
	}
	if len(expected) == 0 {
		return token, nil
	}
	for _, exp := range expected {
		if token.Value == exp {
			return token, nil
		}
	}
	return lexer.Token{}, foundUnexpectedTokenError(expectedStr(expected...), token, pc.input)
}

func endOfInputError() error {
	return errors.NewParseError(`reached end of statement`)
}

func foundUnexpectedTokenError(expectedType string, token lexer.Token, input string) error {
	msg := fmt.Sprintf(`expected %s but found '%s'`, expectedType, token.Value)
	return errorAtPosition(msg, token.Pos, input)
}

func duplicateArgumentError(token lexer.Token, context *ParseContext) error {
	msg := fmt.Sprintf(`argument '%s' is duplicated`, token.Value)
	return errorAtPosition(msg, token.Pos, context.input)
}

func errorAtPosition(msg string, pos lexer.Position, input string) error {
	msg = MessageWithPosition(msg, pos, input)
	return errors.NewParseError(msg)
}

func MessageWithPosition(msg string, pos lexer.Position, input string) string {
	return fmt.Sprintf("%s (line %d column %d):\n%s", msg, pos.Line, pos.Column, lineWithPosHighlight(input, pos))
}

func expectedStr(expected ...string) string {
	sb := strings.Builder{}
	if len(expected) > 1 {
		sb.WriteString("one of: ")
	}
	for i := 0; i < len(expected); i++ {
		sb.WriteRune('\'')
		sb.WriteString(expected[i])
		sb.WriteRune('\'')
		if i != len(expected)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func stripQuotes(literal string) string {
	// We know that string literal always has quotes so this is safe
	return literal[1 : len(literal)-1]
}
