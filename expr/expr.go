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

package expr

import (
	"encoding/json"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
	"strings"
)

type Expression interface {
	EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error)
	EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error)
	EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error)
	EvalDecimal(rowIndex int, batch *evbatch.Batch) (types.Decimal, bool, error)
	EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error)
	EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error)
	EvalTimestamp(rowIndex int, batch *evbatch.Batch) (types.Timestamp, bool, error)
	ResultType() types.ColumnType
}

type ExternalInvokerFactory interface {
	GetFunctionMetadata(fullFunctionName string) (FunctionMetadata, bool)
	CreateExternalInvoker(fullFunctionName string) (ExternalInvoker, error)
}

type FunctionMetadata struct {
	ParamTypes []types.ColumnType
	ReturnType types.ColumnType
}

//goland:noinspection GoMixedReceiverTypes
func (f FunctionMetadata) MarshalJSON() ([]byte, error) {
	var builder strings.Builder
	builder.WriteString(`{"paramTypes":[`)
	for i, pt := range f.ParamTypes {
		builder.WriteRune('"')
		builder.WriteString(pt.String())
		builder.WriteRune('"')
		if i != len(f.ParamTypes)-1 {
			builder.WriteRune(',')
		}
	}
	builder.WriteString(`],"returnType":"`)
	builder.WriteString(f.ReturnType.String())
	builder.WriteString(`"}`)
	return []byte(builder.String()), nil
}

//goland:noinspection GoMixedReceiverTypes
func (f *FunctionMetadata) UnmarshalJSON(buff []byte) error {
	funcMetaMap := map[string]any{}
	if err := json.Unmarshal(buff, &funcMetaMap); err != nil {
		return err
	}
	pTypes, ok := funcMetaMap["paramTypes"]
	if !ok {
		return errors.Error("json object is missing a 'paramTypes' field")
	}
	pTypesArr, ok := pTypes.([]any)
	if !ok {
		return errors.Error("'paramTypes' field must contain a json array")
	}
	var paramTypes []types.ColumnType
	for _, pType := range pTypesArr {
		pTypeStr, ok := pType.(string)
		if !ok {
			return errors.Error("paramTypes must be strings")
		}
		paramType, err := types.StringToColumnType(pTypeStr)
		if err != nil {
			return err
		}
		paramTypes = append(paramTypes, paramType)
	}
	retType, ok := funcMetaMap["returnType"]
	if !ok {
		return errors.Error("json object is missing a 'returnType' field")
	}
	retTypeStr, ok := retType.(string)
	if !ok {
		return errors.Error("'returnType' field must contain a string")
	}
	returnType, err := types.StringToColumnType(retTypeStr)
	if err != nil {
		return err
	}
	f.ParamTypes = paramTypes
	f.ReturnType = returnType
	return nil
}

type ExternalInvoker interface {
	Invoke(args []any) (any, error)
}

type ExpressionFactory struct {
	ExternalInvokerFactory ExternalInvokerFactory
}

func (f *ExpressionFactory) CreateExpression(desc parser.ExprDesc, schema *evbatch.EventSchema) (Expression, error) {
	switch op := desc.(type) {
	case *parser.IntegerConstExprDesc:
		return NewIntegerConstantExpr(int64(op.Value)), nil
	case *parser.FloatConstExprDesc:
		return NewFloatConstantExpr(op.Value), nil
	case *parser.BoolConstExprDesc:
		return NewBoolConstantExpr(op.Value), nil
	case *parser.StringConstExprDesc:
		return NewStringConstantExpr(op.Value), nil
	case *parser.IdentifierExprDesc:
		return createColumnExpr(op, schema)
	case *parser.BinaryOperatorExprDesc:
		return f.createBinaryOperator(op, schema)
	case *parser.UnaryOperatorExprDesc:
		return f.createUnaryOperator(op, schema)
	case *parser.FunctionExprDesc:
		return f.createFunctionOperator(op, schema)
	default:
		panic("unsupported expression")
	}
}

func createColumnExpr(desc *parser.IdentifierExprDesc, schema *evbatch.EventSchema) (Expression, error) {
	colIndex := -1
	var colType types.ColumnType
	for i, cName := range schema.ColumnNames() {
		if cName == desc.IdentifierName {
			colIndex = i
			colType = schema.ColumnTypes()[i]
			break
		}
	}
	if colIndex == -1 {
		return nil, desc.ErrorAtPosition("unknown column '%s'. (available columns: %s)", desc.IdentifierName, schema.String())
	}
	return NewColumnExpression(colIndex, colType), nil
}

func (f *ExpressionFactory) createBinaryOperator(desc *parser.BinaryOperatorExprDesc, schema *evbatch.EventSchema) (Expression, error) {
	left, err := f.CreateExpression(desc.Left, schema)
	if err != nil {
		return nil, err
	}
	right, err := f.CreateExpression(desc.Right, schema)
	if err != nil {
		return nil, err
	}
	switch desc.Op {
	case "+":
		return NewAddOperator(left, right, desc)
	case "-":
		return NewSubtractOperator(left, right, desc)
	case "*":
		return NewMultiplyOperator(left, right, desc)
	case "/":
		return NewDivideOperator(left, right, desc)
	case "%":
		return NewModulusOperator(left, right, desc)
	case "==":
		return NewEqualsOperator(left, right, desc)
	case "!=":
		return NewNotEqualsOperator(left, right, desc)
	case ">":
		return NewGreaterThanOperator(left, right, desc)
	case ">=":
		return NewGreaterOrEqualsOperator(left, right, desc)
	case "<":
		return NewLessThanOperator(left, right, desc)
	case "<=":
		return NewLessOrEqualsOperator(left, right, desc)
	case "&&":
		return NewLogicalAndOperator(left, right, desc)
	case "||":
		return NewLogicalOrOperator(left, right, desc)
	default:
		panic("unsupported op")
	}
}

func (f *ExpressionFactory) createUnaryOperator(desc *parser.UnaryOperatorExprDesc, schema *evbatch.EventSchema) (Expression, error) {
	operand, err := f.CreateExpression(desc.Operand, schema)
	if err != nil {
		return nil, err
	}
	if desc.Op == "!" {
		return NewLogicalNotOperator(operand, desc)
	}
	panic("unexpected operator")
}

func resultTypesEqual(leftType types.ColumnType, rightType types.ColumnType) bool {
	if leftType.ID() != rightType.ID() {
		return false
	}
	if leftType.ID() == types.ColumnTypeIDDecimal {
		ld := leftType.(*types.DecimalType)
		rd := rightType.(*types.DecimalType)
		return ld.Scale == rd.Scale && ld.Precision == rd.Precision
	}
	return true
}

func (f *ExpressionFactory) createFunctionOperator(desc *parser.FunctionExprDesc, schema *evbatch.EventSchema) (Expression, error) {
	args := make([]Expression, len(desc.ArgExprs))
	for i, argDesc := range desc.ArgExprs {
		argExpr, err := f.CreateExpression(argDesc, schema)
		if err != nil {
			return nil, err
		}
		args[i] = argExpr
	}
	switch desc.FunctionName {
	case "if":
		return NewIfFunction(args, desc)
	case "is_null":
		return NewIsNullFunction(args, desc)
	case "is_not_null":
		return NewIsNotNullFunction(args, desc)
	case "in":
		return NewInFunction(args, desc)
	case "case":
		return NewCaseFunction(args, desc)
	case "decimal_shift":
		return NewDecimalShiftFunction(args, desc)
	case "len":
		return NewLenFunction(args, desc)
	case "concat":
		return NewConcatFunction(args, desc)
	case "starts_with":
		return NewStartsWithFunction(args, desc)
	case "ends_with":
		return NewEndsWithFunction(args, desc)
	case "matches":
		return NewMatchesFunction(args, desc)
	case "trim":
		return NewTrimFunction(args, desc)
	case "ltrim":
		return NewLTrimFunction(args, desc)
	case "rtrim":
		return NewRTrimFunction(args, desc)
	case "to_lower":
		return NewToLowerFunction(args, desc)
	case "to_upper":
		return NewToUpperFunction(args, desc)
	case "sub_str":
		return NewSubStrFunction(args, desc)
	case "replace":
		return NewReplaceFunction(args, desc)
	case "sprintf":
		return NewSprintfFunction(args, desc)
	case "to_int":
		return NewToIntFunction(args, desc)
	case "to_float":
		return NewToFloatFunction(args, desc)
	case "to_string":
		return NewToStringFunction(args, desc)
	case "to_decimal":
		return NewToDecimalFunction(args, desc)
	case "to_bytes":
		return NewToBytesFunction(args, desc)
	case "to_timestamp":
		return NewToTimestampFunction(args, desc)
	case "format_date":
		return NewFormatDateFunction(args, desc)
	case "parse_date":
		return NewParseDateFunction(args, desc)
	case "year":
		return NewYearFunction(args, desc)
	case "month":
		return NewMonthFunction(args, desc)
	case "day":
		return NewDayFunction(args, desc)
	case "hour":
		return NewHourFunction(args, desc)
	case "minute":
		return NewMinuteFunction(args, desc)
	case "second":
		return NewSecondFunction(args, desc)
	case "millis":
		return NewMillisFunction(args, desc)
	case "now":
		return NewNowFunction(args, desc)
	case "json_int":
		return NewJsonIntFunction(args, desc)
	case "json_float":
		return NewJsonFloatFunction(args, desc)
	case "json_bool":
		return NewJsonBoolFunction(args, desc)
	case "json_string":
		return NewJsonStringFunction(args, desc)
	case "json_raw":
		return NewJsonRawFunction(args, desc)
	case "json_is_null":
		return NewJsonIsNullFunction(args, desc)
	case "json_type":
		return NewJsonTypeFunction(args, desc)
	case "kafka_build_headers":
		return NewKafkaBuildHeadersFunction(args, desc)
	case "kafka_header":
		return NewKafkaHeaderFunction(args, desc)
	case "bytes_slice":
		return NewBytesSliceFunction(args, desc)
	case "uint64_be":
		return NewUint64BEFunction(args, desc)
	case "uint64_le":
		return NewUint64LEFunction(args, desc)
	case "abs":
		return NewAbsFunction(args, desc)
	default:
		// External function
		return NewExternalFunction(args, desc, f.ExternalInvokerFactory)
	}
}

func NewColumnExpression(colIndex int, columnType types.ColumnType) *ColumnExpr {
	return &ColumnExpr{colIndex: colIndex, exprType: columnType}
}

type ColumnExpr struct {
	colIndex int
	exprType types.ColumnType
}

func (c *ColumnExpr) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	col := batch.GetIntColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return 0, true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error) {
	col := batch.GetFloatColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return 0, true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	col := batch.GetBoolColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return false, true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) EvalDecimal(rowIndex int, batch *evbatch.Batch) (types.Decimal, bool, error) {
	col := batch.GetDecimalColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return types.Decimal{}, true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	col := batch.GetStringColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return "", true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	col := batch.GetBytesColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return nil, true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) EvalTimestamp(rowIndex int, batch *evbatch.Batch) (types.Timestamp, bool, error) {
	col := batch.GetTimestampColumn(c.colIndex)
	if col.IsNull(rowIndex) {
		return types.Timestamp{}, true, nil
	}
	return col.Get(rowIndex), false, nil
}

func (c *ColumnExpr) ResultType() types.ColumnType {
	return c.exprType
}

type IntegerConstantExpr struct {
	baseExpr
	val int64
}

func NewIntegerConstantExpr(val int64) Expression {
	return &IntegerConstantExpr{
		val: val,
	}
}

func (i *IntegerConstantExpr) EvalInt(_ int, _ *evbatch.Batch) (int64, bool, error) {
	return i.val, false, nil
}

func (i *IntegerConstantExpr) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type FloatConstantExpr struct {
	baseExpr
	val float64
}

func NewFloatConstantExpr(val float64) Expression {
	return &FloatConstantExpr{
		val: val,
	}
}

func (fc *FloatConstantExpr) EvalFloat(_ int, _ *evbatch.Batch) (float64, bool, error) {
	return fc.val, false, nil
}

func (fc *FloatConstantExpr) ResultType() types.ColumnType {
	return types.ColumnTypeFloat
}

type BoolConstantExpr struct {
	baseExpr
	val bool
}

func NewBoolConstantExpr(val bool) Expression {
	return &BoolConstantExpr{
		val: val,
	}
}

func (bc *BoolConstantExpr) EvalBool(_ int, _ *evbatch.Batch) (bool, bool, error) {
	return bc.val, false, nil
}

func (bc *BoolConstantExpr) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type StringConstantExpr struct {
	baseExpr
	val string
}

func NewStringConstantExpr(val string) Expression {
	return &StringConstantExpr{
		val: val,
	}
}

func (sc *StringConstantExpr) EvalString(_ int, _ *evbatch.Batch) (string, bool, error) {
	return sc.val, false, nil
}

func (sc *StringConstantExpr) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type baseExpr struct {
}

func (b *baseExpr) EvalInt(_ int, _ *evbatch.Batch) (int64, bool, error) {
	panic("not supported")
}

func (b *baseExpr) EvalFloat(_ int, _ *evbatch.Batch) (float64, bool, error) {
	panic("not supported")
}

func (b *baseExpr) EvalBool(_ int, _ *evbatch.Batch) (bool, bool, error) {
	panic("not supported")
}

func (b *baseExpr) EvalDecimal(_ int, _ *evbatch.Batch) (types.Decimal, bool, error) {
	panic("not supported")
}

func (b *baseExpr) EvalString(_ int, _ *evbatch.Batch) (string, bool, error) {
	panic("not supported")
}

func (b *baseExpr) EvalBytes(_ int, _ *evbatch.Batch) ([]byte, bool, error) {
	panic("not supported")
}

func (b *baseExpr) EvalTimestamp(_ int, _ *evbatch.Batch) (types.Timestamp, bool, error) {
	panic("not supported")
}
