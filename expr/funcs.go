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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
	"github.com/tidwall/gjson"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type IfFunction struct {
	baseExpr
	testExpr  Expression
	trueExpr  Expression
	falseExpr Expression
	exprType  types.ColumnType
}

func NewIfFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*IfFunction, error) {
	if len(argExprs) != 3 {
		return nil, desc.ErrorAtPosition("'if' function requires 3 arguments - %d found", len(argExprs))
	}
	testExpr := argExprs[0]
	if testExpr.ResultType() != types.ColumnTypeBool {
		return nil, desc.ErrorAtPosition("'if' function first argument must be of type bool - %s found", testExpr.ResultType().String())
	}
	trueExpr := argExprs[1]
	falseExpr := argExprs[2]
	if !types.ColumnTypesEqual(trueExpr.ResultType(), falseExpr.ResultType()) {
		return nil, desc.ErrorAtPosition("'if' function second and third arguments must be of same type - found %s and %s",
			trueExpr.ResultType().String(), falseExpr.ResultType().String())
	}
	return &IfFunction{
		testExpr:  testExpr,
		trueExpr:  trueExpr,
		falseExpr: falseExpr,
		exprType:  trueExpr.ResultType(),
	}, nil
}

func (i *IfFunction) EvalInt(rowIndex int, inBatch *evbatch.Batch) (int64, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalInt(rowIndex, inBatch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalInt(rowIndex, inBatch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return val, false, nil
	}
}

func (i *IfFunction) EvalFloat(rowIndex int, inBatch *evbatch.Batch) (float64, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalFloat(rowIndex, inBatch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalFloat(rowIndex, inBatch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return val, false, nil
	}
}

func (i *IfFunction) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalBool(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if null {
			return false, true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalBool(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if null {
			return false, true, nil
		}
		return val, false, nil
	}
}

func (i *IfFunction) EvalDecimal(rowIndex int, inBatch *evbatch.Batch) (types.Decimal, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalDecimal(rowIndex, inBatch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalDecimal(rowIndex, inBatch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		return val, false, nil
	}
}

func (i *IfFunction) EvalString(rowIndex int, inBatch *evbatch.Batch) (string, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalString(rowIndex, inBatch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalString(rowIndex, inBatch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return val, false, nil
	}
}

func (i *IfFunction) EvalBytes(rowIndex int, inBatch *evbatch.Batch) ([]byte, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalBytes(rowIndex, inBatch)
		if err != nil {
			return nil, false, err
		}
		if null {
			return nil, true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalBytes(rowIndex, inBatch)
		if err != nil {
			return nil, false, err
		}
		if null {
			return nil, true, nil
		}
		return val, false, nil
	}
}
func (i *IfFunction) EvalTimestamp(rowIndex int, inBatch *evbatch.Batch) (types.Timestamp, bool, error) {
	testVal, null, err := i.testExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, nil
	}
	if testVal {
		val, null, err := i.trueExpr.EvalTimestamp(rowIndex, inBatch)
		if err != nil {
			return types.Timestamp{}, false, err
		}
		if null {
			return types.Timestamp{}, true, nil
		}
		return val, false, nil
	} else {
		val, null, err := i.falseExpr.EvalTimestamp(rowIndex, inBatch)
		if err != nil {
			return types.Timestamp{}, false, err
		}
		if null {
			return types.Timestamp{}, true, nil
		}
		return val, false, nil
	}
}

func (i *IfFunction) Eval() (evbatch.Column, error) {
	panic("not supported")
}

func (i *IfFunction) ResultType() types.ColumnType {
	return i.exprType
}

type IsNullFunction struct {
	baseExpr
	operand  Expression
	operType types.ColumnType
}

func NewIsNullFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*IsNullFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'is_null' function requires 1 argument - %d found", len(argExprs))
	}
	return &IsNullFunction{
		operand:  argExprs[0],
		operType: argExprs[0].ResultType(),
	}, nil
}

func (in *IsNullFunction) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	var null bool
	var err error
	switch in.operType.ID() {
	case types.ColumnTypeIDInt:
		_, null, err = in.operand.EvalInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		_, null, err = in.operand.EvalFloat(rowIndex, inBatch)
	case types.ColumnTypeIDBool:
		_, null, err = in.operand.EvalBool(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		_, null, err = in.operand.EvalDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		_, null, err = in.operand.EvalString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		_, null, err = in.operand.EvalBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		_, null, err = in.operand.EvalTimestamp(rowIndex, inBatch)
	default:
		panic("unexpected column type")
	}
	if err != nil {
		return false, false, err
	}
	return null, false, nil
}

func (in *IsNullFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type IsNotNullFunction struct {
	baseExpr
	operand  Expression
	operType types.ColumnType
}

func NewIsNotNullFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*IsNotNullFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'is_not_null' function requires 1 argument - %d found", len(argExprs))
	}
	return &IsNotNullFunction{
		operand:  argExprs[0],
		operType: argExprs[0].ResultType(),
	}, nil
}

func (in *IsNotNullFunction) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	var null bool
	var err error
	switch in.operType.ID() {
	case types.ColumnTypeIDInt:
		_, null, err = in.operand.EvalInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		_, null, err = in.operand.EvalFloat(rowIndex, inBatch)
	case types.ColumnTypeIDBool:
		_, null, err = in.operand.EvalBool(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		_, null, err = in.operand.EvalDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		_, null, err = in.operand.EvalString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		_, null, err = in.operand.EvalBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		_, null, err = in.operand.EvalTimestamp(rowIndex, inBatch)
	default:
		panic("unexpected column type")
	}
	if err != nil {
		return false, false, err
	}
	return !null, false, nil
}

func (in *IsNotNullFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type InFunction struct {
	baseExpr
	testOperand Expression
	inOperands  []Expression
	opersType   types.ColumnTypeID
}

func NewInFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*InFunction, error) {
	if len(argExprs) < 3 {
		return nil, desc.ErrorAtPosition("'in' function requires at least 3 arguments - %d found", len(argExprs))
	}
	operType := argExprs[0].ResultType()
	for i, argExpr := range argExprs[1:] {
		if argExpr.ResultType().ID() != operType.ID() {
			return nil, desc.ErrorAtPosition("'in' function arguments must have same type - first arg has type %s - arg at position %d has type %s",
				operType.String(), i+1, argExpr.ResultType().String())
		}
	}
	return &InFunction{
		testOperand: argExprs[0],
		inOperands:  argExprs[1:],
		opersType:   operType.ID(),
	}, nil
}

func (iff *InFunction) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	var val bool
	var null bool
	var err error
	switch iff.opersType {
	case types.ColumnTypeIDInt:
		val, null, err = iff.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		val, null, err = iff.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDBool:
		val, null, err = iff.evalFromBool(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		val, null, err = iff.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		val, null, err = iff.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		val, null, err = iff.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		val, null, err = iff.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unexpected column type")
	}
	return val, null, err
}

func (iff *InFunction) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalInt(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (val == testVal) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalFloat(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (val == testVal) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) evalFromBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalBool(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (val == testVal) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalDecimal(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (val.Equals(&testVal)) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalString(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (val == testVal) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalBytes(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (bytes.Equal(val, testVal)) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	testVal, null, err := iff.testOperand.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	for _, oper := range iff.inOperands {
		val, null, err := oper.EvalTimestamp(rowIndex, inBatch)
		if err != nil {
			return false, false, err
		}
		if !null && (val.Val == testVal.Val) {
			return true, false, nil
		}
	}
	return false, false, nil
}

func (iff *InFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type CaseFunction struct {
	testExpr    Expression
	defaultExpr Expression
	caseExprs   []Expression
	retExprs    []Expression
	resultType  types.ColumnType
}

func NewCaseFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*CaseFunction, error) {
	if len(argExprs) < 4 {
		return nil, desc.ErrorAtPosition("'case' function requires at least 4 arguments - %d found", len(argExprs))
	}
	if len(argExprs)%2 == 1 {
		return nil, desc.ErrorAtPosition("'case' function requires an even number of arguments - %d found", len(argExprs))
	}
	testExpr := argExprs[0]
	defaultExpr := argExprs[len(argExprs)-1]
	var caseExprs []Expression
	var retExprs []Expression
	for i := 1; i < len(argExprs)-1; i += 2 {
		caseExpr := argExprs[i]
		if caseExpr.ResultType().ID() != testExpr.ResultType().ID() {
			return nil, desc.ErrorAtPosition("'case' function cases must have same type as test expression - test expression has type %s - arg at position %d has type %s",
				testExpr.ResultType().String(), len(caseExprs), caseExpr.ResultType().String())
		}
		retExpr := argExprs[i+1]
		if retExpr.ResultType().ID() != defaultExpr.ResultType().ID() {
			return nil, desc.ErrorAtPosition("'case' function return expressions must have same type as default expression - default expression has type %s - arg at position %d has type %s",
				defaultExpr.ResultType().String(), len(retExprs), retExpr.ResultType().String())
		}
		caseExprs = append(caseExprs, caseExpr)
		retExprs = append(retExprs, retExpr)
	}

	return &CaseFunction{
		testExpr:    testExpr,
		defaultExpr: defaultExpr,
		caseExprs:   caseExprs,
		retExprs:    retExprs,
		resultType:  defaultExpr.ResultType(),
	}, nil
}

func (c *CaseFunction) evalIntCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalInt(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalInt(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && testVal == val {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) evalFloatCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalFloat(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalFloat(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && testVal == val {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) evalBoolCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalBool(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalBool(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && testVal == val {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) evalDecimalCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalDecimal(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalDecimal(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && testVal.Equals(&val) {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) evalStringCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalString(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalString(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && testVal == val {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) evalBytesCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalBytes(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalBytes(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && bytes.Equal(testVal, val) {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) evalTimestampCase(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	testVal, null, err := c.testExpr.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	lc := len(c.caseExprs)
	for i := 0; i < lc; i++ {
		caseExpr := c.caseExprs[i]
		val, null, err := caseExpr.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if !null && testVal.Val == val.Val {
			return i, false, nil
		}
	}
	return -1, false, nil
}

func (c *CaseFunction) getMatchingIndex(rowIndex int, batch *evbatch.Batch) (int, bool, error) {
	var matchingIndex int
	var null bool
	var err error
	switch c.testExpr.ResultType().ID() {
	case types.ColumnTypeIDInt:
		matchingIndex, null, err = c.evalIntCase(rowIndex, batch)
	case types.ColumnTypeIDFloat:
		matchingIndex, null, err = c.evalFloatCase(rowIndex, batch)
	case types.ColumnTypeIDBool:
		matchingIndex, null, err = c.evalBoolCase(rowIndex, batch)
	case types.ColumnTypeIDDecimal:
		matchingIndex, null, err = c.evalDecimalCase(rowIndex, batch)
	case types.ColumnTypeIDString:
		matchingIndex, null, err = c.evalStringCase(rowIndex, batch)
	case types.ColumnTypeIDBytes:
		matchingIndex, null, err = c.evalBytesCase(rowIndex, batch)
	case types.ColumnTypeIDTimestamp:
		matchingIndex, null, err = c.evalTimestampCase(rowIndex, batch)
	default:
		panic("unknown type")
	}
	return matchingIndex, null, err
}

func (c *CaseFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalInt(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalInt(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalFloat(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalFloat(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalBool(rowIndex, batch)
		if err != nil {
			return false, false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalBool(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) EvalDecimal(rowIndex int, batch *evbatch.Batch) (types.Decimal, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalDecimal(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalDecimal(rowIndex, batch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalString(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalBytes(rowIndex, batch)
		if err != nil {
			return nil, false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalBytes(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) EvalTimestamp(rowIndex int, batch *evbatch.Batch) (types.Timestamp, bool, error) {
	matchingIndex, null, err := c.getMatchingIndex(rowIndex, batch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, nil
	}
	if matchingIndex == -1 {
		defaultVal, null, err := c.defaultExpr.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return types.Timestamp{}, false, err
		}
		return defaultVal, null, nil
	}
	retExpr := c.retExprs[matchingIndex]
	retVal, null, err := retExpr.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	return retVal, null, nil
}

func (c *CaseFunction) ResultType() types.ColumnType {
	return c.resultType
}

// Decimal funcs

type DecimalShiftFunction struct {
	baseExpr
	operandExpr Expression
	placesExpr  Expression
	roundExpr   Expression
	resultType  types.ColumnType
}

func NewDecimalShiftFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*DecimalShiftFunction, error) {
	if len(argExprs) < 2 || len(argExprs) > 3 {
		return nil, desc.ErrorAtPosition("'decimal_shift' requires 2 or 3 arguments - %d found", len(argExprs))
	}
	operandExpr := argExprs[0]
	if operandExpr.ResultType().ID() != types.ColumnTypeIDDecimal {
		return nil, desc.ErrorAtPosition("'decimal_shift' first argument must be of type decimal - it is of type %s",
			operandExpr.ResultType().String())
	}
	placesExpr := argExprs[1]
	if placesExpr.ResultType().ID() != types.ColumnTypeIDInt {
		return nil, desc.ErrorAtPosition("'decimal_shift' second argument must be of type int - it is of type %s",
			placesExpr.ResultType().String())
	}
	var roundExpression Expression
	if len(argExprs) == 3 {
		roundExpression = argExprs[2]
		if roundExpression.ResultType().ID() != types.ColumnTypeIDBool {
			return nil, desc.ErrorAtPosition("'decimal_shift' third argument must be of type bool - it is of type %s",
				roundExpression.ResultType().String())
		}
	}
	return &DecimalShiftFunction{
		operandExpr: operandExpr,
		placesExpr:  placesExpr,
		roundExpr:   roundExpression,
		resultType:  operandExpr.ResultType(),
	}, nil
}

func (d *DecimalShiftFunction) EvalDecimal(rowIndex int, batch *evbatch.Batch) (shifted types.Decimal, null bool, err error) {

	val, null, err := d.operandExpr.EvalDecimal(rowIndex, batch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, nil
	}
	places, null, err := d.placesExpr.EvalInt(rowIndex, batch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, nil
	}
	round := true
	if d.roundExpr != nil {
		round, null, err = d.roundExpr.EvalBool(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
	}
	defer func() {
		// catch any panic that might occur, e.g. due to result being too large
		r := recover()
		if r == nil {
			return
		}
		err = errors.Errorf("failed to shift decimal %s by %d places: %v", val.String(), places, r)
	}()
	res := val.Shift(int(places), round)
	return res, false, nil
}

func (d *DecimalShiftFunction) ResultType() types.ColumnType {
	return d.resultType
}

// String functions

type LenFunction struct {
	baseExpr
	operandExpr Expression
	isString    bool
}

func NewLenFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*LenFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'len' requires 1 argument - %d found", len(argExprs))
	}
	operandExpr := argExprs[0]
	if operandExpr.ResultType() != types.ColumnTypeString && operandExpr.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'len' argument must be of type string or bytes - it is of type %s",
			operandExpr.ResultType().String())
	}
	return &LenFunction{
		operandExpr: operandExpr,
		isString:    operandExpr.ResultType() == types.ColumnTypeString,
	}, nil
}

func (d *LenFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	if d.isString {
		val, null, err := d.operandExpr.EvalString(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return int64(len(val)), null, err
	} else {
		val, null, err := d.operandExpr.EvalBytes(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return int64(len(val)), null, err
	}
}

func (d *LenFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type ConcatFunction struct {
	baseExpr
	operand1  Expression
	operand2  Expression
	isString  bool
	resulType types.ColumnType
}

func NewConcatFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ConcatFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'concat' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString && operand1.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'concat' first argument must be of type string or bytes - it is of type %s",
			operand1.ResultType().String())
	}
	if operand1.ResultType() != operand2.ResultType() {
		return nil, desc.ErrorAtPosition("'concat' arguments must be of same type - first is %s and second is %s",
			operand1.ResultType().String(), operand2.ResultType().String())
	}
	isString := operand1.ResultType() == types.ColumnTypeString
	return &ConcatFunction{
		operand1:  operand1,
		operand2:  operand2,
		isString:  isString,
		resulType: operand1.ResultType(),
	}, nil
}

func (d *ConcatFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	val1, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	val2, null, err := d.operand2.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return val1 + val2, false, nil
}

func (d *ConcatFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	val1, null, err := d.operand1.EvalBytes(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	val2, null, err := d.operand2.EvalBytes(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	lv1 := len(val1)
	res := make([]byte, lv1+len(val2))
	copy(res, val1)
	copy(res[lv1:], val2)
	return res, false, nil
}

func (d *ConcatFunction) ResultType() types.ColumnType {
	return d.resulType
}

type StartsWithFunction struct {
	baseExpr
	operand1 Expression
	operand2 Expression
}

func NewStartsWithFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*StartsWithFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'starts_with' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'starts_with' first argument must be of type string - it is of type %s",
			operand1.ResultType().String())
	}
	if operand2.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'starts_with' second argument must be of type string - it is of type %s",
			operand2.ResultType().String())
	}
	return &StartsWithFunction{
		operand1: operand1,
		operand2: operand2,
	}, nil
}

func (d *StartsWithFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	str, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	startsWith, null, err := d.operand2.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return strings.HasPrefix(str, startsWith), false, nil
}

func (d *StartsWithFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type EndsWithFunction struct {
	baseExpr
	operand1 Expression
	operand2 Expression
}

func NewEndsWithFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*EndsWithFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'ends_with' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'ends_with' first argument must be of type string - it is of type %s",
			operand1.ResultType().String())
	}
	if operand2.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'ends_with' second argument must be of type string - it is of type %s",
			operand2.ResultType().String())
	}
	return &EndsWithFunction{
		operand1: operand1,
		operand2: operand2,
	}, nil
}

func (d *EndsWithFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	str, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	endsWith, null, err := d.operand2.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return strings.HasSuffix(str, endsWith), false, nil
}

func (d *EndsWithFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type MatchesFunction struct {
	baseExpr
	operand1 Expression
	operand2 Expression
	re       *regexp.Regexp
}

func NewMatchesFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*MatchesFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'matches' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'matches' first argument must be of type string - it is of type %s",
			operand1.ResultType().String())
	}
	_, ok := operand2.(*StringConstantExpr)
	if !ok {
		return nil, desc.ErrorAtPosition("'matches' second argument must be a string literal")
	}
	reString, _, _ := operand2.EvalString(0, nil)
	regex, err := regexp.Compile(reString)
	if err != nil {
		return nil, desc.ArgExprs[1].ErrorAtPosition("invalid regex syntax")
	}
	return &MatchesFunction{
		operand1: operand1,
		operand2: operand2,
		re:       regex,
	}, nil
}

func (d *MatchesFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	str, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return d.re.MatchString(str), false, nil
}

func (d *MatchesFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type TrimFunction struct {
	baseExpr
	operand1 Expression
	operand2 Expression
}

func NewTrimFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*TrimFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'trim' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'trim' first argument must be of type string - it is of type %s",
			operand1.ResultType().String())
	}
	if operand1.ResultType() != operand2.ResultType() {
		return nil, desc.ErrorAtPosition("'trim' arguments must be of same type - first is %s and second is %s",
			operand1.ResultType().String(), operand2.ResultType().String())
	}
	return &TrimFunction{
		operand1: operand1,
		operand2: operand2,
	}, nil
}

func (d *TrimFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	operand1, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	operand2, null, err := d.operand2.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return strings.Trim(operand1, operand2), false, nil
}

func (d *TrimFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type LTrimFunction struct {
	baseExpr
	operand1 Expression
	operand2 Expression
}

func NewLTrimFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*LTrimFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'ltrim' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString && operand1.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'ltrim' first argument must be of type string - it is of type %s",
			operand1.ResultType().String())
	}
	if operand1.ResultType() != operand2.ResultType() {
		return nil, desc.ErrorAtPosition("'ltrim' arguments must be of same type - first is %s and second is %s",
			operand1.ResultType().String(), operand2.ResultType().String())
	}
	return &LTrimFunction{
		operand1: operand1,
		operand2: operand2,
	}, nil
}

func (d *LTrimFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	operand1, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	operand2, null, err := d.operand2.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return strings.TrimLeft(operand1, operand2), false, nil
}

func (d *LTrimFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type RTrimFunction struct {
	baseExpr
	operand1 Expression
	operand2 Expression
}

func NewRTrimFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*RTrimFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'rtrim' requires 2 arguments - %d found", len(argExprs))
	}
	operand1 := argExprs[0]
	operand2 := argExprs[1]
	if operand1.ResultType() != types.ColumnTypeString && operand1.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'rtrim' first argument must be of type string - it is of type %s",
			operand1.ResultType().String())
	}
	if operand1.ResultType() != operand2.ResultType() {
		return nil, desc.ErrorAtPosition("'rtrim' arguments must be of same type - first is %s and second is %s",
			operand1.ResultType().String(), operand2.ResultType().String())
	}
	return &RTrimFunction{
		operand1: operand1,
		operand2: operand2,
	}, nil
}

func (d *RTrimFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	operand1, null, err := d.operand1.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	operand2, null, err := d.operand2.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return strings.TrimRight(operand1, operand2), false, nil
}

func (d *RTrimFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type ToLowerFunction struct {
	baseExpr
	operand Expression
}

func NewToLowerFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToLowerFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_lower' requires 1 argument - %d found", len(argExprs))
	}
	operand := argExprs[0]
	if operand.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'to_lower' argument must be of type string - it is of type %s",
			operand.ResultType().String())
	}
	return &ToLowerFunction{
		operand: operand,
	}, nil
}

func (d *ToLowerFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	operand, null, err := d.operand.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return strings.ToLower(operand), false, nil
}

func (d *ToLowerFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type ToUpperFunction struct {
	baseExpr
	operand Expression
}

func NewToUpperFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToUpperFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_upper' requires 1 argument - %d found", len(argExprs))
	}
	operand := argExprs[0]
	if operand.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'to_upper' argument must be of type string - it is of type %s",
			operand.ResultType().String())
	}
	return &ToUpperFunction{
		operand: operand,
	}, nil
}

func (d *ToUpperFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	operand, null, err := d.operand.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return strings.ToUpper(operand), false, nil
}

func (d *ToUpperFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type SubStrFunction struct {
	baseExpr
	strOperand   Expression
	startOperand Expression
	endOperand   Expression
}

func NewSubStrFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*SubStrFunction, error) {
	if len(argExprs) != 3 {
		return nil, desc.ErrorAtPosition("'sub_str' requires 3 arguments - %d found", len(argExprs))
	}
	strOperand := argExprs[0]
	startOperand := argExprs[1]
	endOperand := argExprs[2]
	if strOperand.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'sub_str' first argument must be of type string - it is of type %s",
			strOperand.ResultType().String())
	}
	if startOperand.ResultType() != types.ColumnTypeInt {
		return nil, desc.ErrorAtPosition("'sub_str' second argument must be of type int - it is of type %s",
			startOperand.ResultType().String())
	}
	if endOperand.ResultType() != types.ColumnTypeInt {
		return nil, desc.ErrorAtPosition("'sub_str' third argument must be of type int - it is of type %s",
			endOperand.ResultType().String())
	}
	return &SubStrFunction{
		strOperand:   strOperand,
		startOperand: startOperand,
		endOperand:   endOperand,
	}, nil
}

func (d *SubStrFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	strOperand, null, err := d.strOperand.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	s, null, err := d.startOperand.EvalInt(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	start := int(s)
	e, null, err := d.endOperand.EvalInt(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	end := int(e)
	if start < 0 {
		start = 0
	}
	if end < 0 {
		return "", false, nil
	}
	ls := len(strOperand)
	if start > ls {
		return "", false, nil
	}
	if end > ls {
		end = ls
	}
	if start > end {
		return "", false, nil
	}
	return strOperand[start:end], false, nil
}

func (d *SubStrFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type ReplaceFunction struct {
	baseExpr
	strOperand Expression
	oldOperand Expression
	newOperand Expression
}

func NewReplaceFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ReplaceFunction, error) {
	if len(argExprs) != 3 {
		return nil, desc.ErrorAtPosition("'replace' requires 3 arguments - %d found", len(argExprs))
	}
	strOperand := argExprs[0]
	oldOperand := argExprs[1]
	newOperand := argExprs[2]
	if strOperand.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'replace' first argument must be of type string - it is of type %s",
			strOperand.ResultType().String())
	}
	if oldOperand.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'replace' second argument must be of type string - it is of type %s",
			oldOperand.ResultType().String())
	}
	if newOperand.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'replace' third argument must be of type string - it is of type %s",
			newOperand.ResultType().String())
	}
	return &ReplaceFunction{
		strOperand: strOperand,
		oldOperand: oldOperand,
		newOperand: newOperand,
	}, nil
}

func (d *ReplaceFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	str, null, err := d.strOperand.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	oldVal, null, err := d.oldOperand.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	newVal, null, err := d.newOperand.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return strings.ReplaceAll(str, oldVal, newVal), false, nil
}

func (d *ReplaceFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type SprintfFunction struct {
	baseExpr
	formatExpr Expression
	args       []Expression
}

func NewSprintfFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*SprintfFunction, error) {
	if len(argExprs) < 2 {
		return nil, desc.ErrorAtPosition("'sprintf' requires at least 2 arguments - %d found", len(argExprs))
	}
	formatExpr := argExprs[0]

	if formatExpr.ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'sprintf' first argument must be of type string - it is of type %s",
			formatExpr.ResultType().String())
	}
	return &SprintfFunction{
		formatExpr: formatExpr,
		args:       argExprs[1:],
	}, nil
}

func (d *SprintfFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	format, null, err := d.formatExpr.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	args := make([]any, len(d.args))
	for i, argExpr := range d.args {
		var val any
		var null bool
		var err error
		switch argExpr.ResultType().ID() {
		case types.ColumnTypeIDInt:
			val, null, err = argExpr.EvalInt(rowIndex, batch)
		case types.ColumnTypeIDFloat:
			val, null, err = argExpr.EvalFloat(rowIndex, batch)
		case types.ColumnTypeIDBool:
			val, null, err = argExpr.EvalBool(rowIndex, batch)
		case types.ColumnTypeIDDecimal:
			var vb types.Decimal
			vb, null, err = argExpr.EvalDecimal(rowIndex, batch)
			val = vb.String()
		case types.ColumnTypeIDString:
			val, null, err = argExpr.EvalString(rowIndex, batch)
		case types.ColumnTypeIDBytes:
			val, null, err = argExpr.EvalBytes(rowIndex, batch)
		case types.ColumnTypeIDTimestamp:
			var vt types.Timestamp
			vt, null, err = argExpr.EvalTimestamp(rowIndex, batch)
			val = vt.Val
		default:
			panic("unknown type")
		}
		if err != nil {
			return "", false, err
		}
		if null {
			val = nil
		}
		args[i] = val
	}
	return fmt.Sprintf(format, args...), false, nil
}

func (d *SprintfFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

// Type conversions

type ToIntFunction struct {
	baseExpr
	oper Expression
}

func NewToIntFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToIntFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_int' requires 1 argument")
	}
	oper := argExprs[0]
	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDTimestamp: {},
	}
	if !supportsTypeID(supportedTypes, oper.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("'to_int' has operand with unsupported type %s", oper.ResultType().String())
	}
	return &ToIntFunction{
		oper: oper,
	}, nil
}

func (d *ToIntFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	switch d.oper.ResultType().ID() {
	case types.ColumnTypeIDInt:
		val, null, err := d.oper.EvalInt(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return val, false, nil
	case types.ColumnTypeIDFloat:
		f, null, err := d.oper.EvalFloat(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return int64(f), false, nil
	case types.ColumnTypeIDDecimal:
		d, null, err := d.oper.EvalDecimal(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return d.ToInt64(), false, nil
	case types.ColumnTypeIDString:
		s, null, err := d.oper.EvalString(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		val, err := strconv.Atoi(s)
		if err != nil {
			// It might be a non integer - try converting from float then casting. This gives consistent behaviour with
			// other types e.g. to_int of a float where the float is rounded to the int value
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return 0, false, errors.Errorf("function 'to_int' - cannot convert %s to int", s)
			}
			return int64(f), false, nil
		}
		return int64(val), false, nil
	case types.ColumnTypeIDTimestamp:
		t, null, err := d.oper.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return t.Val, false, nil
	default:
		panic("unknown type")
	}
}

func (d *ToIntFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type ToFloatFunction struct {
	baseExpr
	oper Expression
}

func NewToFloatFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToFloatFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_float' requires 1 argument")
	}
	oper := argExprs[0]
	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDTimestamp: {},
	}
	if !supportsTypeID(supportedTypes, oper.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("'to_float' has operand with unsupported type %s", oper.ResultType().String())
	}
	return &ToFloatFunction{
		oper: oper,
	}, nil
}

func (d *ToFloatFunction) EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error) {
	switch d.oper.ResultType().ID() {
	case types.ColumnTypeIDInt:
		val, null, err := d.oper.EvalInt(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return float64(val), false, nil
	case types.ColumnTypeIDFloat:
		f, null, err := d.oper.EvalFloat(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return f, false, nil
	case types.ColumnTypeIDDecimal:
		d, null, err := d.oper.EvalDecimal(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return d.ToFloat64(), false, nil
	case types.ColumnTypeIDString:
		s, null, err := d.oper.EvalString(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false, errors.Errorf("function 'to_float' - cannot convert %s to float", s)
		}
		return val, false, nil
	case types.ColumnTypeIDTimestamp:
		t, null, err := d.oper.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return 0, false, err
		}
		if null {
			return 0, true, nil
		}
		return float64(t.Val), false, nil
	default:
		panic("unknown type")
	}
}

func (d *ToFloatFunction) ResultType() types.ColumnType {
	return types.ColumnTypeFloat
}

type ToStringFunction struct {
	baseExpr
	oper Expression
}

func NewToStringFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToStringFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_string' requires 1 argument")
	}
	return &ToStringFunction{
		oper: argExprs[0],
	}, nil
}

func (d *ToStringFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	switch d.oper.ResultType().ID() {
	case types.ColumnTypeIDInt:
		val, null, err := d.oper.EvalInt(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return strconv.Itoa(int(val)), false, nil
	case types.ColumnTypeIDFloat:
		f, null, err := d.oper.EvalFloat(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		s := strconv.FormatFloat(f, 'f', -1, 64)
		return s, false, nil
	case types.ColumnTypeIDBool:
		b, null, err := d.oper.EvalBool(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		var s string
		if b {
			s = "true"
		} else {
			s = "false"
		}
		return s, false, nil
	case types.ColumnTypeIDDecimal:
		d, null, err := d.oper.EvalDecimal(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return d.String(), false, nil
	case types.ColumnTypeIDString:
		s, null, err := d.oper.EvalString(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return s, false, nil
	case types.ColumnTypeIDBytes:
		b, null, err := d.oper.EvalBytes(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return string(b), false, nil
	case types.ColumnTypeIDTimestamp:
		t, null, err := d.oper.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return "", false, err
		}
		if null {
			return "", true, nil
		}
		return strconv.Itoa(int(t.Val)), false, nil
	default:
		panic("unknown type")
	}
}

func (d *ToStringFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type ToDecimalFunction struct {
	baseExpr
	operArg Expression
	prec    int
	scale   int
	resType types.ColumnType
}

func NewToDecimalFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToDecimalFunction, error) {
	if len(argExprs) != 3 {
		return nil, desc.ErrorAtPosition("'to_decimal' requires 3 arguments")
	}
	operArg := argExprs[0]
	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDTimestamp: {},
	}
	if !supportsTypeID(supportedTypes, operArg.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("'to_decimal' has operand with unsupported type %s", operArg.ResultType().String())
	}
	precCol, ok := argExprs[1].(*IntegerConstantExpr)
	if !ok {
		return nil, desc.ErrorAtPosition("'to_decimal' second argument must be an int constant")
	}
	prec := int(precCol.val)
	if prec < 1 || prec > 38 {
		return nil, desc.ErrorAtPosition("'to_decimal' precision must be between 1 and 38 inclusive")
	}
	scaleCol, ok := argExprs[2].(*IntegerConstantExpr)
	if !ok {
		return nil, desc.ErrorAtPosition("'to_decimal' third argument must be an int constant")
	}
	scale := int(scaleCol.val)
	if scale < 0 || scale > 38 {
		return nil, desc.ErrorAtPosition("'to_decimal' scale must be between 0 and 38 inclusive")
	}
	if scale > prec {
		return nil, desc.ErrorAtPosition("'to_decimal' scale cannot be greater than precision")
	}

	resType := &types.DecimalType{
		Precision: prec,
		Scale:     scale,
	}
	return &ToDecimalFunction{
		operArg: operArg,
		prec:    prec,
		scale:   scale,
		resType: resType,
	}, nil
}

func (d *ToDecimalFunction) EvalDecimal(rowIndex int, batch *evbatch.Batch) (types.Decimal, bool, error) {
	switch d.operArg.ResultType().ID() {
	case types.ColumnTypeIDInt:
		val, null, err := d.operArg.EvalInt(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		return types.NewDecimalFromInt64(val, d.prec, d.scale), false, nil
	case types.ColumnTypeIDFloat:
		f, null, err := d.operArg.EvalFloat(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		d, err := types.NewDecimalFromFloat64(f, d.prec, d.scale)
		if err != nil {
			return types.Decimal{}, false, err
		}
		return d, false, nil
	case types.ColumnTypeIDDecimal:
		dec, null, err := d.operArg.EvalDecimal(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		if d.prec == dec.Precision && d.scale == dec.Scale {
			return dec, false, nil
		}
		return dec.ConvertPrecisionAndScale(d.prec, d.scale), false, nil
	case types.ColumnTypeIDString:
		s, null, err := d.operArg.EvalString(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		d, err := types.NewDecimalFromString(s, d.prec, d.scale)
		if err != nil {
			return types.Decimal{}, false, errors.Errorf("cannot create decimal from string: '%s'", s)
		}
		return d, false, nil
	case types.ColumnTypeIDTimestamp:
		t, null, err := d.operArg.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return types.Decimal{}, false, err
		}
		if null {
			return types.Decimal{}, true, nil
		}
		return types.NewDecimalFromInt64(t.Val, d.prec, d.scale), false, nil
	default:
		panic("unknown type")
	}
}

func (d *ToDecimalFunction) ResultType() types.ColumnType {
	return d.resType
}

type ToBytesFunction struct {
	baseExpr
	operArg Expression
}

func NewToBytesFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToBytesFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_bytes' requires 1 argument")
	}
	operArg := argExprs[0]
	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDBytes:  {},
		types.ColumnTypeIDString: {},
	}
	if !supportsTypeID(supportedTypes, operArg.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("'to_bytes' has operand with unsupported type %s", operArg.ResultType().String())
	}
	return &ToBytesFunction{
		operArg: operArg,
	}, nil
}

func (d *ToBytesFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	switch d.operArg.ResultType().ID() {

	case types.ColumnTypeIDString:
		s, null, err := d.operArg.EvalString(rowIndex, batch)
		if err != nil {
			return nil, false, err
		}
		if null {
			return nil, true, nil
		}
		return []byte(s), false, nil
	case types.ColumnTypeIDBytes:
		b, null, err := d.operArg.EvalBytes(rowIndex, batch)
		if err != nil {
			return nil, false, err
		}
		if null {
			return nil, true, nil
		}
		return b, false, nil
	default:
		panic("unknown type")
	}
}

func (d *ToBytesFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBytes
}

type ToTimestampFunction struct {
	baseExpr
	operArg Expression
}

func NewToTimestampFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ToTimestampFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'to_timestamp' requires 1 argument")
	}
	operArg := argExprs[0]
	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDTimestamp: {},
	}
	if !supportsTypeID(supportedTypes, operArg.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("'to_timestamp' has operand with unsupported type %s", operArg.ResultType().String())
	}
	return &ToTimestampFunction{
		operArg: operArg,
	}, nil
}

func (d *ToTimestampFunction) EvalTimestamp(rowIndex int, batch *evbatch.Batch) (types.Timestamp, bool, error) {
	switch d.operArg.ResultType().ID() {

	case types.ColumnTypeIDInt:
		val, null, err := d.operArg.EvalInt(rowIndex, batch)
		if err != nil {
			return types.Timestamp{}, false, err
		}
		if null {
			return types.Timestamp{}, true, nil
		}
		return types.NewTimestamp(val), false, nil
	case types.ColumnTypeIDTimestamp:
		ts, null, err := d.operArg.EvalTimestamp(rowIndex, batch)
		if err != nil {
			return types.Timestamp{}, false, err
		}
		if null {
			return types.Timestamp{}, true, nil
		}
		return ts, false, nil
	default:
		panic("unknown type")
	}
}

func (d *ToTimestampFunction) ResultType() types.ColumnType {
	return types.ColumnTypeTimestamp
}

// Date functions

type FormatDateFunction struct {
	baseExpr
	tsArg     Expression
	formatArg Expression
}

func NewFormatDateFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*FormatDateFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'format_date' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'format_date' first argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	if argExprs[1].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'format_date' second argument must be of type string - it is %s",
			argExprs[1].ResultType().String())
	}
	return &FormatDateFunction{
		tsArg:     argExprs[0],
		formatArg: argExprs[1],
	}, nil
}

func (d *FormatDateFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	ts, null, err := d.tsArg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	format, null, err := d.formatArg.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	t := time.UnixMilli(ts.Val).UTC()
	return t.Format(format), false, nil
}

func (d *FormatDateFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type ParseDateFunction struct {
	baseExpr
	strArg    Expression
	formatArg Expression
}

func NewParseDateFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*ParseDateFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'parse_date' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'parse_date' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	if argExprs[1].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'parse_date' second argument must be of type string - it is %s",
			argExprs[1].ResultType().String())
	}
	return &ParseDateFunction{
		strArg:    argExprs[0],
		formatArg: argExprs[1],
	}, nil
}

func (d *ParseDateFunction) EvalTimestamp(rowIndex int, batch *evbatch.Batch) (types.Timestamp, bool, error) {
	s, null, err := d.strArg.EvalString(rowIndex, batch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, nil
	}
	format, null, err := d.formatArg.EvalString(rowIndex, batch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, nil
	}
	t, err := time.Parse(format, s)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	return types.NewTimestamp(t.UnixMilli()), false, nil
}

func (d *ParseDateFunction) ResultType() types.ColumnType {
	return types.ColumnTypeTimestamp
}

type YearFunction struct {
	baseExpr
	arg Expression
}

func NewYearFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*YearFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'year' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'year' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &YearFunction{
		arg: argExprs[0],
	}, nil
}

func (d *YearFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Year()), false, nil
}

func (d *YearFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type MonthFunction struct {
	baseExpr
	arg Expression
}

func NewMonthFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*MonthFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'month' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'month' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &MonthFunction{
		arg: argExprs[0],
	}, nil
}

func (d *MonthFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Month()), false, nil
}

func (d *MonthFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type DayFunction struct {
	baseExpr
	arg Expression
}

func NewDayFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*DayFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'day' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'day' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &DayFunction{
		arg: argExprs[0],
	}, nil
}

func (d *DayFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Day()), false, nil
}

func (d *DayFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type HourFunction struct {
	baseExpr
	arg Expression
}

func NewHourFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*HourFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'hour' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'hour' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &HourFunction{
		arg: argExprs[0],
	}, nil
}

func (d *HourFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Hour()), false, nil
}

func (d *HourFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type MinuteFunction struct {
	baseExpr
	arg Expression
}

func NewMinuteFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*MinuteFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'minute' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'minute' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &MinuteFunction{
		arg: argExprs[0],
	}, nil
}

func (d *MinuteFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Minute()), false, nil
}

func (d *MinuteFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type SecondFunction struct {
	baseExpr
	arg Expression
}

func NewSecondFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*SecondFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'second' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'second' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &SecondFunction{
		arg: argExprs[0],
	}, nil
}

func (d *SecondFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Second()), false, nil
}

func (d *SecondFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type MillisFunction struct {
	baseExpr
	arg Expression
}

func NewMillisFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*MillisFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'millis' requires 1 argument")
	}
	if argExprs[0].ResultType() != types.ColumnTypeTimestamp {
		return nil, desc.ErrorAtPosition("'millis' argument must be of type timestamp - it is %s",
			argExprs[0].ResultType().String())
	}
	return &MillisFunction{
		arg: argExprs[0],
	}, nil
}

func (d *MillisFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	ts, null, err := d.arg.EvalTimestamp(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return int64(time.UnixMilli(ts.Val).UTC().Nanosecond() / 1000000), false, nil
}

func (d *MillisFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type NowFunction struct {
	baseExpr
}

func NewNowFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*NowFunction, error) {
	if len(argExprs) > 0 {
		return nil, desc.ErrorAtPosition("'now' does not take any arguments")
	}
	return &NowFunction{}, nil
}

func (d *NowFunction) EvalTimestamp(_ int, _ *evbatch.Batch) (types.Timestamp, bool, error) {
	now := time.Now().UTC()
	return types.NewTimestamp(now.UnixMilli()), false, nil
}

func (d *NowFunction) ResultType() types.ColumnType {
	return types.ColumnTypeTimestamp
}

type JsonIntFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonIntFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonIntFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_int' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_int' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_int' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonIntFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonIntFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() || res.Type == gjson.Null {
		return 0, true, nil
	}
	return res.Int(), false, nil
}

func (d *JsonIntFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type JsonFloatFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonFloatFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonFloatFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_float' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_float' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_float' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonFloatFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonFloatFunction) EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() || res.Type == gjson.Null {
		return 0, true, nil
	}
	return res.Float(), false, nil
}

func (d *JsonFloatFunction) ResultType() types.ColumnType {
	return types.ColumnTypeFloat
}

type JsonBoolFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonBoolFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonBoolFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_bool' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_bool' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_bool' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonBoolFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonBoolFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() || res.Type == gjson.Null {
		return false, true, nil
	}
	return res.Bool(), false, nil
}

func (d *JsonBoolFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type JsonStringFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonStringFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonStringFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_string' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_string' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_string' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonStringFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonStringFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() || res.Type == gjson.Null {
		return "", true, nil
	}
	return res.String(), false, nil
}

func (d *JsonStringFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type JsonRawFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonRawFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonRawFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_raw' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_raw' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_raw' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonRawFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonRawFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() || res.Type == gjson.Null {
		return "", true, nil
	}
	return res.Raw, false, nil
}

func (d *JsonRawFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type JsonIsNullFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonIsNullFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonIsNullFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_is_null' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_is_null' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_is_null' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonIsNullFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonIsNullFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() {
		return false, true, nil
	}
	return res.Type == gjson.Null, false, nil
}

func (d *JsonIsNullFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type JsonTypeFunction struct {
	baseExpr
	pathArg Expression
	jsonArg Expression
	isBytes bool
}

func NewJsonTypeFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*JsonTypeFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'json_type' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'json_type' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	jsonArgType := argExprs[1].ResultType()
	var isBytes bool
	if jsonArgType == types.ColumnTypeString {
		isBytes = false
	} else if jsonArgType == types.ColumnTypeBytes {
		isBytes = true
	} else {
		return nil, desc.ErrorAtPosition("'json_type' second argument must be of type string or bytes - it is %s",
			argExprs[1].ResultType().String())
	}
	return &JsonTypeFunction{
		pathArg: argExprs[0],
		jsonArg: argExprs[1],
		isBytes: isBytes,
	}, nil
}

func (d *JsonTypeFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	path, null, err := d.pathArg.EvalString(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	var jsonBytes []byte
	if d.isBytes {
		jsonBytes, null, err = d.jsonArg.EvalBytes(rowIndex, batch)
	} else {
		var s string
		s, null, err = d.jsonArg.EvalString(rowIndex, batch)
		jsonBytes = []byte(s)
	}
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	res := gjson.GetBytes(jsonBytes, path)
	if !res.Exists() {
		// we return null if the path cannot be found
		return "", true, nil
	}
	var typeStr string
	switch res.Type {
	case gjson.Null:
		typeStr = "null"
	case gjson.True, gjson.False:
		typeStr = "bool"
	case gjson.String:
		typeStr = "string"
	case gjson.Number:
		typeStr = "number"
	case gjson.JSON:
		typeStr = "json"
	default:
		panic("unknown type")
	}
	return typeStr, false, nil
}

func (d *JsonTypeFunction) ResultType() types.ColumnType {
	return types.ColumnTypeString
}

type KafkaBuildHeadersFunction struct {
	baseExpr
	headerNameExprs []Expression
	headerValExprs  []Expression
	isBytes         bool
}

func NewKafkaBuildHeadersFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*KafkaBuildHeadersFunction, error) {
	if len(argExprs) < 2 {
		return nil, desc.ErrorAtPosition("'kafka_build_headers' requires at least 2 arguments")
	}
	if len(argExprs)%2 == 1 {
		return nil, desc.ErrorAtPosition("'kafka_build_headers' requires an even number of arguments")
	}
	var headerNameExprs []Expression
	var headerValExprs []Expression
	for i := 0; i < len(argExprs); i += 2 {
		headerNameExpr := argExprs[i]
		headerValExpr := argExprs[i+1]
		if headerNameExpr.ResultType() != types.ColumnTypeString {
			return nil, desc.ErrorAtPosition("'kafka_build_headers' header name expression must be of type string - expression at position %d is of type %s",
				i, headerNameExpr.ResultType().String())
		}
		if headerValExpr.ResultType() != types.ColumnTypeString && headerValExpr.ResultType() != types.ColumnTypeBytes {
			return nil, desc.ErrorAtPosition("'kafka_build_headers' header value expression must be of type string or bytes - expression at position %d is of type %s",
				i, headerValExpr.ResultType().String())
		}
		headerNameExprs = append(headerNameExprs, headerNameExpr)
		headerValExprs = append(headerValExprs, headerValExpr)
	}
	return &KafkaBuildHeadersFunction{
		headerNameExprs: headerNameExprs,
		headerValExprs:  headerValExprs,
	}, nil
}

func (d *KafkaBuildHeadersFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	lexpr := len(d.headerNameExprs)
	headerBytes := make([]byte, 0, 64)
	headerBytes = binary.AppendVarint(headerBytes, int64(lexpr))
	for i := 0; i < lexpr; i++ {
		headerNameExpr := d.headerNameExprs[i]
		headerValExpr := d.headerValExprs[i]
		headerName, null, err := headerNameExpr.EvalString(rowIndex, batch)
		if err != nil {
			return nil, false, err
		}
		if null {
			// skip it
			continue
		}
		var headerValBytes []byte
		if headerValExpr.ResultType() == types.ColumnTypeString {
			var sName string
			sName, null, err = headerValExpr.EvalString(rowIndex, batch)
			headerValBytes = []byte(sName)
		} else {
			headerValBytes, null, err = headerValExpr.EvalBytes(rowIndex, batch)
		}
		if err != nil {
			return nil, false, err
		}
		if null {
			headerValBytes = nil
		}
		headerBytes = binary.AppendVarint(headerBytes, int64(len(headerName)))
		headerBytes = append(headerBytes, []byte(headerName)...)
		headerBytes = binary.AppendVarint(headerBytes, int64(len(headerValBytes)))
		headerBytes = append(headerBytes, headerValBytes...)
	}
	return headerBytes, false, nil
}

func (d *KafkaBuildHeadersFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBytes
}

type KafkaHeaderFunction struct {
	baseExpr
	headerNameExpr Expression
	headersExpr    Expression
	isBytes        bool
}

func NewKafkaHeaderFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*KafkaHeaderFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'kafka_header' requires 2 arguments")
	}
	if argExprs[0].ResultType() != types.ColumnTypeString {
		return nil, desc.ErrorAtPosition("'kafka_header' first argument must be of type string - it is %s",
			argExprs[0].ResultType().String())
	}
	if argExprs[1].ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'kafka_header' second argument must be of type bytes - it is %s",
			argExprs[0].ResultType().String())
	}
	return &KafkaHeaderFunction{
		headerNameExpr: argExprs[0],
		headersExpr:    argExprs[1],
	}, nil
}

func (d *KafkaHeaderFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	headerName, null, err := d.headerNameExpr.EvalString(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, err
	}
	headers, null, err := d.headersExpr.EvalBytes(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, err
	}
	headerVal, err := kafkaGetHeader(headerName, headers)
	if err != nil {
		return nil, false, err
	}
	if headerVal == nil {
		return nil, true, nil
	}
	return headerVal, false, nil
}

func kafkaGetHeader(headerName string, kafkaHeaders []byte) ([]byte, error) {
	numHeaders, off := binary.Varint(kafkaHeaders)
	if off <= 0 {
		return nil, errors.Errorf("failed to decode uvarint from kafka headers: %d", off)
	}
	hl := len(headerName)
	in := int(numHeaders)
	headerNameBytes := common.StringToByteSliceZeroCopy(headerName)
	for i := 0; i < in; i++ {
		headerNameLen, n := binary.Varint(kafkaHeaders[off:])
		if n <= 0 {
			return nil, errors.Errorf("failed to decode uvarint from kafka headers: %d", n)
		}
		off += n
		hNameOff := off
		iHNameLen := int(headerNameLen)
		off += iHNameLen
		headerValLen, n := binary.Varint(kafkaHeaders[off:])
		if n <= 0 {
			return nil, errors.Errorf("failed to decode uvarint from kafka headers: %d", n)
		}
		off += n
		hValOff := off
		iHValLen := int(headerValLen)
		off += iHValLen
		if iHNameLen == hl {
			hNameBytes := kafkaHeaders[hNameOff : hNameOff+iHNameLen]
			if bytes.Equal(headerNameBytes, hNameBytes) {
				headerVal := kafkaHeaders[hValOff : hValOff+iHValLen]
				return headerVal, nil
			}
		}
	}
	return nil, nil
}

func (d *KafkaHeaderFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBytes
}

type BytesSliceFunction struct {
	baseExpr
	bytesOperand Expression
	startOperand Expression
	endOperand   Expression
}

func NewBytesSliceFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*BytesSliceFunction, error) {
	if len(argExprs) != 3 {
		return nil, desc.ErrorAtPosition("'bytes_slice' requires 3 arguments - %d found", len(argExprs))
	}
	strOperand := argExprs[0]
	startOperand := argExprs[1]
	endOperand := argExprs[2]
	if strOperand.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'bytes_slice' first argument must be of type bytes - it is of type %s",
			strOperand.ResultType().String())
	}
	if startOperand.ResultType() != types.ColumnTypeInt {
		return nil, desc.ErrorAtPosition("'bytes_slice' second argument must be of type int - it is of type %s",
			startOperand.ResultType().String())
	}
	if endOperand.ResultType() != types.ColumnTypeInt {
		return nil, desc.ErrorAtPosition("'bytes_slice' third argument must be of type int - it is of type %s",
			endOperand.ResultType().String())
	}
	return &BytesSliceFunction{
		bytesOperand: strOperand,
		startOperand: startOperand,
		endOperand:   endOperand,
	}, nil
}

func (d *BytesSliceFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	bytesVal, null, err := d.bytesOperand.EvalBytes(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	s, null, err := d.startOperand.EvalInt(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	start := int(s)
	e, null, err := d.endOperand.EvalInt(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	end := int(e)
	if start < 0 {
		start = 0
	}
	if end < 0 {
		return nil, false, nil
	}
	ls := len(bytesVal)
	if start > ls {
		return nil, false, nil
	}
	if end > ls {
		end = ls
	}
	if start > end {
		return nil, false, nil
	}
	return bytesVal[start:end], false, nil
}

func (d *BytesSliceFunction) ResultType() types.ColumnType {
	return types.ColumnTypeBytes
}

type Uint64BEFunction struct {
	baseExpr
	bytesOperand Expression
	posOperand   Expression
}

func NewUint64BEFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*Uint64BEFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'uint64_be' requires 2 arguments - %d found", len(argExprs))
	}
	bytesOperand := argExprs[0]
	posOperand := argExprs[1]
	if bytesOperand.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'uint64_be' first argument must be of type bytes - it is of type %s",
			bytesOperand.ResultType().String())
	}
	if posOperand.ResultType() != types.ColumnTypeInt {
		return nil, desc.ErrorAtPosition("'uint64_be' second argument must be of type int - it is of type %s",
			posOperand.ResultType().String())
	}
	return &Uint64BEFunction{
		bytesOperand: bytesOperand,
		posOperand:   posOperand,
	}, nil
}

func (d *Uint64BEFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	bytesVal, null, err := d.bytesOperand.EvalBytes(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	s, null, err := d.posOperand.EvalInt(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	pos := int(s)
	if pos < 0 || pos >= len(bytesVal) {
		// out of bounds - return null
		return 0, true, nil
	}
	u := binary.BigEndian.Uint64(bytesVal[pos:])
	// Note - this returns the uint64 value cast to an int64. any numbers > max int64 will basically be -ve int64 values
	return int64(u), false, nil
}

func (d *Uint64BEFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type Uint64LEFunction struct {
	baseExpr
	bytesOperand Expression
	posOperand   Expression
}

func NewUint64LEFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*Uint64LEFunction, error) {
	if len(argExprs) != 2 {
		return nil, desc.ErrorAtPosition("'uint64_le' requires 2 arguments - %d found", len(argExprs))
	}
	bytesOperand := argExprs[0]
	posOperand := argExprs[1]
	if bytesOperand.ResultType() != types.ColumnTypeBytes {
		return nil, desc.ErrorAtPosition("'uint64_le' first argument must be of type bytes - it is of type %s",
			bytesOperand.ResultType().String())
	}
	if posOperand.ResultType() != types.ColumnTypeInt {
		return nil, desc.ErrorAtPosition("'uint64_le' second argument must be of type int - it is of type %s",
			posOperand.ResultType().String())
	}
	return &Uint64LEFunction{
		bytesOperand: bytesOperand,
		posOperand:   posOperand,
	}, nil
}

func (d *Uint64LEFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	bytesVal, null, err := d.bytesOperand.EvalBytes(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	s, null, err := d.posOperand.EvalInt(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	pos := int(s)
	if pos < 0 || pos >= len(bytesVal) {
		// out of bounds - return null
		return 0, true, nil
	}
	u := binary.LittleEndian.Uint64(bytesVal[pos:])
	// Note - this returns the uint64 value cast to an int64. any numbers > max int64 will basically be -ve int64 values
	return int64(u), false, nil
}

func (d *Uint64LEFunction) ResultType() types.ColumnType {
	return types.ColumnTypeInt
}

type AbsFunction struct {
	baseExpr
	operandExpr Expression
}

func NewAbsFunction(argExprs []Expression, desc *parser.FunctionExprDesc) (*AbsFunction, error) {
	if len(argExprs) != 1 {
		return nil, desc.ErrorAtPosition("'abs' requires 1 argument - %d found", len(argExprs))
	}
	operandExpr := argExprs[0]
	if operandExpr.ResultType() != types.ColumnTypeInt && operandExpr.ResultType() != types.ColumnTypeFloat &&
		operandExpr.ResultType().ID() != types.ColumnTypeIDDecimal {
		return nil, desc.ErrorAtPosition("'abs' argument must be of type int, float or decimal - it is of type %s",
			operandExpr.ResultType().String())
	}
	return &AbsFunction{
		operandExpr: operandExpr,
	}, nil
}

func (d *AbsFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	val, null, err := d.operandExpr.EvalInt(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	if val < 0 {
		return -val, false, nil
	}
	return val, false, nil
}

func (d *AbsFunction) EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error) {
	val, null, err := d.operandExpr.EvalFloat(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	if val < 0 {
		return -val, false, nil
	}
	return val, false, nil
}

func (d *AbsFunction) EvalDecimal(rowIndex int, batch *evbatch.Batch) (types.Decimal, bool, error) {
	val, null, err := d.operandExpr.EvalDecimal(rowIndex, batch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, nil
	}
	return types.Decimal{
		Num:       val.Num.Abs(),
		Precision: val.Precision,
		Scale:     val.Scale,
	}, false, nil
}

func (d *AbsFunction) ResultType() types.ColumnType {
	return d.operandExpr.ResultType()
}
