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
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
	"strings"
)

type AddOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
	exprType  types.ColumnType
}

func NewAddOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*AddOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())

	}
	if !supportsTypeID(supportedTypes, right.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has right operand with unsupported type '%s'", desc.Op,
			right.ResultType().String())
	}
	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &AddOperator{
		leftExpr:  left,
		rightExpr: right,
		exprType:  getAddSubtractResultType(left, right),
	}
	return oper, nil
}

func getAddSubtractResultType(left Expression, right Expression) types.ColumnType {
	lrt := left.ResultType()
	rrt := right.ResultType()
	var rt types.ColumnType
	decType1, isDecimal := lrt.(*types.DecimalType)
	if isDecimal {
		decType2, isDecimal := rrt.(*types.DecimalType)
		if !isDecimal {
			panic("rhs not decimal")
		}
		prec, scale := types.AddResultPrecScale(decType1.Precision, decType1.Scale, decType2.Precision, decType2.Scale)
		rt = &types.DecimalType{
			Precision: prec,
			Scale:     scale,
		}
	} else {
		rt = lrt
	}
	return rt
}

func (a *AddOperator) EvalInt(rowIndex int, inBatch *evbatch.Batch) (int64, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	return left + right, false, nil
}

func (a *AddOperator) EvalFloat(rowIndex int, inBatch *evbatch.Batch) (float64, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	return left + right, false, nil
}

func (a *AddOperator) EvalDecimal(rowIndex int, inBatch *evbatch.Batch) (types.Decimal, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	res, err := left.Add(&right)
	if err != nil {
		return types.Decimal{}, false, err
	}
	return res, false, nil
}

func (a *AddOperator) EvalTimestamp(rowIndex int, inBatch *evbatch.Batch) (types.Timestamp, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	return types.NewTimestamp(left.Val + right.Val), false, nil
}

func (a *AddOperator) ResultType() types.ColumnType {
	return a.exprType
}

type SubtractOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
	exprType  types.ColumnType
}

func NewSubtractOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*SubtractOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())

	}
	if !supportsTypeID(supportedTypes, right.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has right operand with unsupported type '%s'", desc.Op,
			right.ResultType().String())
	}
	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &SubtractOperator{
		leftExpr:  left,
		rightExpr: right,
		exprType:  getAddSubtractResultType(left, right),
	}
	return oper, nil
}

func (s *SubtractOperator) EvalInt(rowIndex int, inBatch *evbatch.Batch) (int64, bool, error) {
	left, null, err := s.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := s.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	return left - right, false, nil
}

func (s *SubtractOperator) EvalFloat(rowIndex int, inBatch *evbatch.Batch) (float64, bool, error) {
	left, null, err := s.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := s.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	return left - right, false, nil
}

func (s *SubtractOperator) EvalDecimal(rowIndex int, inBatch *evbatch.Batch) (types.Decimal, bool, error) {
	left, null, err := s.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	right, null, err := s.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	res, err := left.Subtract(&right)
	if err != nil {
		return types.Decimal{}, false, err
	}
	return res, false, nil
}

func (s *SubtractOperator) EvalTimestamp(rowIndex int, inBatch *evbatch.Batch) (types.Timestamp, bool, error) {
	left, null, err := s.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	right, null, err := s.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	return types.NewTimestamp(left.Val - right.Val), false, nil
}

func (s *SubtractOperator) ResultType() types.ColumnType {
	return s.exprType
}

type MultiplyOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
	exprType  types.ColumnType
}

func NewMultiplyOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*MultiplyOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}
	if !supportsTypeID(supportedTypes, right.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has right operand with unsupported type '%s'", desc.Op,
			right.ResultType().String())
	}
	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	lrt := left.ResultType()
	rrt := right.ResultType()
	var rt types.ColumnType
	decType1, isDecimal := lrt.(*types.DecimalType)
	if isDecimal {
		decType2, isDecimal := rrt.(*types.DecimalType)
		if !isDecimal {
			panic("rhs not decimal")
		}
		prec, scale := types.MultiplyResultPrecScale(decType1.Precision, decType1.Scale, decType2.Precision, decType2.Scale)
		rt = &types.DecimalType{
			Precision: prec,
			Scale:     scale,
		}
	} else {
		rt = lrt
	}

	oper := &MultiplyOperator{
		leftExpr:  left,
		rightExpr: right,
		exprType:  rt,
	}
	return oper, nil
}

func (m *MultiplyOperator) EvalInt(rowIndex int, inBatch *evbatch.Batch) (int64, bool, error) {
	left, null, err := m.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := m.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	return left * right, false, nil
}

func (m *MultiplyOperator) EvalFloat(rowIndex int, inBatch *evbatch.Batch) (float64, bool, error) {
	left, null, err := m.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := m.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	return left * right, false, nil
}

func (m *MultiplyOperator) EvalDecimal(rowIndex int, inBatch *evbatch.Batch) (types.Decimal, bool, error) {
	left, null, err := m.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	right, null, err := m.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	res, err := left.Multiply(&right)
	if err != nil {
		return types.Decimal{}, false, err
	}
	return res, false, nil
}

func (m *MultiplyOperator) EvalTimestamp(rowIndex int, inBatch *evbatch.Batch) (types.Timestamp, bool, error) {
	left, null, err := m.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	right, null, err := m.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	return types.NewTimestamp(left.Val * right.Val), false, nil
}

func (m *MultiplyOperator) ResultType() types.ColumnType {
	return m.exprType
}

type DivideOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
	exprType  types.ColumnType
	desc      *parser.BinaryOperatorExprDesc
}

func NewDivideOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*DivideOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())

	}
	if !supportsTypeID(supportedTypes, right.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has right operand with unsupported type '%s'", desc.Op,
			right.ResultType().String())
	}
	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	lrt := left.ResultType()
	rrt := right.ResultType()
	var rt types.ColumnType
	decType1, isDecimal := lrt.(*types.DecimalType)
	if isDecimal {
		_, isDecimal := rrt.(*types.DecimalType)
		if !isDecimal {
			panic("rhs not decimal")
		}
		rt = &types.DecimalType{
			Precision: decType1.Precision,
			Scale:     decType1.Scale,
		}
	} else {
		rt = lrt
	}
	oper := &DivideOperator{
		leftExpr:  left,
		rightExpr: right,
		exprType:  rt,
		desc:      desc,
	}
	return oper, nil
}

func (d *DivideOperator) EvalInt(rowIndex int, inBatch *evbatch.Batch) (int64, bool, error) {
	left, null, err := d.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := d.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	if right == 0 {
		return 0, false, d.desc.ErrorAtPosition("divide by zero when evaluating expression")
	}
	return left / right, false, nil
}

func (d *DivideOperator) EvalFloat(rowIndex int, inBatch *evbatch.Batch) (float64, bool, error) {
	left, null, err := d.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := d.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	if right == 0 {
		return 0, false, d.desc.ErrorAtPosition("divide by zero when evaluating expression")
	}
	return left / right, false, nil
}

func (d *DivideOperator) EvalDecimal(rowIndex int, inBatch *evbatch.Batch) (types.Decimal, bool, error) {
	left, null, err := d.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	right, null, err := d.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, err
	}
	if right.Num.LowBits() == 0 && right.Num.HighBits() == 0 {
		return types.Decimal{}, false, d.desc.ErrorAtPosition("divide by zero when evaluating expression")
	}
	res, err := left.Divide(&right)
	if err != nil {
		return types.Decimal{}, false, err
	}
	return res, false, nil
}

func (d *DivideOperator) EvalTimestamp(rowIndex int, inBatch *evbatch.Batch) (types.Timestamp, bool, error) {
	left, null, err := d.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	right, null, err := d.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, err
	}
	if right.Val == 0 {
		return types.Timestamp{}, false, d.desc.ErrorAtPosition("divide by zero when evaluating expression")
	}
	return types.NewTimestamp(left.Val / right.Val), false, nil
}

func (d *DivideOperator) ResultType() types.ColumnType {
	return d.exprType
}

type ModulusOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
	exprType  types.ColumnType
	desc      *parser.BinaryOperatorExprDesc
}

func NewModulusOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*ModulusOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())

	}
	if !supportsTypeID(supportedTypes, right.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has right operand with unsupported type '%s'", desc.Op,
			right.ResultType().String())
	}
	if !resultTypesEqual(left.ResultType(), right.ResultType()) {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}
	oper := &ModulusOperator{
		leftExpr:  left,
		rightExpr: right,
		exprType:  left.ResultType(),
		desc:      desc,
	}
	return oper, nil
}

func (m *ModulusOperator) EvalInt(rowIndex int, inBatch *evbatch.Batch) (int64, bool, error) {
	left, null, err := m.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	right, null, err := m.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, err
	}
	if right == 0 {
		return 0, false, m.desc.ErrorAtPosition("modulus by zero when evaluating expression")
	}
	return left % right, false, nil
}

func (m *ModulusOperator) ResultType() types.ColumnType {
	return m.exprType
}

type LogicalNotOperator struct {
	baseExpr
	operand Expression
}

func NewLogicalNotOperator(operand Expression, desc *parser.UnaryOperatorExprDesc) (*LogicalNotOperator, error) {
	if operand.ResultType() != types.ColumnTypeBool {
		return nil, desc.ErrorAtPosition("operator '!' must have operand of type bool - found %s", operand.ResultType().String())
	}
	return &LogicalNotOperator{operand: operand}, nil
}

func (n *LogicalNotOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	val, null, err := n.operand.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	return !val, null, err
}

func (n *LogicalNotOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type EqualsOperator struct {
	baseExpr
	leftExpr    Expression
	rightExpr   Expression
	exprType    types.ColumnType
	InputTypeID types.ColumnTypeID
}

func NewEqualsOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*EqualsOperator, error) {

	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &EqualsOperator{
		leftExpr:    left,
		rightExpr:   right,
		exprType:    types.ColumnTypeBool,
		InputTypeID: left.ResultType().ID(),
	}
	return oper, nil
}

func (a *EqualsOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	switch a.InputTypeID {
	case types.ColumnTypeIDInt:
		return a.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		return a.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDBool:
		return a.evalFromBool(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		return a.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		return a.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		return a.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		return a.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unknown type")
	}
}

func (a *EqualsOperator) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left == right, false, nil
}

func (a *EqualsOperator) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left == right, false, nil
}

func (a *EqualsOperator) evalFromBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left == right, false, nil
}

func (a *EqualsOperator) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Equals(&right), false, nil
}

func (a *EqualsOperator) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left == right, false, nil
}

func (a *EqualsOperator) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return bytes.Equal(left, right), false, nil
}

func (a *EqualsOperator) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Val == right.Val, false, nil
}

func (a *EqualsOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type NotEqualsOperator struct {
	baseExpr
	leftExpr    Expression
	rightExpr   Expression
	exprType    types.ColumnType
	inputTypeID types.ColumnTypeID
}

func NewNotEqualsOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*NotEqualsOperator, error) {

	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &NotEqualsOperator{
		leftExpr:    left,
		rightExpr:   right,
		exprType:    types.ColumnTypeBool,
		inputTypeID: left.ResultType().ID(),
	}
	return oper, nil
}

func (a *NotEqualsOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	switch a.inputTypeID {
	case types.ColumnTypeIDInt:
		return a.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		return a.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDBool:
		return a.evalFromBool(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		return a.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		return a.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		return a.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		return a.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unknown type")
	}
}

func (a *NotEqualsOperator) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left != right, false, nil
}

func (a *NotEqualsOperator) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left != right, false, nil
}

func (a *NotEqualsOperator) evalFromBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left != right, false, nil
}

func (a *NotEqualsOperator) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return !left.Equals(&right), false, nil
}

func (a *NotEqualsOperator) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left != right, false, nil
}

func (a *NotEqualsOperator) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return !bytes.Equal(left, right), false, nil
}

func (a *NotEqualsOperator) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Val != right.Val, false, nil
}

func (a *NotEqualsOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type GreaterThanOperator struct {
	baseExpr
	leftExpr    Expression
	rightExpr   Expression
	exprType    types.ColumnType
	inputTypeID types.ColumnTypeID
}

func NewGreaterThanOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*GreaterThanOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDBytes:     {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}

	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &GreaterThanOperator{
		leftExpr:    left,
		rightExpr:   right,
		exprType:    types.ColumnTypeBool,
		inputTypeID: left.ResultType().ID(),
	}
	return oper, nil
}

func (a *GreaterThanOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	switch a.inputTypeID {
	case types.ColumnTypeIDInt:
		return a.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		return a.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		return a.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		return a.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		return a.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		return a.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unknown type")
	}
}

func (a *GreaterThanOperator) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left > right, false, nil
}

func (a *GreaterThanOperator) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left > right, false, nil
}

func (a *GreaterThanOperator) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.GreaterThan(&right), false, nil
}

func (a *GreaterThanOperator) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return strings.Compare(left, right) > 0, false, nil
}

func (a *GreaterThanOperator) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return bytes.Compare(left, right) > 0, false, nil
}

func (a *GreaterThanOperator) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Val > right.Val, false, nil
}

func (a *GreaterThanOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type GreaterOrEqualsOperator struct {
	baseExpr
	leftExpr    Expression
	rightExpr   Expression
	exprType    types.ColumnType
	inputTypeID types.ColumnTypeID
}

func NewGreaterOrEqualsOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*GreaterOrEqualsOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDBytes:     {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}

	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &GreaterOrEqualsOperator{
		leftExpr:    left,
		rightExpr:   right,
		exprType:    types.ColumnTypeBool,
		inputTypeID: left.ResultType().ID(),
	}
	return oper, nil
}

func (a *GreaterOrEqualsOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	switch a.inputTypeID {
	case types.ColumnTypeIDInt:
		return a.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		return a.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		return a.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		return a.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		return a.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		return a.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unknown type")
	}
}

func (a *GreaterOrEqualsOperator) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left >= right, false, nil
}

func (a *GreaterOrEqualsOperator) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left >= right, false, nil
}

func (a *GreaterOrEqualsOperator) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.GreaterOrEquals(&right), false, nil
}

func (a *GreaterOrEqualsOperator) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return strings.Compare(left, right) >= 0, false, nil
}

func (a *GreaterOrEqualsOperator) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return bytes.Compare(left, right) >= 0, false, nil
}

func (a *GreaterOrEqualsOperator) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Val >= right.Val, false, nil
}

func (a *GreaterOrEqualsOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type LessThanOperator struct {
	baseExpr
	leftExpr    Expression
	rightExpr   Expression
	exprType    types.ColumnType
	inputTypeID types.ColumnTypeID
}

func NewLessThanOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*LessThanOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDBytes:     {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}

	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &LessThanOperator{
		leftExpr:    left,
		rightExpr:   right,
		exprType:    types.ColumnTypeBool,
		inputTypeID: left.ResultType().ID(),
	}
	return oper, nil
}

func (a *LessThanOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	switch a.inputTypeID {
	case types.ColumnTypeIDInt:
		return a.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		return a.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		return a.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		return a.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		return a.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		return a.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unknown type")
	}
}

func (a *LessThanOperator) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left < right, false, nil
}

func (a *LessThanOperator) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left < right, false, nil
}

func (a *LessThanOperator) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.LessThan(&right), false, nil
}

func (a *LessThanOperator) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return strings.Compare(left, right) < 0, false, nil
}

func (a *LessThanOperator) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return bytes.Compare(left, right) < 0, false, nil
}

func (a *LessThanOperator) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Val < right.Val, false, nil
}

func (a *LessThanOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type LessOrEqualsOperator struct {
	baseExpr
	leftExpr    Expression
	rightExpr   Expression
	exprType    types.ColumnType
	inputTypeID types.ColumnTypeID
}

func NewLessOrEqualsOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*LessOrEqualsOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDInt:       {},
		types.ColumnTypeIDFloat:     {},
		types.ColumnTypeIDDecimal:   {},
		types.ColumnTypeIDString:    {},
		types.ColumnTypeIDBytes:     {},
		types.ColumnTypeIDTimestamp: {},
	}

	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}

	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &LessOrEqualsOperator{
		leftExpr:    left,
		rightExpr:   right,
		exprType:    types.ColumnTypeBool,
		inputTypeID: left.ResultType().ID(),
	}
	return oper, nil
}

func (a *LessOrEqualsOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	switch a.inputTypeID {
	case types.ColumnTypeIDInt:
		return a.evalFromInt(rowIndex, inBatch)
	case types.ColumnTypeIDFloat:
		return a.evalFromFloat(rowIndex, inBatch)
	case types.ColumnTypeIDDecimal:
		return a.evalFromDecimal(rowIndex, inBatch)
	case types.ColumnTypeIDString:
		return a.evalFromString(rowIndex, inBatch)
	case types.ColumnTypeIDBytes:
		return a.evalFromBytes(rowIndex, inBatch)
	case types.ColumnTypeIDTimestamp:
		return a.evalFromTimestamp(rowIndex, inBatch)
	default:
		panic("unknown type")
	}
}

func (a *LessOrEqualsOperator) evalFromInt(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalInt(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left <= right, false, nil
}

func (a *LessOrEqualsOperator) evalFromFloat(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalFloat(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left <= right, false, nil
}

func (a *LessOrEqualsOperator) evalFromDecimal(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalDecimal(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.LessOrEquals(&right), false, nil
}

func (a *LessOrEqualsOperator) evalFromString(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalString(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return strings.Compare(left, right) <= 0, false, nil
}

func (a *LessOrEqualsOperator) evalFromBytes(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBytes(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return bytes.Compare(left, right) <= 0, false, nil
}

func (a *LessOrEqualsOperator) evalFromTimestamp(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalTimestamp(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left.Val <= right.Val, false, nil
}

func (a *LessOrEqualsOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type LogicalAndOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
}

func NewLogicalAndOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*LogicalAndOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDBool: {},
	}
	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}
	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &LogicalAndOperator{
		leftExpr:  left,
		rightExpr: right,
	}
	return oper, nil
}

func (a *LogicalAndOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	right, null, err := a.rightExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return left && right, false, nil
}

func (a *LogicalAndOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

type LogicalOrOperator struct {
	baseExpr
	leftExpr  Expression
	rightExpr Expression
}

func NewLogicalOrOperator(left Expression, right Expression, desc *parser.BinaryOperatorExprDesc) (*LogicalOrOperator, error) {

	supportedTypes := map[types.ColumnTypeID]struct{}{
		types.ColumnTypeIDBool: {},
	}
	if !supportsTypeID(supportedTypes, left.ResultType().ID()) {
		return nil, desc.ErrorAtPosition("operator '%s' has left operand with unsupported type '%s'", desc.Op,
			left.ResultType().String())
	}
	if left.ResultType().ID() != right.ResultType().ID() {
		return nil, desc.ErrorAtPosition("operator '%s' left operand type '%s' and right operand type '%s' are not the same.", desc.Op,
			left.ResultType().String(), right.ResultType().String())
	}

	oper := &LogicalOrOperator{
		leftExpr:  left,
		rightExpr: right,
	}
	return oper, nil
}

func (a *LogicalOrOperator) EvalBool(rowIndex int, inBatch *evbatch.Batch) (bool, bool, error) {
	left, null, err := a.leftExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	if left {
		return true, false, nil
	}
	right, null, err := a.rightExpr.EvalBool(rowIndex, inBatch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return right, false, nil
}

func (a *LogicalOrOperator) ResultType() types.ColumnType {
	return types.ColumnTypeBool
}

func supportsTypeID(supportedTypes map[types.ColumnTypeID]struct{}, id types.ColumnTypeID) bool {
	_, ok := supportedTypes[id]
	return ok
}
