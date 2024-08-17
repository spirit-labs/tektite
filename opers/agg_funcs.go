package opers

import (
	"bytes"
	"encoding/binary"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/types"
	"math"
	"strings"
)

type TektiteTypes interface {
	int64 | float64 | bool | types.Decimal | string | []byte | types.Timestamp
}

type AggFunc interface {
	ComputeInt(prevVal any, extraData []byte, vals []int64) (any, []byte, error)
	ComputeFloat(prevVal any, extraData []byte, vals []float64) (any, []byte, error)
	ComputeBool(prevVal any, extraData []byte, vals []bool) (any, []byte, error)
	ComputeDecimal(prevVal any, extraData []byte, vals []types.Decimal) (any, []byte, error)
	ComputeString(prevVal any, extraData []byte, vals []string) (any, []byte, error)
	ComputeBytes(prevVal any, extraData []byte, vals [][]byte) (any, []byte, error)
	ComputeTimestamp(prevVal any, extraData []byte, vals []types.Timestamp) (any, []byte, error)
	ReturnTypeForExpressionType(t types.ColumnType) types.ColumnType
	RequiresExtraData() bool
}

var aggFuncsMap = map[string]AggFunc{
	"sum":   saf,
	"count": caf,
	"min":   min,
	"max":   maxAgg,
	"avg":   avg,
}

var saf = &SumAggFunc{}
var caf = &CountAggFunc{}
var min = &MinAggFunc{}
var maxAgg = &MaxAggFunc{}
var avg = &AvgAggFunc{}

type AvgAggFunc struct {
	dummyAggFunc
}

func (a AvgAggFunc) ComputeInt(_ any, extraData []byte, vals []int64) (any, []byte, error) {
	valsTot := int64(0)
	for _, val := range vals {
		valsTot += val
	}
	return computeAvg(extraData, float64(valsTot), len(vals))
}

func (a AvgAggFunc) ComputeFloat(_ any, extraData []byte, vals []float64) (any, []byte, error) {
	valsTot := float64(0)
	for _, val := range vals {
		valsTot += val
	}
	return computeAvg(extraData, valsTot, len(vals))
}

func (a AvgAggFunc) ComputeTimestamp(_ any, extraData []byte, vals []types.Timestamp) (any, []byte, error) {
	valsTot := int64(0)
	for _, val := range vals {
		valsTot += val.Val
	}
	avg, extra, err := computeAvg(extraData, float64(valsTot), len(vals))
	return types.NewTimestamp(int64(avg)), extra, err
}

func (a AvgAggFunc) ComputeDecimal(_ any, extraData []byte, vals []types.Decimal) (any, []byte, error) {
	valsTot := types.Decimal{
		Num:       decimal128.New(0, 0),
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	for _, val := range vals {
		var err error
		valsTot, err = valsTot.Add(&val)
		if err != nil {
			return nil, nil, err
		}
	}
	fTot := valsTot.ToFloat64()
	fRes, extra, err := computeAvg(extraData, fTot, len(vals))
	if err != nil {
		return nil, nil, err
	}
	num, err := decimal128.FromFloat64(fRes, types.DefaultDecimalPrecision, types.DefaultDecimalScale)
	if err != nil {
		return nil, nil, err
	}
	res := types.Decimal{
		Num:       num,
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	return res, extra, nil
}

func computeAvg(extraData []byte, valsTot float64, valsCount int) (float64, []byte, error) {
	tot := float64(0)
	count := 0
	if extraData != nil {
		u := binary.LittleEndian.Uint64(extraData)
		tot = math.Float64frombits(u)
		count = int(binary.LittleEndian.Uint64(extraData[8:]))
	} else {
		extraData = make([]byte, 16)
	}
	tot += valsTot
	count += valsCount
	avg := tot / float64(count)
	binary.LittleEndian.PutUint64(extraData, math.Float64bits(tot))
	binary.LittleEndian.PutUint64(extraData[8:], uint64(count))
	return avg, extraData, nil
}

func (a AvgAggFunc) ReturnTypeForExpressionType(t types.ColumnType) types.ColumnType {
	if t.ID() == types.ColumnTypeIDTimestamp {
		return types.ColumnTypeTimestamp
	} else if t.ID() == types.ColumnTypeIDDecimal {
		return &types.DecimalType{
			Precision: types.DefaultDecimalPrecision,
			Scale:     types.DefaultDecimalScale,
		}
	}
	return types.ColumnTypeFloat
}

func (a AvgAggFunc) RequiresExtraData() bool {
	return true
}

type CountAggFunc struct {
}

func (c CountAggFunc) ComputeInt(p any, _ []byte, vals []int64) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ComputeFloat(p any, _ []byte, vals []float64) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ComputeBool(p any, _ []byte, vals []bool) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ComputeDecimal(p any, _ []byte, vals []types.Decimal) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ComputeString(p any, _ []byte, vals []string) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ComputeBytes(p any, _ []byte, vals [][]byte) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ComputeTimestamp(p any, _ []byte, vals []types.Timestamp) (any, []byte, error) {
	var prev int64
	if p != nil {
		prev = p.(int64)
	}
	return prev + int64(len(vals)), nil, nil
}

func (c CountAggFunc) ReturnTypeForExpressionType(types.ColumnType) types.ColumnType {
	return types.ColumnTypeInt
}

func (c CountAggFunc) RequiresExtraData() bool {
	return false
}

type SumAggFunc struct {
	dummyAggFunc
}

func (c SumAggFunc) ComputeInt(s any, _ []byte, vals []int64) (any, []byte, error) {
	var sum int64
	if s != nil {
		sum = s.(int64)
	}
	for _, val := range vals {
		sum += val
	}
	return sum, nil, nil
}

func (c SumAggFunc) ComputeFloat(s any, _ []byte, vals []float64) (any, []byte, error) {
	var sum float64
	if s != nil {
		sum = s.(float64)
	}
	for _, val := range vals {
		sum += val
	}
	return sum, nil, nil
}

func (c SumAggFunc) ComputeDecimal(s any, _ []byte, vals []types.Decimal) (any, []byte, error) {
	var sum types.Decimal
	if s != nil {
		sum = s.(types.Decimal)
	}
	for _, val := range vals {
		var err error
		sum, err = sum.Add(&val)
		if err != nil {
			return nil, nil, err
		}
	}
	return sum, nil, nil
}

func (c SumAggFunc) ReturnTypeForExpressionType(t types.ColumnType) types.ColumnType {
	return t
}

type MinAggFunc struct {
	dummyAggFunc
}

func (c MinAggFunc) ComputeInt(s any, _ []byte, vals []int64) (any, []byte, error) {
	var min int64
	if s != nil {
		min = s.(int64)
	} else {
		min = math.MaxInt64
	}
	for _, val := range vals {
		if val < min {
			min = val
		}
	}
	return min, nil, nil
}

func (c MinAggFunc) ComputeFloat(s any, _ []byte, vals []float64) (any, []byte, error) {
	var min float64
	if s != nil {
		min = s.(float64)
	} else {
		min = math.MaxFloat64
	}
	for _, val := range vals {
		if val < min {
			min = val
		}
	}
	return min, nil, nil
}

func (c MinAggFunc) ComputeDecimal(s any, _ []byte, vals []types.Decimal) (any, []byte, error) {
	var min types.Decimal
	first := false
	if s != nil {
		min = s.(types.Decimal)
	} else {
		first = true
	}
	for _, val := range vals {
		if first || val.Num.Less(min.Num) {
			min = val
		}
		first = false
	}
	return min, nil, nil
}

func (c MinAggFunc) ComputeString(s any, _ []byte, vals []string) (any, []byte, error) {
	var min string
	first := false
	if s != nil {
		min = s.(string)
	} else {
		first = true
	}
	for _, val := range vals {
		if first || strings.Compare(val, min) < 0 {
			min = val
		}
		first = false
	}
	return min, nil, nil
}

func (c MinAggFunc) ComputeTimestamp(s any, _ []byte, vals []types.Timestamp) (any, []byte, error) {
	var min types.Timestamp
	if s != nil {
		min = s.(types.Timestamp)
	} else {
		min = types.NewTimestamp(math.MaxInt64)
	}
	for _, val := range vals {
		if val.Val < min.Val {
			min = val
		}
	}
	return min, nil, nil
}

func (c MinAggFunc) ComputeBytes(s any, _ []byte, vals [][]byte) (any, []byte, error) {
	var min []byte
	first := false
	if s != nil {
		min = s.([]byte)
	} else {
		first = true
	}
	for _, val := range vals {
		if first || bytes.Compare(val, min) < 0 {
			min = val
		}
		first = false
	}
	return min, nil, nil
}

func (c MinAggFunc) ReturnTypeForExpressionType(t types.ColumnType) types.ColumnType {
	return t
}

//

type MaxAggFunc struct {
	dummyAggFunc
}

func (c MaxAggFunc) ComputeInt(s any, _ []byte, vals []int64) (any, []byte, error) {
	var maxAggVal int64
	if s != nil {
		maxAggVal = s.(int64)
	} else {
		maxAggVal = math.MinInt64
	}
	for _, val := range vals {
		if val > maxAggVal {
			maxAggVal = val
		}
	}
	return maxAggVal, nil, nil
}

func (c MaxAggFunc) ComputeFloat(s any, _ []byte, vals []float64) (any, []byte, error) {
	var maxAggVal float64
	if s != nil {
		maxAggVal = s.(float64)
	} else {
		maxAggVal = -math.MaxFloat64
	}
	for _, val := range vals {
		if val > maxAggVal {
			maxAggVal = val
		}
	}
	return maxAggVal, nil, nil
}

func (c MaxAggFunc) ComputeDecimal(s any, _ []byte, vals []types.Decimal) (any, []byte, error) {
	var maxAggVal types.Decimal
	first := false
	if s != nil {
		maxAggVal = s.(types.Decimal)
	} else {
		first = true
	}
	for _, val := range vals {
		if first || val.Num.Greater(maxAggVal.Num) {
			maxAggVal = val
		}
		first = false
	}
	return maxAggVal, nil, nil
}

func (c MaxAggFunc) ComputeString(s any, _ []byte, vals []string) (any, []byte, error) {
	var maxAggVal string
	if s != nil {
		maxAggVal = s.(string)
	}
	for _, val := range vals {
		if strings.Compare(val, maxAggVal) > 0 {
			maxAggVal = val
		}
	}
	return maxAggVal, nil, nil
}

func (c MaxAggFunc) ComputeTimestamp(s any, _ []byte, vals []types.Timestamp) (any, []byte, error) {
	var maxAggVal types.Timestamp
	if s != nil {
		maxAggVal = s.(types.Timestamp)
	} else {
		maxAggVal = types.NewTimestamp(math.MinInt64)
	}
	for _, val := range vals {
		if val.Val > maxAggVal.Val {
			maxAggVal = val
		}
	}
	return maxAggVal, nil, nil
}

func (c MaxAggFunc) ComputeBytes(s any, _ []byte, vals [][]byte) (any, []byte, error) {
	var maxAggVal []byte
	if s != nil {
		maxAggVal = s.([]byte)
	}
	for _, val := range vals {
		if bytes.Compare(val, maxAggVal) > 0 {
			maxAggVal = val
		}
	}
	return maxAggVal, nil, nil
}

func (c MaxAggFunc) ReturnTypeForExpressionType(t types.ColumnType) types.ColumnType {
	return t
}

type dummyAggFunc struct {
}

func (d *dummyAggFunc) ComputeInt(_ any, _ []byte, _ []int64) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) ComputeFloat(_ any, _ []byte, _ []float64) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) ComputeBool(_ any, _ []byte, _ []bool) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) ComputeDecimal(_ any, _ []byte, _ []types.Decimal) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) ComputeString(_ any, _ []byte, _ []string) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) ComputeBytes(_ any, _ []byte, _ [][]byte) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) ComputeTimestamp(_ any, _ []byte, _ []types.Timestamp) (any, []byte, error) {
	panic("not implemented for this agg func")
}

func (d *dummyAggFunc) RequiresExtraData() bool {
	return false
}
