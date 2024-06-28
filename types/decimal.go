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

package types

import (
	"fmt"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/errors"
	"math/big"
)

const (
	DefaultDecimalPrecision = 38
	DefaultDecimalScale     = 6
)

type Decimal struct {
	Num       decimal128.Num
	Precision int
	Scale     int
}

func NewDecimalFromInt64(val int64, precision int, scale int) Decimal {
	decNum := decimal128.FromI64(val)
	if scale > 0 {
		decNum = decNum.IncreaseScaleBy(int32(scale))
	} else if scale < 0 {
		decNum = decNum.ReduceScaleBy(-int32(scale), true)
	}
	return Decimal{
		Num:       decNum,
		Precision: precision,
		Scale:     scale,
	}
}

func NewDecimalFromFloat64(val float64, precision int, scale int) (Decimal, error) {
	decNum, err := decimal128.FromFloat64(val, int32(precision), int32(scale))
	if err != nil {
		return Decimal{}, err
	}
	return Decimal{
		Num:       decNum,
		Precision: precision,
		Scale:     scale,
	}, nil
}

func NewDecimalFromString(val string, precision int, scale int) (Decimal, error) {
	decNum, err := decimal128.FromString(val, int32(precision), int32(scale))
	if err != nil {
		return Decimal{}, err
	}
	return Decimal{
		Num:       decNum,
		Precision: precision,
		Scale:     scale,
	}, nil
}

func (d *Decimal) ConvertPrecisionAndScale(prec int, scale int) Decimal {
	scaleDiff := scale - d.Scale
	if scaleDiff > 0 {
		num := d.Num.IncreaseScaleBy(int32(scaleDiff))
		return Decimal{
			Num:       num,
			Precision: prec,
			Scale:     scale,
		}
	} else if scaleDiff < 0 {
		num := d.Num.ReduceScaleBy(-int32(scaleDiff), true)
		return Decimal{
			Num:       num,
			Precision: prec,
			Scale:     scale,
		}
	} else {
		return Decimal{
			Num:       d.Num,
			Precision: prec,
			Scale:     scale,
		}
	}
}

func (d *Decimal) GreaterThan(d2 *Decimal) bool {
	if d.Precision == d2.Precision && d.Scale == d2.Scale {
		return d.Num.Greater(d2.Num)
	}
	if d.Scale > d2.Scale {
		adjustedNum := d2.Num.IncreaseScaleBy(int32(d.Scale - d2.Scale))
		return d.Num.Greater(adjustedNum)
	} else {
		adjustedNum := d.Num.IncreaseScaleBy(int32(d2.Scale - d.Scale))
		return adjustedNum.Greater(d2.Num)
	}
}

func (d *Decimal) LessThan(d2 *Decimal) bool {
	if d.Precision == d2.Precision && d.Scale == d2.Scale {
		return d.Num.Less(d2.Num)
	}
	if d.Scale > d2.Scale {
		adjustedNum := d2.Num.IncreaseScaleBy(int32(d.Scale - d2.Scale))
		return d.Num.Less(adjustedNum)
	} else {
		adjustedNum := d.Num.IncreaseScaleBy(int32(d2.Scale - d.Scale))
		return adjustedNum.Less(d2.Num)
	}
}

func (d *Decimal) GreaterOrEquals(d2 *Decimal) bool {
	return !d.LessThan(d2)
}

func (d *Decimal) LessOrEquals(d2 *Decimal) bool {
	return !d.GreaterThan(d2)
}

func (d *Decimal) Equals(d2 *Decimal) bool {
	if d.Precision == d2.Precision && d.Scale == d2.Scale {
		return d.Num == d2.Num
	}
	if d.Scale > d2.Scale {
		adjustedNum := d2.Num.IncreaseScaleBy(int32(d.Scale - d2.Scale))
		return d.Num == adjustedNum
	} else {
		adjustedNum := d.Num.IncreaseScaleBy(int32(d2.Scale - d.Scale))
		return d2.Num == adjustedNum
	}
}

func (d *Decimal) Add(d2 *Decimal) (Decimal, error) {
	prec, scale := AddResultPrecScale(d.Precision, d.Scale, d2.Precision, d2.Scale)
	var n decimal128.Num
	if d.Scale == d2.Scale {
		n = d.Num.Add(d2.Num)
	} else if d2.Scale > d.Scale {
		adjustedNum := d.Num.IncreaseScaleBy(int32(d2.Scale - d.Scale))
		n = d2.Num.Add(adjustedNum)
	} else {
		adjustedNum := d2.Num.IncreaseScaleBy(int32(d.Scale - d2.Scale))
		n = d.Num.Add(adjustedNum)
	}
	ret := Decimal{
		Num:       n,
		Precision: prec,
		Scale:     scale,
	}
	if err := checkResultFits(ret.Num, prec); err != nil {
		return Decimal{}, err
	}
	return ret, nil
}

func (d *Decimal) Subtract(d2 *Decimal) (Decimal, error) {
	prec, scale := AddResultPrecScale(d.Precision, d.Scale, d2.Precision, d2.Scale)
	var n decimal128.Num
	if d2.Scale == d.Scale {
		n = d.Num.Sub(d2.Num)
	} else if d2.Scale > d.Scale {
		adjustedNum := d.Num.IncreaseScaleBy(int32(d2.Scale - d.Scale))
		n = adjustedNum.Sub(d2.Num)
	} else {
		adjustedNum := d2.Num.IncreaseScaleBy(int32(d.Scale - d2.Scale))
		n = d.Num.Sub(adjustedNum)
	}
	ret := Decimal{
		Num:       n,
		Precision: prec,
		Scale:     scale,
	}
	if err := checkResultFits(ret.Num, prec); err != nil {
		return Decimal{}, err
	}
	return ret, nil
}

func AddResultPrecScale(prec1 int, scale1 int, prec2 int, scale2 int) (int, int) {
	maxPrec := prec1
	if prec2 > prec1 {
		maxPrec = prec2
	}
	maxScale := scale1
	if scale2 > maxScale {
		maxScale = scale2
	}
	return maxPrec, maxScale
}

func MultiplyResultPrecScale(prec1 int, scale1 int, prec2 int, scale2 int) (int, int) {
	maxPrec := prec1
	if prec2 > prec1 {
		maxPrec = prec2
	}
	newScale := scale1 + scale2
	return maxPrec, newScale
}

func (d *Decimal) Multiply(d2 *Decimal) (Decimal, error) {
	n := d.Num.Mul(d2.Num)
	prec, scale := MultiplyResultPrecScale(d.Precision, d.Scale, d2.Precision, d2.Scale)
	if err := checkResultFits(n, prec); err != nil {
		return Decimal{}, err
	}
	return Decimal{
		Num:       n,
		Precision: prec,
		Scale:     scale,
	}, nil
}

func (d *Decimal) Divide(d2 *Decimal) (Decimal, error) {
	num1 := d.Num.IncreaseScaleBy(int32(d2.Scale))
	b1 := num1.BigInt()
	out, _ := b1.QuoRem(b1, d2.Num.BigInt(), &big.Int{})
	numRes := decimal128.FromBigInt(out)
	return Decimal{
		Num:       numRes,
		Precision: d.Precision,
		Scale:     d.Scale,
	}, nil
}

func (d *Decimal) Shift(places int, round bool) Decimal {
	if places > 0 {
		return Decimal{
			Num:       d.Num.IncreaseScaleBy(int32(places)),
			Precision: d.Precision,
			Scale:     d.Scale,
		}
	} else {
		return Decimal{
			Num:       d.Num.ReduceScaleBy(int32(-places), round),
			Precision: d.Precision,
			Scale:     d.Scale,
		}
	}
}

func (d *Decimal) ToFloat64() float64 {
	return d.Num.ToFloat64(int32(d.Scale))
}

func (d *Decimal) ToInt64() int64 {
	if d.Scale > 0 {
		res := &big.Int{}
		mult := decimal128.GetScaleMultiplier(d.Scale)
		bi := res.Div(d.Num.BigInt(), mult.BigInt())
		return bi.Int64()
	} else if d.Scale < 0 {
		res := &big.Int{}
		mult := decimal128.GetScaleMultiplier(-d.Scale)
		bi := res.Mul(d.Num.BigInt(), mult.BigInt())
		return bi.Int64()
	} else {
		return d.Num.BigInt().Int64()
	}
}

func (d *Decimal) String() string {
	return d.Num.ToString(int32(d.Scale))
}

func checkResultFits(n decimal128.Num, prec int) error {
	if !n.FitsInPrecision(int32(prec)) {
		return errors.New(fmt.Sprintf("result of decimal arithmetic does not fit in precision %d", prec))
	}
	return nil
}
