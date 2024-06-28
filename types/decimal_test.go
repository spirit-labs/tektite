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
	"math"
	"testing"

	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/stretchr/testify/require"
)

func TestDecimalComparison(t *testing.T) {
	type Want struct {
		Equal           bool
		LessThan        bool
		LessOrEquals    bool
		GreaterThan     bool
		GreaterOrEquals bool
	}
	tests := []struct {
		name string
		d1   Decimal
		d2   Decimal
		want Want
	}{
		{
			name: "SamePrecScale__(+A)<(+B)",
			d1:   Dec(123421, 10, 2),
			d2:   Dec(456456, 10, 2),
			want: Want{
				Equal:           false,
				LessThan:        true,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: false,
			},
		},
		{
			name: "SamePrecScale__(+A)>(+B)",
			d1:   Dec(456456, 10, 2),
			d2:   Dec(123421, 10, 2),
			want: Want{
				Equal:           false,
				LessThan:        false,
				LessOrEquals:    false,
				GreaterThan:     true,
				GreaterOrEquals: true,
			},
		},
		{
			name: "SamePrecScale__(+A)==(+B)",
			d1:   Dec(123421, 10, 2),
			d2:   Dec(123421, 10, 2),
			want: Want{
				Equal:           true,
				LessThan:        false,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: true,
			},
		},
		{
			name: "DifferentPrecScale__(+A)<(+B)",
			d1:   Dec(123421, 10, 2),
			d2:   Dec(22342100, 13, 4),
			want: Want{
				Equal:           false,
				LessThan:        true,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: false,
			},
		},
		{
			name: "DifferentPrecScale__(+A)>(+B)",
			d1:   Dec(123421, 10, 2),
			d2:   Dec(11342100, 13, 4),
			want: Want{
				Equal:           false,
				LessThan:        false,
				LessOrEquals:    false,
				GreaterThan:     true,
				GreaterOrEquals: true,
			},
		},
		{
			name: "DifferentPrecScale__(+A)==(+B)",
			d1:   Dec(123421, 10, 2),
			d2:   Dec(12342100, 13, 4),
			want: Want{
				Equal:           true,
				LessThan:        false,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: true,
			},
		},
		{
			name: "SamePrecScale__(-A)<(+B)",
			d1:   Dec(-456456, 10, 2),
			d2:   Dec(123421, 10, 2),
			want: Want{
				Equal:           false,
				LessThan:        true,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: false,
			},
		},
		{
			name: "SamePrecScale__(-A)<(-B)",
			d1:   Dec(-456456, 10, 2),
			d2:   Dec(-123421, 10, 2),
			want: Want{
				Equal:           false,
				LessThan:        true,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: false,
			},
		},
		{
			name: "SamePrecScale__(-A)==(-B)",
			d1:   Dec(-123421, 10, 2),
			d2:   Dec(-123421, 10, 2),
			want: Want{
				Equal:           true,
				LessThan:        false,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: true,
			},
		},
		{
			name: "DifferentPrecScale__(-A)<(+B)",
			d1:   Dec(-123421, 10, 2),
			d2:   Dec(22342100, 13, 4),
			want: Want{
				Equal:           false,
				LessThan:        true,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: false,
			},
		},
		{
			name: "DifferentPrecScale__(-A)>(-B)",
			d1:   Dec(-11342100, 13, 4),
			d2:   Dec(-123421, 10, 2),
			want: Want{
				Equal:           false,
				LessThan:        false,
				LessOrEquals:    false,
				GreaterThan:     true,
				GreaterOrEquals: true,
			},
		},
		{
			name: "DifferentPrecScale__(-A)==(-B)",
			d1:   Dec(-123421, 10, 2),
			d2:   Dec(-12342100, 13, 4),
			want: Want{
				Equal:           true,
				LessThan:        false,
				LessOrEquals:    true,
				GreaterThan:     false,
				GreaterOrEquals: true,
			},
		},

	}
	for _, tc := range tests {
		t.Run(
			tc.name, func(t *testing.T) {
				d1 := tc.d1
				d2 := tc.d2
				require.Equal(t, tc.want.Equal, d1.Equals(&d2), "%s == %s failed", d1.String(), d2.String())
				require.Equal(t, tc.want.Equal, d2.Equals(&d1), "%s == %s", d2.String(), d1.String())

				require.Equal(t, tc.want.LessThan, d1.LessThan(&d2), "%s < %s", d1.String(), d2.String())
				require.Equal(
					t, !tc.want.Equal && !tc.want.LessThan, d2.LessThan(&d1), "%s < %s", d2.String(), d1.String(),
				)

				require.Equal(
					t, tc.want.LessOrEquals, d1.LessOrEquals(&d2), "%s <= %s failed", d1.String(), d2.String(),
				)
				require.Equal(
					t, !tc.want.LessOrEquals || tc.want.Equal, d2.LessOrEquals(&d1), "%s <= %s", d2.String(),
					d1.String(),
				)

				require.Equal(t, tc.want.GreaterThan, d1.GreaterThan(&d2), "%s > %s", d1.String(), d2.String())
				require.Equal(
					t, !tc.want.Equal && !tc.want.GreaterThan, d2.GreaterThan(&d1), "%s > %s", d2.String(), d1.String(),
				)

				require.Equal(t, tc.want.GreaterOrEquals, d1.GreaterOrEquals(&d2), "%s >= %s", d1.String(), d2.String())
				require.Equal(
					t, !tc.want.GreaterOrEquals || tc.want.Equal, d2.GreaterOrEquals(&d1), "%s >= %s", d2.String(),
					d1.String(),
				)
			},
		)
	}
}

func TestDecimalArithmetic(t *testing.T) {
	type Want struct {
		Add      Decimal
		Subtract Decimal
		Multiply Decimal
		Divide   Decimal
	}
	tests := []struct {
		name string
		d1   Decimal
		d2   Decimal
		want Want
	}{
		{
			name: "SamePrecScale__(+A),(+B)",
			d1:   Dec(1234, 8, 2),
			d2:   Dec(5678, 8, 2),
			want: Want{
				Add:      Dec(6912, 8, 2),
				Subtract: Dec(-4444, 8, 2),
				Multiply: Dec(7006652, 8, 4),
				Divide:   Dec(21, 8, 2),
			},
		},
		{
			name: "DifferentPrecScale__(+A),(+B)",
			d1:   Dec(123456, 9, 3),
			d2:   Dec(789, 3, 2),
			want: Want{
				Add:      Dec(131346, 9, 3),
				Subtract: Dec(115566, 9, 3),
				Multiply: Dec(97406784, 9, 5),
				Divide:   Dec(15647, 9, 3),
			},
		},
		{
			name: "SamePrecScale__(-A),(+B)",
			d1:   Dec(-1234, 8, 2),
			d2:   Dec(789, 8, 2),
			want: Want{
				Add:      Dec(-445, 8, 2),
				Subtract: Dec(-2023, 8, 2),
				Multiply: Dec(-973626, 8, 4),
				Divide:   Dec(-156, 8, 2),
			},
		},
		{
			name: "DifferentPrecScale__(-A),(+B)",
			d1:   Dec(-123456, 9, 3),
			d2:   Dec(789, 3, 2),
			want: Want{
				Add:      Dec(-115566, 9, 3),
				Subtract: Dec(-131346, 9, 3),
				Multiply: Dec(-97406784, 9, 5),
				Divide:   Dec(-15647, 9, 3),
			},
		},
		{
			name: "SamePrecScale__(-A),(-B)",
			d1:   Dec(-1234, 8, 2),
			d2:   Dec(-5678, 8, 2),
			want: Want{
				Add:      Dec(-6912, 8, 2),
				Subtract: Dec(4444, 8, 2),
				Multiply: Dec(7006652, 8, 4),
				Divide:   Dec(21, 8, 2),
			},
		},
		{
			name: "DifferentPrecScale__(-A),(-B)",
			d1:   Dec(-123456, 9, 3),
			d2:   Dec(-789, 3, 2),
			want: Want{
				Add:      Dec(-131346, 9, 3),
				Subtract: Dec(-115566, 9, 3),
				Multiply: Dec(97406784, 9, 5),
				Divide:   Dec(15647, 9, 3),
			},
		},
	}
	for _, tc := range tests {
		t.Run(
			tc.name, func(t *testing.T) {
				d1 := tc.d1
				d2 := tc.d2
				dr, err := d1.Add(&d2)
				require.NoError(t, err, "%s + %s", d1.String(), d2.String())
				require.Equal(t, tc.want.Add, dr, "%s + %s == %s", d1.String(), d2.String(), dr.String())

				dr, err = d1.Subtract(&d2)
				require.NoError(t, err, "%s - %s", d1.String(), d2.String())
				require.Equal(t, tc.want.Subtract, dr, "%s - %s == %s", d1.String(), d2.String(), dr.String())

				dr, err = d1.Multiply(&d2)
				require.NoError(t, err, "%s * %s", d1.String(), d2.String())
				require.Equal(t, tc.want.Multiply, dr, "%s * %s == %s", d1.String(), d2.String(), dr.String())

				dr, err = d1.Divide(&d2)
				require.NoError(t, err, "%s / %s", d1.String(), d2.String())
				require.Equal(t, tc.want.Divide, dr, "%s / %s == %s", d1.String(), d2.String(), dr.String())
			},
		)
	}
}

func TestAddDoesNotFitInPrecision(t *testing.T) {
	d1 := Dec(923421, 6, 2)
	d2 := Dec(911233, 6, 2)
	_, err := d1.Add(&d2)
	require.Error(t, err)
	require.Equal(t, "result of decimal arithmetic does not fit in precision 6", err.Error())
}

func TestSubtractDoesNotFitInPrecision(t *testing.T) {
	d1 := Dec(923421, 6, 2)
	d2 := Dec(911233, 6, 2)
	d2.Num = d2.Num.Negate()
	_, err := d1.Subtract(&d2)
	require.Error(t, err)
	require.Equal(t, "result of decimal arithmetic does not fit in precision 6", err.Error())
}

func TestMultiplyDoesNotFitInPrecision(t *testing.T) {
	d1 := Dec(123421, 6, 2)
	d2 := Dec(211233, 6, 2)
	_, err := d1.Multiply(&d2)
	require.Error(t, err)
	require.Equal(t, "result of decimal arithmetic does not fit in precision 6", err.Error())
}

func TestShiftLeft(t *testing.T) {
	d1 := Dec(123421, 38, 2)
	dr := d1.Shift(4, false)
	require.Equal(t, Dec(1234210000, 38, 2), dr)
}

func TestShiftRight(t *testing.T) {
	d1 := Dec(123457, 38, 2)
	dr := d1.Shift(-2, false)
	require.Equal(t, Dec(1234, 38, 2), dr)
	dr = d1.Shift(-2, true)
	require.Equal(t, Dec(1235, 38, 2), dr)
}

func TestToFloat64(t *testing.T) {
	d := Dec(211233, 38, 2)
	require.Equal(t, 2112.33, d.ToFloat64())

	d = Dec(211233, 38, 0)
	require.Equal(t, float64(211233), d.ToFloat64())

	d = Dec(125, 38, 5)
	require.Equal(t, 0.00125, d.ToFloat64())
}

func TestToInt64(t *testing.T) {
	d := Dec(211233, 38, 2)
	require.Equal(t, int64(2112), d.ToInt64())

	d = Dec(211233, 38, 0)
	require.Equal(t, int64(211233), d.ToInt64())

	d = Dec(1234, 38, -2)
	require.Equal(t, int64(123400), d.ToInt64())
}

func TestNewDecimalFromInt64(t *testing.T) {
	d := NewDecimalFromInt64(0, 38, 6)
	require.Equal(t, int64(0), d.ToInt64())
	require.Equal(t, "0.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d = NewDecimalFromInt64(123456, 38, 6)
	require.Equal(t, int64(123456), d.ToInt64())
	require.Equal(t, "123456.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d = NewDecimalFromInt64(-123456, 38, 6)
	require.Equal(t, int64(-123456), d.ToInt64())
	require.Equal(t, "-123456.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d = NewDecimalFromInt64(math.MaxInt64, 38, 6)
	require.Equal(t, int64(math.MaxInt64), d.ToInt64())
	require.Equal(t, "9223372036854775807.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d = NewDecimalFromInt64(math.MinInt64, 38, 6)
	require.Equal(t, int64(math.MinInt64), d.ToInt64())
	require.Equal(t, "-9223372036854775808.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)
}

func TestNewDecimalFromFloat64(t *testing.T) {
	d, err := NewDecimalFromFloat64(0, 38, 6)
	require.NoError(t, err)
	require.Equal(t, float64(0), d.ToFloat64())
	require.Equal(t, "0.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d, err = NewDecimalFromFloat64(123456.25, 38, 6)
	require.NoError(t, err)
	require.Equal(t, float64(123456.25), d.ToFloat64())
	require.Equal(t, "123456.250000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d, err = NewDecimalFromFloat64(-123456.25, 38, 6)
	require.NoError(t, err)
	require.Equal(t, float64(-123456.25), d.ToFloat64())
	require.Equal(t, "-123456.250000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)
}

func TestNewDecimalFromString(t *testing.T) {
	d, err := NewDecimalFromString("0", 38, 6)
	require.NoError(t, err)
	require.Equal(t, int64(0), d.ToInt64())
	require.Equal(t, "0.000000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d, err = NewDecimalFromString("123456.25", 38, 6)
	require.NoError(t, err)
	require.Equal(t, float64(123456.25), d.ToFloat64())
	require.Equal(t, "123456.250000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)

	d, err = NewDecimalFromString("-123456.25", 38, 6)
	require.NoError(t, err)
	require.Equal(t, float64(-123456.25), d.ToFloat64())
	require.Equal(t, "-123456.250000", d.String())
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)
}

func TestConvertPrecisionAndScale(t *testing.T) {
	d, err := NewDecimalFromString("123456.654321", 38, 6)
	require.NoError(t, err)
	require.Equal(t, 38, d.Precision)
	require.Equal(t, 6, d.Scale)
	require.Equal(t, "123456.654321", d.String())

	d2 := d.ConvertPrecisionAndScale(38, 10)
	require.Equal(t, 38, d2.Precision)
	require.Equal(t, 10, d2.Scale)
	require.Equal(t, "123456.6543210000", d2.String())
}

func Dec(lo int64, prec int, scale int) Decimal {
	var num decimal128.Num
	if lo >= 0 {
		num = decimal128.New(0, uint64(lo))
	} else {
		num = decimal128.New(-1, uint64(lo))
	}

	return Decimal{
		Num:       num,
		Precision: prec,
		Scale:     scale,
	}
}
