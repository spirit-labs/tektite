package types

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/stretchr/testify/require"
)

func TestDecimal_Equals(t *testing.T) {
	type d struct {
		Num       int64
		Precision int
		Scale     int
	}
	tests := []struct {
		name string
		d1   d
		d2   d
		want bool
	}{
		{
			name: "same precision and scale and equal values (+ve)",
			d1:   d{123421, 10, 2},
			d2:   d{123421, 10, 2},
			want: true,
		},
		{
			name: "same precision and scale and not equal values (+ve)",
			d1:   d{123421, 10, 2},
			d2:   d{456456, 10, 2},
			want: false,
		},
		{
			name: "same precision and scale and equal values (-ve)",
			d1:   d{-123421, 10, 2},
			d2:   d{-123421, 10, 2},
			want: true,
		},
		{
			name: "same precision and scale and not equal values (-ve)",
			d1:   d{-123421, 10, 2},
			d2:   d{-456456, 10, 2},
			want: false,
		},
		{
			name: "different precision and scale and equal values (+ve)",
			d1:   d{123421, 10, 2},
			d2:   d{12342100, 13, 4},
			want: true,
		},
		{
			name: "different precision and scale and not equal values (+ve)",
			d1:   d{123421, 10, 2},
			d2:   d{45645600, 13, 4},
			want: false,
		},
		{
			name: "different precision and scale and equal values (-ve)",
			d1:   d{-123421, 10, 2},
			d2:   d{-12342100, 13, 4},
			want: true,
		},
		{
			name: "different precision and scale and not equal values (-ve)",
			d1:   d{-123421, 10, 2},
			d2:   d{-45645600, 13, 4},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(
			tc.name, func(t *testing.T) {
				d1 := createDecimal(tc.d1.Num, tc.d1.Precision, tc.d1.Scale)
				d2 := createDecimal(tc.d2.Num, tc.d2.Precision, tc.d2.Scale)
				got := d1.Equals(&d2)
				if got != tc.want {
					t.Errorf("Equals() d1 -> d2 = %v, want %v", got, tc.want)
				}
				got = d2.Equals(&d1)
				if got != tc.want {
					t.Errorf("Equals() d2 -> d1 = %v, want %v", got, tc.want)
				}
			},
		)
	}
}

func TestLessThanSamePrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(123421, 10, 2)
	d3 := createDecimal(456456, 10, 2)
	require.True(t, d1.LessThan(&d3))
	require.False(t, d3.LessThan(&d1))
	require.False(t, d1.LessThan(&d2))
	require.False(t, d2.LessThan(&d1))
}

func TestLessThanDifferentPrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(12342100, 13, 4)
	d3 := createDecimal(22342100, 13, 4)
	d4 := createDecimal(11342100, 13, 4)
	require.False(t, d1.LessThan(&d2))
	require.False(t, d2.LessThan(&d1))
	require.True(t, d1.LessThan(&d3))
	require.False(t, d1.LessThan(&d4))
	require.True(t, d4.LessThan(&d1))
}

func TestLessOrEqualsSamePrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(123421, 10, 2)
	d3 := createDecimal(456456, 10, 2)
	require.True(t, d1.LessOrEquals(&d3))
	require.False(t, d3.LessOrEquals(&d1))
	require.True(t, d1.LessOrEquals(&d2))
	require.True(t, d2.LessOrEquals(&d1))
}

func TestLessOrEqualsDifferentPrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(12342100, 13, 4)
	d3 := createDecimal(22342100, 13, 4)
	d4 := createDecimal(11342100, 13, 4)
	require.True(t, d1.LessOrEquals(&d2))
	require.True(t, d2.LessOrEquals(&d1))
	require.True(t, d1.LessOrEquals(&d3))
	require.False(t, d1.LessOrEquals(&d4))
	require.True(t, d4.LessOrEquals(&d1))
}

func TestGreaterThanSamePrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(123421, 10, 2)
	d3 := createDecimal(456456, 10, 2)
	require.True(t, d3.GreaterThan(&d1))
	require.False(t, d1.GreaterThan(&d2))
	require.False(t, d2.GreaterThan(&d1))
}

func TestGreaterThanDifferentPrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(12342100, 13, 4)
	d3 := createDecimal(22342100, 13, 4)
	d4 := createDecimal(11342100, 13, 4)
	require.False(t, d1.GreaterThan(&d2))
	require.False(t, d2.GreaterThan(&d1))
	require.False(t, d1.GreaterThan(&d3))
	require.True(t, d1.GreaterThan(&d4))
	require.False(t, d4.GreaterThan(&d1))
}

func TestGreaterOrEqualsSamePrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(123421, 10, 2)
	d3 := createDecimal(456456, 10, 2)
	require.True(t, d3.GreaterOrEquals(&d1))
	require.True(t, d1.GreaterOrEquals(&d2))
	require.True(t, d2.GreaterOrEquals(&d1))
	require.True(t, d3.GreaterOrEquals(&d1))
}

func TestGreaterOrEqualsDifferentPrecScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(12342100, 13, 4)
	d3 := createDecimal(22342100, 13, 4)
	d4 := createDecimal(11342100, 13, 4)
	require.True(t, d1.GreaterOrEquals(&d2))
	require.True(t, d2.GreaterOrEquals(&d1))
	require.False(t, d1.GreaterOrEquals(&d3))
	require.True(t, d1.GreaterThan(&d4))
	require.False(t, d4.GreaterOrEquals(&d1))
}

func TestAddSameScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(211233, 10, 2)
	dr, err := d1.Add(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(334654, 10, 2), dr)
}

func TestAddDifferentScale(t *testing.T) {
	d1 := createDecimal(123421456, 10, 5)
	d2 := createDecimal(211233, 10, 2)
	dr, err := d1.Add(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(334654456, 10, 5), dr)
}

func TestSubtractSameScale(t *testing.T) {
	d1 := createDecimal(123421, 10, 2)
	d2 := createDecimal(211233, 10, 2)
	dr, err := d2.Subtract(&d1)
	require.NoError(t, err)
	require.Equal(t, createDecimal(87812, 10, 2), dr)
}

func TestSubtractDifferentScale(t *testing.T) {
	d1 := createDecimal(123421456, 10, 5)
	d2 := createDecimal(211233, 10, 2)
	dr, err := d2.Subtract(&d1)
	require.NoError(t, err)
	require.Equal(t, createDecimal(87811544, 10, 5), dr)
}

func TestMultiplySameScale(t *testing.T) {
	d1 := createDecimal(123421, 38, 2)
	d2 := createDecimal(211233, 38, 2)
	dr, err := d1.Multiply(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(26070588093, 38, 4), dr)
}

func TestMultiplyDifferentScale(t *testing.T) {
	d1 := createDecimal(123421765, 38, 5)
	d2 := createDecimal(211233, 38, 2)
	dr, err := d1.Multiply(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(26070749686245, 38, 7), dr)
}

func TestDivide(t *testing.T) {
	d1 := createDecimal(333333, 38, 2)
	d2 := createDecimal(300, 38, 2)
	dr, err := d1.Divide(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(111111, 38, 2), dr)

	d1 = createDecimal(444444, 38, 2)
	d2 = createDecimal(200, 38, 2)
	dr, err = d1.Divide(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(222222, 38, 2), dr)

	d1 = createDecimal(1111, 38, 2)
	d2 = createDecimal(25, 38, 2)
	dr, err = d1.Divide(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(4444, 38, 2), dr)

	d1 = createDecimal(333333, 38, 5)
	d2 = createDecimal(300, 38, 2)
	dr, err = d1.Divide(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(111111, 38, 5), dr)

	d1 = createDecimal(1111, 38, 4)
	d2 = createDecimal(25, 38, 2)
	dr, err = d1.Divide(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(4444, 38, 4), dr)

	d1 = createDecimal(1111, 38, 4)
	d2 = createDecimal(25, 38, 2)
	dr, err = d1.Divide(&d2)
	require.NoError(t, err)
	require.Equal(t, createDecimal(4444, 38, 4), dr)
}

func TestAddDoesNotFitInPrecision(t *testing.T) {
	d1 := createDecimal(923421, 6, 2)
	d2 := createDecimal(911233, 6, 2)
	_, err := d1.Add(&d2)
	require.Error(t, err)
	require.Equal(t, "result of decimal arithmetic does not fit in precision 6", err.Error())
}

func TestSubtractDoesNotFitInPrecision(t *testing.T) {
	d1 := createDecimal(923421, 6, 2)
	d2 := createDecimal(911233, 6, 2)
	d2.Num = d2.Num.Negate()
	_, err := d1.Subtract(&d2)
	require.Error(t, err)
	require.Equal(t, "result of decimal arithmetic does not fit in precision 6", err.Error())
}

func TestMultiplyDoesNotFitInPrecision(t *testing.T) {
	d1 := createDecimal(123421, 6, 2)
	d2 := createDecimal(211233, 6, 2)
	_, err := d1.Multiply(&d2)
	require.Error(t, err)
	require.Equal(t, "result of decimal arithmetic does not fit in precision 6", err.Error())
}

func TestShiftLeft(t *testing.T) {
	d1 := createDecimal(123421, 38, 2)
	dr := d1.Shift(4, false)
	require.Equal(t, createDecimal(1234210000, 38, 2), dr)
}

func TestShiftRight(t *testing.T) {
	d1 := createDecimal(123457, 38, 2)
	dr := d1.Shift(-2, false)
	require.Equal(t, createDecimal(1234, 38, 2), dr)
	dr = d1.Shift(-2, true)
	require.Equal(t, createDecimal(1235, 38, 2), dr)
}

func TestToFloat64(t *testing.T) {
	d := createDecimal(211233, 38, 2)
	require.Equal(t, 2112.33, d.ToFloat64())

	d = createDecimal(211233, 38, 0)
	require.Equal(t, float64(211233), d.ToFloat64())

	d = createDecimal(125, 38, 5)
	require.Equal(t, 0.00125, d.ToFloat64())
}

func TestToInt64(t *testing.T) {
	d := createDecimal(211233, 38, 2)
	require.Equal(t, int64(2112), d.ToInt64())

	d = createDecimal(211233, 38, 0)
	require.Equal(t, int64(211233), d.ToInt64())

	d = createDecimal(1234, 38, -2)
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

func createDecimal(lo int64, prec int, scale int) Decimal {
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
