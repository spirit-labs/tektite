package types

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"strconv"
	"strings"
)

type ColumnTypeID int

type Timestamp struct {
	Val int64
}

func NewTimestamp(val int64) Timestamp {
	return Timestamp{Val: val}
}

const (
	ColumnTypeIDInt = iota + 1
	ColumnTypeIDFloat
	ColumnTypeIDBool
	ColumnTypeIDDecimal
	ColumnTypeIDString
	ColumnTypeIDBytes
	ColumnTypeIDTimestamp
)

var ColumnTypeInt = &nonParameterizedType{id: ColumnTypeIDInt}
var ColumnTypeFloat = &nonParameterizedType{id: ColumnTypeIDFloat}
var ColumnTypeBool = &nonParameterizedType{id: ColumnTypeIDBool}
var ColumnTypeString = &nonParameterizedType{id: ColumnTypeIDString}
var ColumnTypeBytes = &nonParameterizedType{id: ColumnTypeIDBytes}
var ColumnTypeTimestamp = &nonParameterizedType{id: ColumnTypeIDTimestamp}

type nonParameterizedType struct {
	id ColumnTypeID
}

func (n nonParameterizedType) ID() ColumnTypeID {
	return n.id
}

func (n nonParameterizedType) String() string {
	switch n.id {
	case ColumnTypeIDInt:
		return "int"
	case ColumnTypeIDFloat:
		return "float"
	case ColumnTypeIDBool:
		return "bool"
	case ColumnTypeIDString:
		return "string"
	case ColumnTypeIDBytes:
		return "bytes"
	case ColumnTypeIDTimestamp:
		return "timestamp"
	default:
		panic("unexpected type")
	}
}

func StringToColumnType(sColumnType string) (ColumnType, error) {
	var cType ColumnType
	switch sColumnType {
	case "int":
		cType = ColumnTypeInt
	case "float":
		cType = ColumnTypeFloat
	case "bool":
		cType = ColumnTypeBool
	case "string":
		cType = ColumnTypeString
	case "bytes":
		cType = ColumnTypeBytes
	case "timestamp":
		cType = ColumnTypeTimestamp
	default:
		if strings.HasPrefix(sColumnType, "decimal(") {
			decType, err := parseDecimalType(sColumnType)
			if err != nil {
				return nil, err
			}
			cType = decType
		} else {
			return nil, errwrap.Errorf("invalid type '%s'", sColumnType)
		}
	}
	return cType, nil
}

func ColumnTypesToString(columnTypes []ColumnType) string {
	var sb strings.Builder
	for i, ct := range columnTypes {
		sb.WriteString(ct.String())
		if i != len(columnTypes)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func parseDecimalType(sargtype string) (ColumnType, error) {
	if len(sargtype) > 8 {
		rem := sargtype[8 : len(sargtype)-1]
		if len(rem) >= 3 {
			comIndex := strings.IndexRune(rem, ',')
			if comIndex != -1 {
				sPrec := strings.Trim(rem[:comIndex], " \t")
				sScale := strings.Trim(rem[comIndex+1:], " \t")
				prec, err := strconv.Atoi(sPrec)
				if err != nil {
					return nil, errwrap.Errorf("invalid decimal precision, not a valid integer %s", sPrec)
				}
				if prec < 1 || prec > 38 {
					return nil, errwrap.Errorf("invalid decimal precision, must be > 1 and <= 38 %s", sargtype)
				}
				scale, err := strconv.Atoi(sScale)
				if err != nil {
					return nil, errwrap.Errorf("invalid decimal scale, not a valid integer %s", sScale)
				}
				if scale < 0 || scale > 38 {
					return nil, errwrap.Errorf("invalid decimal scale, must be >= 0 and <= 38 %s", sargtype)
				}
				if scale > prec {
					return nil, errwrap.Errorf("invalid decimal scale cannot be > precision %s", sargtype)
				}
				return &DecimalType{
					Precision: prec,
					Scale:     scale,
				}, nil
			}
		}
	}
	return nil, errwrap.Errorf("invalid decimal argument type: %s", sargtype)
}

type ColumnType interface {
	ID() ColumnTypeID
	String() string
}

func ColumnTypesEqual(ct1 ColumnType, ct2 ColumnType) bool {
	if ct1.ID() != ct2.ID() {
		return false
	}
	d1, ok1 := ct1.(*DecimalType)
	d2, ok2 := ct2.(*DecimalType)
	if !ok1 && !ok2 {
		return true
	}
	if !ok1 || !ok2 {
		return false
	}
	return d1.Scale == d2.Scale && d1.Precision == d2.Precision
}

type DecimalType struct {
	Precision int
	Scale     int
}

func (d *DecimalType) ID() ColumnTypeID {
	return ColumnTypeIDDecimal
}

func (d *DecimalType) String() string {
	return fmt.Sprintf("decimal(%d,%d)", d.Precision, d.Scale)
}
