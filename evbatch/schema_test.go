package evbatch

import (
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSchemaGetters(t *testing.T) {
	decType := &types.DecimalType{
		Scale:     10,
		Precision: 5,
	}
	fNames := []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6"}
	fTypes := []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString,
		types.ColumnTypeBytes, types.ColumnTypeTimestamp}
	schema := NewEventSchema(fNames, fTypes)
	require.Equal(t, fNames, schema.ColumnNames())
	require.Equal(t, fTypes, schema.ColumnTypes())
}
