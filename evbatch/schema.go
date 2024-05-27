package evbatch

import (
	"github.com/spirit-labs/tektite/types"
	"strings"
)

type EventSchema struct {
	columnNames []string
	columnTypes []types.ColumnType
}

func NewEventSchema(columnNames []string, columnTypes []types.ColumnType) *EventSchema {
	if len(columnNames) != len(columnTypes) {
		panic("columnNames and columnTypes must be same length")
	}
	return &EventSchema{
		columnNames: columnNames,
		columnTypes: columnTypes,
	}
}

func (s *EventSchema) ColumnNames() []string {
	return s.columnNames
}

func (s *EventSchema) ColumnTypes() []types.ColumnType {
	return s.columnTypes
}

func (s *EventSchema) String() string {
	sb := strings.Builder{}
	for i, colName := range s.columnNames {
		colType := s.columnTypes[i]
		sb.WriteString(colName)
		sb.WriteString(": ")
		sb.WriteString(colType.String())
		if i != len(s.columnNames)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}
