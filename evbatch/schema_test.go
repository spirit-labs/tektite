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
