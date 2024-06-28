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

package wasm

import (
	"fmt"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkStringInvocation(b *testing.B) {

	paramTypes := []types.ColumnType{types.ColumnTypeInt}
	returnType := types.ColumnTypeInt

	mgr := setup(b, "langs/tinygo/test_mod.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod", "funcIntReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(b, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod.funcIntReturn")
	require.NoError(b, err)

	b.ResetTimer()

	var l int64

	for i := 0; i < b.N; i++ {
		res, err := invoker.Invoke([]any{int64(23)})
		if err != nil {
			panic(err)
		}
		l += res.(int64)
	}

	b.StopTimer()

	fmt.Print(l)
}
