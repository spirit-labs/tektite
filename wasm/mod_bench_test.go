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
