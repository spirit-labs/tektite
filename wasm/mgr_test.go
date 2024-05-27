package wasm

import (
	"encoding/json"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/lock"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestPassAllTypes(t *testing.T) {

	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	paramTypes := []types.ColumnType{types.ColumnTypeInt,
		types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
		types.ColumnTypeTimestamp}
	returnType := types.ColumnTypeString

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcArgsAllTypes", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcArgsAllTypes")

	args := []any{int64(123456), float64(23.23), true, createDecimal(t, "76543.3456", types.DefaultDecimalPrecision, types.DefaultDecimalScale), "aardvarks", []byte("ABCDE"),
		types.NewTimestamp(102020202)}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, "123456 23.23 true 76543.345600 aardvarks [65 66 67 68 69] 102020202", res)

	args = []any{int64(-123456), float64(-23.23), false, createDecimal(t, "-76543.3456", types.DefaultDecimalPrecision, types.DefaultDecimalScale), "aardvarks", []byte("ABCDE"),
		types.NewTimestamp(102020202)}
	res, err = invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, "-123456 -23.23 false -76543.345600 aardvarks [65 66 67 68 69] 102020202", res)
}

func TestIntReturn(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeInt}
	returnType := types.ColumnTypeInt

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcIntReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcIntReturn")
	require.NoError(t, err)

	args := []any{int64(1234567)}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, int64(1234568), res)

	args = []any{int64(-1234567)}
	res, err = invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, int64(-1234566), res)
}

func TestFloatReturn(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeFloat}
	returnType := types.ColumnTypeFloat

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcFloatReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcFloatReturn")
	require.NoError(t, err)

	args := []any{float64(23.23)}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, float64(24.23), res)

	args = []any{float64(-23.23)}
	res, err = invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, float64(-22.23), res)
}

func TestBoolReturn(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeBool}
	returnType := types.ColumnTypeBool

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcBoolReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcBoolReturn")
	require.NoError(t, err)

	args := []any{true}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, false, res)

	args = []any{false}
	res, err = invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, true, res)
}

func TestDecimalReturn(t *testing.T) {
	decType := &types.DecimalType{
		Precision: 27,
		Scale:     3,
	}
	paramTypes := []types.ColumnType{decType}
	returnType := decType
	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcDecimalReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcDecimalReturn")
	require.NoError(t, err)

	args := []any{createDecimal(t, "34567.321", 27, 3)}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, createDecimal(t, "734567.321", 27, 3), res)
}

func TestStringReturn(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeString}
	returnType := types.ColumnTypeString

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcStringReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcStringReturn")
	require.NoError(t, err)

	args := []any{"antelopes"}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, "ANTELOPES", res)

	args = []any{""}
	res, err = invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, "", res)
}

func TestBytesReturn(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeBytes}
	returnType := types.ColumnTypeBytes

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcBytesReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcBytesReturn")
	require.NoError(t, err)

	args := []any{[]byte("giraffes")}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, []byte("GIRAFFES"), res)

	args = []any{nil}
	res, err = invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, []byte(nil), res)
}

func TestTimestampReturn(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeTimestamp}
	returnType := types.ColumnTypeTimestamp

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcTimestampReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	invoker, err := mgr.CreateInvoker("test_mod1.funcTimestampReturn")
	require.NoError(t, err)

	args := []any{types.NewTimestamp(3435343)}
	res, err := invoker.Invoke(args)
	require.NoError(t, err)
	require.Equal(t, types.NewTimestamp(3435344), res)
}

func TestRegisterUnRegisterModules(t *testing.T) {
	mgr := createModuleManager(t)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	modBytes, err := os.ReadFile("langs/tinygo/testmod2/test_mod2.wasm")
	require.NoError(t, err)
	meta := ModuleMetadata{
		ModuleName: "test_mod2",
		FunctionsMetadata: map[string]expr.FunctionMetadata{
			"foo": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt},
				ReturnType: types.ColumnTypeInt,
			},
			"bar": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt},
				ReturnType: types.ColumnTypeInt,
			},
		},
	}
	err = mgr.RegisterModule(meta, modBytes)
	require.NoError(t, err)

	modBytes, err = os.ReadFile("langs/tinygo/testmod3/test_mod3.wasm")
	require.NoError(t, err)
	meta = ModuleMetadata{
		ModuleName: "test_mod3",
		FunctionsMetadata: map[string]expr.FunctionMetadata{
			"quux": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt},
				ReturnType: types.ColumnTypeInt,
			},
			"kweep": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt},
				ReturnType: types.ColumnTypeInt,
			},
		},
	}
	err = mgr.RegisterModule(meta, modBytes)
	require.NoError(t, err)

	invokerFoo, err := mgr.CreateInvoker("test_mod2.foo")
	require.NoError(t, err)
	res, err := invokerFoo.Invoke([]any{int64(23)})
	require.NoError(t, err)
	require.Equal(t, int64(24), res.(int64))

	invokerBar, err := mgr.CreateInvoker("test_mod2.bar")
	require.NoError(t, err)
	res, err = invokerBar.Invoke([]any{int64(23)})
	require.NoError(t, err)
	require.Equal(t, int64(25), res.(int64))

	invokerQuux, err := mgr.CreateInvoker("test_mod3.quux")
	require.NoError(t, err)
	res, err = invokerQuux.Invoke([]any{int64(23)})
	require.NoError(t, err)
	require.Equal(t, int64(24), res.(int64))

	invokerKweep, err := mgr.CreateInvoker("test_mod3.kweep")
	require.NoError(t, err)
	res, err = invokerKweep.Invoke([]any{int64(23)})
	require.NoError(t, err)
	require.Equal(t, int64(25), res.(int64))

	err = mgr.UnregisterModule("test_mod2")
	require.NoError(t, err)

	res, err = invokerFoo.Invoke([]any{int64(23)})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))

	res, err = invokerBar.Invoke([]any{int64(23)})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))

	res, err = invokerQuux.Invoke([]any{int64(23)})
	require.NoError(t, err)
	require.Equal(t, int64(24), res.(int64))

	res, err = invokerKweep.Invoke([]any{int64(23)})
	require.NoError(t, err)
	require.Equal(t, int64(25), res.(int64))

	err = mgr.UnregisterModule("test_mod3")
	require.NoError(t, err)

	res, err = invokerQuux.Invoke([]any{int64(23)})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))

	res, err = invokerKweep.Invoke([]any{int64(23)})
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
}

func TestCreateInvokerUnknownModule(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeInt}
	returnType := types.ColumnTypeInt

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcIntReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	_, err := mgr.CreateInvoker("test_mod2.foo")
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
	require.Equal(t, "module 'test_mod2' is not registered", err.Error())
}

func TestCreateInvokerUnknownFunction(t *testing.T) {
	paramTypes := []types.ColumnType{types.ColumnTypeInt}
	returnType := types.ColumnTypeInt

	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return createSingleFuncMetadata("test_mod1", "funcIntReturn", paramTypes, returnType)
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	_, err := mgr.CreateInvoker("test_mod1.wibble")
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
	require.Equal(t, "function 'wibble' not exported from module 'test_mod1'", err.Error())
}

func TestMetaWithUnknownFunction(t *testing.T) {
	mgr := createModuleManager(t)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	modBytes, err := os.ReadFile("langs/tinygo/testmod1/test_mod1.wasm")
	require.NoError(t, err)
	meta := ModuleMetadata{
		ModuleName: "test_mod1",
		FunctionsMetadata: map[string]expr.FunctionMetadata{
			"foo": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt},
				ReturnType: types.ColumnTypeInt,
			},
		},
	}
	err = mgr.RegisterModule(meta, modBytes)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
	require.Equal(t, "module 'test_mod1' does not contain function 'foo'", err.Error())
}

func TestMetaWithNoFunctions(t *testing.T) {
	mgr := createModuleManager(t)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	modBytes, err := os.ReadFile("langs/tinygo/testmod1/test_mod1.wasm")
	require.NoError(t, err)
	meta := ModuleMetadata{
		ModuleName:        "test_mod1",
		FunctionsMetadata: map[string]expr.FunctionMetadata{},
	}
	err = mgr.RegisterModule(meta, modBytes)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
	require.Equal(t, "module 'test_mod1' does not export any functions", err.Error())
}

func TestMetaWithIncorrectParamTypes(t *testing.T) {
	mgr := createModuleManager(t)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	modBytes, err := os.ReadFile("langs/tinygo/testmod1/test_mod1.wasm")
	require.NoError(t, err)
	meta := ModuleMetadata{
		ModuleName: "test_mod1",
		FunctionsMetadata: map[string]expr.FunctionMetadata{
			"funcArgsAllTypes": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp},
				ReturnType: types.ColumnTypeString,
			},
		},
	}
	err = mgr.RegisterModule(meta, modBytes)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
	require.Equal(t, "function 'funcArgsAllTypes' as defined in the json metadata would require a wasm function with wasm parameter types [i64,f64,i32,i64,i64,i64]. But the actual wasm function has parameter types [i64,f64,i32,i64,i64,i64,i64]", err.Error())
}

func TestMetaWithIncorrectReturnType(t *testing.T) {
	mgr := createModuleManager(t)
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()

	modBytes, err := os.ReadFile("langs/tinygo/testmod1/test_mod1.wasm")
	require.NoError(t, err)
	decType := &types.DecimalType{
		Precision: 38,
		Scale:     6,
	}
	meta := ModuleMetadata{
		ModuleName: "test_mod1",
		FunctionsMetadata: map[string]expr.FunctionMetadata{
			"funcArgsAllTypes": {
				ParamTypes: []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes, types.ColumnTypeTimestamp},
				ReturnType: types.ColumnTypeBool,
			},
		},
	}
	err = mgr.RegisterModule(meta, modBytes)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, errors.WasmError))
	require.Equal(t, "function 'funcArgsAllTypes' as defined in the json metadata would require a wasm function with return type i32. But the actual wasm function has return type i64", err.Error())
}

func TestModuleMetadataFromJson(t *testing.T) {
	str := `
{
    "name": "my_mod_23",
    "functions": {
        "func1": {
            "paramTypes": ["int", "float", "bool", "decimal(19,3)", "string", "bytes", "timestamp"],
            "returnType": "float"
        },
        "func2": {
            "paramTypes": ["int", "bytes", "timestamp"],
            "returnType": "string"
        },
        "func3": {
            "paramTypes": [],
            "returnType": "decimal(23, 4)"
        }
    }
}
`
	var meta ModuleMetadata
	err := json.Unmarshal([]byte(str), &meta)
	require.NoError(t, err)

	require.Equal(t, "my_mod_23", meta.ModuleName)
	require.Equal(t, 3, len(meta.FunctionsMetadata))

	funcMeta1, ok := meta.FunctionsMetadata["func1"]
	require.True(t, ok)
	require.Equal(t, 7, len(funcMeta1.ParamTypes))
	require.Equal(t, types.ColumnTypeInt, funcMeta1.ParamTypes[0])
	require.Equal(t, types.ColumnTypeFloat, funcMeta1.ParamTypes[1])
	require.Equal(t, types.ColumnTypeBool, funcMeta1.ParamTypes[2])
	decType1 := types.DecimalType{
		Precision: 19,
		Scale:     3,
	}
	require.Equal(t, &decType1, funcMeta1.ParamTypes[3])
	require.Equal(t, types.ColumnTypeString, funcMeta1.ParamTypes[4])
	require.Equal(t, types.ColumnTypeBytes, funcMeta1.ParamTypes[5])
	require.Equal(t, types.ColumnTypeTimestamp, funcMeta1.ParamTypes[6])
	require.Equal(t, types.ColumnTypeFloat, funcMeta1.ReturnType)

	funcMeta2, ok := meta.FunctionsMetadata["func2"]
	require.True(t, ok)
	require.Equal(t, 3, len(funcMeta2.ParamTypes))
	require.Equal(t, types.ColumnTypeInt, funcMeta2.ParamTypes[0])
	require.Equal(t, types.ColumnTypeBytes, funcMeta2.ParamTypes[1])
	require.Equal(t, types.ColumnTypeTimestamp, funcMeta2.ParamTypes[2])
	require.Equal(t, types.ColumnTypeString, funcMeta2.ReturnType)

	funcMeta3, ok := meta.FunctionsMetadata["func3"]
	require.True(t, ok)
	require.Equal(t, 0, len(funcMeta3.ParamTypes))
	decType2 := types.DecimalType{
		Precision: 23,
		Scale:     4,
	}
	require.Equal(t, &decType2, funcMeta3.ReturnType)
}

func TestGetFunctionMetadata(t *testing.T) {
	decType := &types.DecimalType{
		Precision: types.DefaultDecimalPrecision,
		Scale:     types.DefaultDecimalScale,
	}
	funcMeta1 := expr.FunctionMetadata{
		ParamTypes: []types.ColumnType{types.ColumnTypeInt,
			types.ColumnTypeFloat, types.ColumnTypeBool, decType, types.ColumnTypeString, types.ColumnTypeBytes,
			types.ColumnTypeTimestamp},
		ReturnType: types.ColumnTypeString,
	}
	funcMeta2 := expr.FunctionMetadata{
		ParamTypes: []types.ColumnType{types.ColumnTypeInt},
		ReturnType: types.ColumnTypeInt,
	}
	funcMeta3 := expr.FunctionMetadata{
		ParamTypes: []types.ColumnType{types.ColumnTypeFloat},
		ReturnType: types.ColumnTypeFloat,
	}
	mgr := setup(t, "langs/tinygo/testmod1/test_mod1.wasm", func() ModuleMetadata {
		return ModuleMetadata{
			ModuleName: "test_mod1",
			FunctionsMetadata: map[string]expr.FunctionMetadata{
				"funcArgsAllTypes": funcMeta1,
				"funcIntReturn":    funcMeta2,
				"funcFloatReturn":  funcMeta3,
			},
		}
	})
	defer func() {
		err := mgr.Stop()
		require.NoError(t, err)
	}()
	meta, ok, err := mgr.GetFunctionMetadata("test_mod1.funcArgsAllTypes")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, funcMeta1, meta)
	meta, ok, err = mgr.GetFunctionMetadata("test_mod1.funcIntReturn")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, funcMeta2, meta)
	meta, ok, err = mgr.GetFunctionMetadata("test_mod1.funcFloatReturn")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, funcMeta3, meta)
	meta, ok, err = mgr.GetFunctionMetadata("test_mod1.notExists")
	require.NoError(t, err)
	require.False(t, ok)
	meta, ok, err = mgr.GetFunctionMetadata("mod_not_exists.funcFloatReturn")
	require.NoError(t, err)
	require.False(t, ok)
}

func createModuleManager(t require.TestingT) *ModuleManager {
	objStore := dev.NewInMemStore(0)
	lockMgr := lock.NewInMemLockManager()
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	mgr := NewModuleManager(objStore, lockMgr, cfg)
	err := mgr.Start()
	require.NoError(t, err)
	return mgr
}

func setup(t require.TestingT, modFile string, createMetadataFunc func() ModuleMetadata) *ModuleManager {
	mgr := createModuleManager(t)
	modBytes, err := os.ReadFile(modFile)
	require.NoError(t, err)
	meta := createMetadataFunc()
	err = mgr.RegisterModule(meta, modBytes)
	require.NoError(t, err)
	return mgr
}

func createDecimal(t *testing.T, str string, precision int, scale int) types.Decimal {
	num, err := decimal128.FromString(str, int32(precision), int32(scale))
	require.NoError(t, err)
	return types.Decimal{
		Num:       num,
		Precision: precision,
		Scale:     scale,
	}
}

func createSingleFuncMetadata(moduleName string, functionName string, paramTypes []types.ColumnType, returnType types.ColumnType) ModuleMetadata {
	metaData := ModuleMetadata{
		ModuleName: moduleName,
		FunctionsMetadata: map[string]expr.FunctionMetadata{
			functionName: {
				ParamTypes: paramTypes,
				ReturnType: returnType,
			},
		},
	}
	return metaData
}
