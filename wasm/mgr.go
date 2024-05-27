package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/lock"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/types"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	moduleManagerLock       = "wasm-module-lock"
	moduleObjectStorePrefix = "wasm-module"
)

type ModuleManager struct {
	lock              sync.RWMutex
	started           bool
	cfg               *conf.Config
	objStoreClient    objstore.Client
	lockMgr           lock.Manager
	registeredModules map[string]*RegisteredModule
	runtime           wazero.Runtime
}

type RegisteredModule struct {
	metaData        ModuleMetadata
	runtime         wazero.Runtime
	compiledModule  wazero.CompiledModule
	moduleInstances []*modWrapper
	instancePos     int64
}

type ModuleMetadata struct {
	ModuleName        string                           `json:"name"`
	FunctionsMetadata map[string]expr.FunctionMetadata `json:"functions"`
}

type modWrapper struct {
	instance api.Module
	lock     sync.Mutex
}

func (mm *ModuleMetadata) ToJsonBytes() []byte {
	bytes, err := json.Marshal(mm)
	if err != nil {
		panic(err)
	}
	return bytes
}

func NewModuleManager(objStoreClient objstore.Client, lockMgr lock.Manager, cfg *conf.Config) *ModuleManager {
	return &ModuleManager{
		cfg:               cfg,
		objStoreClient:    objStoreClient,
		lockMgr:           lockMgr,
		registeredModules: map[string]*RegisteredModule{},
	}
}

func (m *ModuleManager) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	r := wazero.NewRuntime(context.Background())
	_, err := r.NewHostModuleBuilder("env").
		NewFunctionBuilder().WithFunc(logString).Export("log").
		Instantiate(context.Background())
	if err != nil {
		return err
	}
	wasi_snapshot_preview1.MustInstantiate(context.Background(), r)
	m.runtime = r
	m.started = true
	return nil
}

func (m *ModuleManager) Stop() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	return m.runtime.Close(context.Background())
}

func (m *ModuleManager) RegisterModule(metaData ModuleMetadata, moduleBytes []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}

	if err := m.getClusterWideLock(); err != nil {
		return err
	}
	defer func() {
		_, err := m.lockMgr.ReleaseLock(moduleManagerLock)
		if err != nil {
			log.Errorf("failed to release lock %v", err)
		}
	}()
	_, ok := m.registeredModules[metaData.ModuleName]
	if !ok {
		modKey, jsonKey := createModuleKeys(metaData.ModuleName)
		bytes, err := m.objStoreClient.Get(jsonKey)
		if err != nil {
			return err
		}
		if bytes == nil {
			registeredModule, err := m.compileAndCreateRegisteredModule(metaData, moduleBytes)
			if err != nil {
				return err
			}
			if err := m.objStoreClient.Put(modKey, moduleBytes); err != nil {
				return err
			}
			metaBytes, err := json.Marshal(&metaData)
			if err != nil {
				return err
			}
			if err := m.objStoreClient.Put(jsonKey, metaBytes); err != nil {
				return err
			}
			m.registeredModules[metaData.ModuleName] = registeredModule
			return nil
		}
	}
	return errors.NewTektiteErrorf(errors.WasmError, "module '%s' already registered", metaData.ModuleName)
}

func (m *ModuleManager) compileAndCreateRegisteredModule(metadata ModuleMetadata, moduleBytes []byte) (*RegisteredModule, error) {
	mod, err := m.runtime.CompileModule(context.Background(), moduleBytes)
	if err != nil {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "failed to compile wasm module (possibly corrupt): %v", err)
	}
	// Create the instances
	moduleInstances := make([]*modWrapper, m.cfg.WasmModuleInstances)
	for i := range moduleInstances {
		instance, err := m.runtime.InstantiateModule(context.Background(), mod, wazero.NewModuleConfig().WithName(""))
		if err != nil {
			return nil, errors.NewTektiteErrorf(errors.WasmError, "failed to instantiate wasm module %v", err)
		}
		moduleInstances[i] = &modWrapper{instance: instance}
	}
	registeredModule := &RegisteredModule{
		metaData:        metadata,
		runtime:         m.runtime,
		compiledModule:  mod,
		moduleInstances: moduleInstances,
	}
	if err := registeredModule.Validate(); err != nil {
		// failed validation, close the module
		registeredModule.close()
		return nil, err
	}
	return registeredModule, nil
}

func (m *ModuleManager) UnregisterModule(name string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return errors.New("not started")
	}
	if err := m.getClusterWideLock(); err != nil {
		return err
	}
	defer func() {
		_, err := m.lockMgr.ReleaseLock(moduleManagerLock)
		if err != nil {
			log.Errorf("failed to release lock %v", err)
		}
	}()
	registeredModule, ok := m.registeredModules[name]
	if !ok {
		return errors.NewTektiteErrorf(errors.WasmError, "unknown module '%s'", name)
	}
	registeredModule.close()
	delete(m.registeredModules, name)
	modKey, jsonKey := createModuleKeys(name)
	if err := m.objStoreClient.Delete(modKey); err != nil {
		return err
	}
	return m.objStoreClient.Delete(jsonKey)
}

func (m *ModuleManager) getClusterWideLock() error {
	for {
		ok, err := m.lockMgr.GetLock(moduleManagerLock)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// Lock is already held - retry after delay
		time.Sleep(250 * time.Millisecond)
		continue
	}
}

func (m *ModuleManager) GetFunctionMetadata(fullFuncName string) (expr.FunctionMetadata, bool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return expr.FunctionMetadata{}, false, errors.New("not started")
	}
	modName, funcName, err := extractModAndFuncName(fullFuncName)
	if err != nil {
		return expr.FunctionMetadata{}, false, err
	}
	registeredModule, ok := m.registeredModules[modName]
	if !ok {
		// Lazy load
		registeredModule, err = m.maybeLoadModule(modName)
		if err != nil {
			return expr.FunctionMetadata{}, false, err
		}
		if registeredModule == nil {
			return expr.FunctionMetadata{}, false, nil
		}
	}
	meta, ok := registeredModule.metaData.FunctionsMetadata[funcName]
	if !ok {
		return expr.FunctionMetadata{}, false, nil
	}
	return meta, true, nil
}

func (m *ModuleManager) maybeLoadModule(moduleName string) (*RegisteredModule, error) {
	modKey, jsonKey := createModuleKeys(moduleName)
	modBytes, err := m.objStoreClient.Get(modKey)
	if err != nil {
		return nil, err
	}
	if modBytes == nil {
		return nil, nil
	}
	jsonBytes, err := m.objStoreClient.Get(jsonKey)
	if err != nil {
		return nil, err
	}
	if jsonBytes == nil {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "json bytes object for module '%s' not found in object store",
			moduleName)
	}
	var metaData ModuleMetadata
	if err := json.Unmarshal(jsonBytes, &metaData); err != nil {
		return nil, err
	}
	registeredModule, err := m.compileAndCreateRegisteredModule(metaData, modBytes)
	if err != nil {
		return nil, err
	}
	m.registeredModules[metaData.ModuleName] = registeredModule
	return registeredModule, nil
}

func (m *ModuleManager) CreateInvoker(fullFuncName string) (*Invoker, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.started {
		return nil, errors.New("not started")
	}
	modName, funcName, err := extractModAndFuncName(fullFuncName)
	if err != nil {
		return nil, err
	}
	registeredModule, ok := m.registeredModules[modName]
	if !ok {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "module '%s' is not registered", modName)
	}
	return registeredModule.createInvoker(funcName)
}

func extractModAndFuncName(fullFuncName string) (string, string, error) {
	pos := strings.Index(fullFuncName, ".")
	if pos < 1 {
		return "", "", errors.NewTektiteError(errors.WasmError, "invalid external function name. must be of form <module_name>.<function_name>")
	}
	modName := fullFuncName[:pos]
	funcName := fullFuncName[pos+1:]
	return modName, funcName, nil
}

func (r *RegisteredModule) Validate() error {
	if len(r.metaData.FunctionsMetadata) == 0 {
		return errors.NewTektiteErrorf(errors.WasmError, "module '%s' does not export any functions", r.metaData.ModuleName)
	}
	for funcName, funcMetaData := range r.metaData.FunctionsMetadata {
		f := r.moduleInstances[0].instance.ExportedFunction(funcName)
		if f == nil {
			return errors.NewTektiteErrorf(errors.WasmError, "module '%s' does not contain function '%s'", r.metaData.ModuleName, funcName)
		}
		if err := r.checkFunctionSignature(f, funcMetaData.ParamTypes, funcMetaData.ReturnType); err != nil {
			return err
		}
	}
	return nil
}

func (r *RegisteredModule) createInvoker(funcName string) (*Invoker, error) {
	// choose a module instance round-robin (non-strict)
	pos := int(atomic.AddInt64(&r.instancePos, 1)) % len(r.moduleInstances)
	wrapper := r.moduleInstances[pos]
	meta, ok := r.metaData.FunctionsMetadata[funcName]
	if !ok {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "function '%s' not exported from module '%s'", funcName, r.metaData.ModuleName)
	}
	f := wrapper.instance.ExportedFunction(funcName)

	malloc := wrapper.instance.ExportedFunction("malloc")
	if malloc == nil {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "module '%s' must export a 'malloc' function", r.metaData.ModuleName)
	}
	free := wrapper.instance.ExportedFunction("free")
	if malloc == nil {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "module '%s' must export a 'free' function", r.metaData.ModuleName)
	}
	invoker := &Invoker{
		meta:   meta,
		mod:    wrapper,
		f:      f,
		malloc: malloc,
		free:   free,
	}
	return invoker, nil
}

func wasmTypesToString(wasmTypes []api.ValueType) string {
	var sb strings.Builder
	sb.WriteRune('[')
	for i, wasmType := range wasmTypes {
		sb.WriteString(api.ValueTypeName(wasmType))
		if i != len(wasmTypes)-1 {
			sb.WriteRune(',')
		}
	}
	sb.WriteRune(']')
	return sb.String()
}

func (r *RegisteredModule) checkFunctionSignature(f api.Function, paramTypes []types.ColumnType, returnType types.ColumnType) error {
	def := f.Definition()
	pts := def.ParamTypes()
	var expectedParamTypes []api.ValueType
	for _, paramType := range paramTypes {
		wasmType := wasmTypeForTektiteType(paramType)
		expectedParamTypes = append(expectedParamTypes, wasmType)
	}
	expectedReturnType := wasmTypeForTektiteType(returnType)
	if !reflect.DeepEqual(pts, expectedParamTypes) {
		return errors.NewTektiteErrorf(errors.WasmError, "function '%s' as defined in the json metadata would require a wasm function with wasm parameter types %s. But the actual wasm function has parameter types %s",
			def.Name(), wasmTypesToString(expectedParamTypes), wasmTypesToString(pts))
	}
	if len(def.ResultTypes()) != 1 {
		return errors.NewTektiteErrorf(errors.WasmError, "function '%s' must have one return value but it has %d", def.Name(), len(def.ResultTypes()))
	}
	if expectedReturnType != def.ResultTypes()[0] {
		return errors.NewTektiteErrorf(errors.WasmError, "function '%s' as defined in the json metadata would require a wasm function with return type %s. But the actual wasm function has return type %s",
			def.Name(), api.ValueTypeName(expectedReturnType), api.ValueTypeName(def.ResultTypes()[0]))
	}
	return nil
}

func (r *RegisteredModule) close() {
	for _, wrapper := range r.moduleInstances {
		if err := wrapper.instance.Close(context.Background()); err != nil {
			log.Warnf("failed to close wasm instance: %v", err)
		}
	}
	if err := r.compiledModule.Close(context.Background()); err != nil {
		log.Warnf("failed to close wasm compiled module: %v", err)
	}
}

func wasmTypeForTektiteType(tt types.ColumnType) api.ValueType {
	switch tt.ID() {
	case types.ColumnTypeIDInt, types.ColumnTypeIDTimestamp, types.ColumnTypeIDString, types.ColumnTypeIDBytes, types.ColumnTypeIDDecimal:
		return api.ValueTypeI64
	case types.ColumnTypeIDFloat:
		return api.ValueTypeF64
	case types.ColumnTypeIDBool:
		return api.ValueTypeI32
	default:
		panic("unexpected type")
	}
}

type Invoker struct {
	meta   expr.FunctionMetadata
	mod    *modWrapper
	f      api.Function
	malloc api.Function
	free   api.Function
}

func (ii *Invoker) Invoke(inArgs []any) (any, error) {
	// Module instances do not support concurrent invocation so we lock during invocation
	ii.mod.lock.Lock()
	defer ii.mod.lock.Unlock()
	ctx := context.Background()
	args := make([]uint64, 0, len(inArgs))
	for i, inArg := range inArgs {
		argType := ii.meta.ParamTypes[i]
		switch argType.ID() {
		case types.ColumnTypeIDInt:
			args = append(args, uint64(inArg.(int64)))
		case types.ColumnTypeIDFloat:
			v := math.Float64bits(inArg.(float64))
			args = append(args, v)
		case types.ColumnTypeIDBool:
			b := inArg.(bool)
			if b {
				args = append(args, uint64(1))
			} else {
				args = append(args, uint64(0))
			}
		case types.ColumnTypeIDDecimal:
			d := inArg.(types.Decimal)
			// Currently we pass Decimals as strings
			arg, freeFunc, err := ii.prepareBytesArg(common.StringToByteSliceZeroCopy(d.String()), ctx)
			if err != nil {
				return nil, err
			}
			if freeFunc != nil {
				//goland:noinspection GoDeferInLoop
				defer freeFunc()
			}
			args = append(args, arg)
		case types.ColumnTypeIDString:
			val := inArg.(string)
			arg, freeFunc, err := ii.prepareBytesArg(common.StringToByteSliceZeroCopy(val), ctx)
			if err != nil {
				return nil, err
			}
			if freeFunc != nil {
				//goland:noinspection GoDeferInLoop
				defer freeFunc()
			}
			args = append(args, arg)
		case types.ColumnTypeIDBytes:
			var val []byte
			if inArg != nil {
				val = inArg.([]byte)
			}
			arg, freeFunc, err := ii.prepareBytesArg(val, ctx)
			if err != nil {
				return nil, err
			}
			if freeFunc != nil {
				//goland:noinspection GoDeferInLoop
				defer freeFunc()
			}
			args = append(args, arg)
		case types.ColumnTypeIDTimestamp:
			val := inArg.(types.Timestamp).Val
			args = append(args, uint64(val))
		default:
			panic("unexpected type")
		}
	}

	resArr, err := ii.f.Call(ctx, args...)
	if err != nil {
		return nil, errors.NewTektiteErrorf(errors.WasmError, "failed to call wasm function %s : %v",
			ii.f.Definition().Name(), err)
	}

	res := resArr[0]
	switch ii.meta.ReturnType.ID() {
	case types.ColumnTypeIDInt:
		return int64(res), nil
	case types.ColumnTypeIDFloat:
		return math.Float64frombits(res), nil
	case types.ColumnTypeIDBool:
		return res == 1, nil
	case types.ColumnTypeIDDecimal:
		// Decimals are returned as strings
		bytes, freeFunc, err := ii.decodeBytesReturn(res, ctx)
		if err != nil {
			return nil, err
		}
		if freeFunc != nil {
			defer freeFunc()
		}
		decType := ii.meta.ReturnType.(*types.DecimalType)
		return types.NewDecimalFromString(common.ByteSliceToStringZeroCopy(bytes), decType.Precision, decType.Scale)
	case types.ColumnTypeIDString:
		bytes, freeFunc, err := ii.decodeBytesReturn(res, ctx)
		if err != nil {
			return nil, err
		}
		if freeFunc != nil {
			defer freeFunc()
		}
		return common.ByteSliceToStringZeroCopy(bytes), nil
	case types.ColumnTypeIDBytes:
		bytes, freeFunc, err := ii.decodeBytesReturn(res, ctx)
		if err != nil {
			return nil, err
		}
		if freeFunc != nil {
			defer freeFunc()
		}
		return bytes, nil
	case types.ColumnTypeIDTimestamp:
		return types.NewTimestamp(int64(res)), nil
	default:
		panic("unexpected type")
	}
}

func (ii *Invoker) prepareBytesArg(val []byte, ctx context.Context) (uint64, func(), error) {
	lv := len(val)
	if lv > 0 {
		results, err := ii.malloc.Call(ctx, uint64(lv))
		if err != nil {
			return 0, nil, err
		}
		memPtr := results[0]
		if !ii.mod.instance.Memory().Write(uint32(memPtr), val) {
			return 0, nil, errors.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
				memPtr, lv, ii.mod.instance.Memory().Size())
		}
		arg := memPtr<<32 | uint64(lv)
		freeFunc := func() {
			if _, err := ii.free.Call(ctx, memPtr); err != nil {
				log.Warnf("failed to free memory: %v", err)
			}
		}
		return arg, freeFunc, nil
	} else {
		return 0, nil, nil
	}
}

func (ii *Invoker) decodeBytesReturn(res uint64, ctx context.Context) ([]byte, func(), error) {
	resPtr := uint32(res >> 32)
	resSize := uint32(res)
	var bytes []byte
	if resSize > 0 {
		var ok bool
		bytes, ok = ii.mod.instance.Memory().Read(resPtr, resSize)
		if !ok {
			return nil, nil, errors.Errorf("Memory.Read(%d, %d) out of range of memory size %d",
				resPtr, resSize, ii.mod.instance.Memory().Size())
		}
	}
	var freeFunc func()
	if resPtr != 0 {
		freeFunc = func() {
			_, err := ii.free.Call(ctx, uint64(resPtr))
			if err != nil {
				log.Warnf("failed to free memory: %v", err)
			}
		}
	}
	return bytes, freeFunc, nil
}

func createModuleKeys(modName string) ([]byte, []byte) {
	return []byte(fmt.Sprintf("%s.%s", moduleObjectStorePrefix, modName)),
		[]byte(fmt.Sprintf("%s-json.%s", moduleObjectStorePrefix, modName))
}

func logString(_ context.Context, m api.Module, offset, byteCount uint32) {
	buf, ok := m.Memory().Read(offset, byteCount)
	if !ok {
		panic(fmt.Sprintf("Memory.Read(%d, %d) out of range", offset, byteCount))
	}
	log.Infof("log from wasm module: %s", string(buf))
}

type InvokerFactory struct {
	ModManager *ModuleManager
}

func (w *InvokerFactory) GetFunctionMetadata(functionName string) (expr.FunctionMetadata, bool) {
	meta, ok, _ := w.ModManager.GetFunctionMetadata(functionName)
	if ok {
		return meta, true
	}
	return expr.FunctionMetadata{}, false
}

func (w *InvokerFactory) CreateExternalInvoker(fullFunctionName string) (expr.ExternalInvoker, error) {
	return w.ModManager.CreateInvoker(fullFunctionName)
}
