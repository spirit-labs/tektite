package expr

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/types"
)

type ExternalFunction struct {
	functionName   string
	operands       []Expression
	paramTypes     []types.ColumnType
	returnType     types.ColumnType
	invokerFactory ExternalInvokerFactory
	grLocal        common.GRLocal
}

func NewExternalFunction(operands []Expression, desc *parser.FunctionExprDesc, invokerFactory ExternalInvokerFactory) (*ExternalFunction, error) {
	funcMetadata, ok := invokerFactory.GetFunctionMetadata(desc.FunctionName)
	if !ok {
		// shouldn't happen is we check if function exists in the parser already
		panic("cannot find function metadata")
	}
	paramTypes := make([]types.ColumnType, len(operands))
	for i, arg := range operands {
		paramTypes[i] = arg.ResultType()
	}
	same := false
	if len(paramTypes) == len(funcMetadata.ParamTypes) {
		same = true
		for i, paramType := range paramTypes {
			actualType := funcMetadata.ParamTypes[i]
			if !types.ColumnTypesEqual(paramType, actualType) {
				same = false
				break
			}
		}
	}
	if !same {
		return nil, desc.ErrorAtPosition("function '%s' requires arguments of types [%s] but receives argument types [%s]",
			desc.FunctionName, types.ColumnTypesToString(funcMetadata.ParamTypes), types.ColumnTypesToString(paramTypes))
	}

	return &ExternalFunction{
		functionName:   desc.FunctionName,
		operands:       operands,
		paramTypes:     paramTypes,
		returnType:     funcMetadata.ReturnType,
		invokerFactory: invokerFactory,
		grLocal:        common.NewGRLocal(),
	}, nil
}

func (e *ExternalFunction) EvalInt(rowIndex int, batch *evbatch.Batch) (int64, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return r.(int64), false, nil
}

func (e *ExternalFunction) EvalFloat(rowIndex int, batch *evbatch.Batch) (float64, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return 0, false, err
	}
	if null {
		return 0, true, nil
	}
	return r.(float64), false, nil
}

func (e *ExternalFunction) EvalBool(rowIndex int, batch *evbatch.Batch) (bool, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return false, false, err
	}
	if null {
		return false, true, nil
	}
	return r.(bool), false, nil
}

func (e *ExternalFunction) EvalDecimal(rowIndex int, batch *evbatch.Batch) (types.Decimal, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return types.Decimal{}, false, err
	}
	if null {
		return types.Decimal{}, true, nil
	}
	return r.(types.Decimal), false, nil
}

func (e *ExternalFunction) EvalString(rowIndex int, batch *evbatch.Batch) (string, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return "", false, err
	}
	if null {
		return "", true, nil
	}
	return r.(string), false, nil
}

func (e *ExternalFunction) EvalBytes(rowIndex int, batch *evbatch.Batch) ([]byte, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return nil, false, err
	}
	if null {
		return nil, true, nil
	}
	return r.([]byte), false, nil
}

func (e *ExternalFunction) EvalTimestamp(rowIndex int, batch *evbatch.Batch) (types.Timestamp, bool, error) {
	r, null, err := e.eval(rowIndex, batch)
	if err != nil {
		return types.Timestamp{}, false, err
	}
	if null {
		return types.Timestamp{}, true, nil
	}
	return r.(types.Timestamp), false, nil
}

func (e *ExternalFunction) ResultType() types.ColumnType {
	return e.returnType
}

func (e *ExternalFunction) eval(rowIndex int, batch *evbatch.Batch) (any, bool, error) {
	args := make([]any, len(e.operands))
	for i, operand := range e.operands {
		var v any
		var null bool
		var err error
		switch operand.ResultType().ID() {
		case types.ColumnTypeIDInt:
			v, null, err = operand.EvalInt(rowIndex, batch)
		case types.ColumnTypeIDFloat:
			v, null, err = operand.EvalFloat(rowIndex, batch)
		case types.ColumnTypeIDBool:
			v, null, err = operand.EvalBool(rowIndex, batch)
		case types.ColumnTypeIDDecimal:
			v, null, err = operand.EvalDecimal(rowIndex, batch)
		case types.ColumnTypeIDString:
			v, null, err = operand.EvalString(rowIndex, batch)
		case types.ColumnTypeIDBytes:
			v, null, err = operand.EvalBytes(rowIndex, batch)
		case types.ColumnTypeIDTimestamp:
			v, null, err = operand.EvalTimestamp(rowIndex, batch)
		default:
			panic("unexpected column type")
		}
		if err != nil {
			return nil, false, err
		}
		if null {
			return nil, true, nil
		}
		args[i] = v
	}
	invoker, err := e.getInvoker()
	if err != nil {
		return 0, false, err
	}
	r, err := invoker.Invoke(args)
	if err != nil {
		return 0, false, err
	}
	return r, false, nil
}

func (e *ExternalFunction) getInvoker() (ExternalInvoker, error) {
	// Creating an invoker has some cost, so we cache them on per goroutine level.
	// Also, invokers cannot be used concurrently so safe to multiple times by same goroutine, and this avoids us having
	// to provide locking
	var invoker ExternalInvoker
	o, ok := e.grLocal.Get()
	if !ok {
		var err error
		invoker, err = e.invokerFactory.CreateExternalInvoker(e.functionName)
		if err != nil {
			return nil, err
		}
		e.grLocal.Set(invoker)
	} else {
		invoker = o.(ExternalInvoker)
	}
	return invoker, nil
}
