package queryexpr

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/lmika/dynamo-browse/internal/common/sliceutils"
	"github.com/lmika/dynamo-browse/internal/dynamo-browse/models"
	"github.com/pkg/errors"
)

func (a *astFunctionCall) evalToIR(ctx *evalContext, info *models.TableInfo) (irAtom, error) {
	callerIr, err := a.Caller.evalToIR(ctx, info)
	if err != nil {
		return nil, err
	}
	if !a.IsCall {
		return callerIr, nil
	}

	nameIr, isNameIr := callerIr.(nameIRAtom)
	if !isNameIr || nameIr.keyName() == "" {
		return nil, OperandNotANameError("")
	}

	irNodes, err := sliceutils.MapWithError(a.Args, func(x *astExpr) (irAtom, error) { return x.evalToIR(ctx, info) })
	if err != nil {
		return nil, err
	}

	// Special handling of functions that have IR nodes
	switch nameIr.keyName() {
	case "size":
		if len(irNodes) != 1 {
			return nil, InvalidArgumentNumberError{Name: "size", Expected: 1, Actual: len(irNodes)}
		}
		name, isName := irNodes[0].(nameIRAtom)
		if !isName {
			return nil, OperandNotANameError(a.Args[0].String())
		}
		return irSizeFn{name}, nil
	}

	builtinFn, hasBuiltin := nativeFuncs[nameIr.keyName()]
	if !hasBuiltin {
		return nil, UnrecognisedFunctionError{Name: nameIr.keyName()}
	}

	// Normal functions which are evaluated to regular values
	irValues, err := sliceutils.MapWithError(irNodes, func(a irAtom) (exprValue, error) {
		v, isV := a.(valueIRAtom)
		if !isV {
			return nil, errors.New("cannot use value")
		}
		return v.exprValue(), nil
	})
	if err != nil {
		return nil, err
	}

	cCtx := context.WithValue(context.Background(), timeSourceContextKey, ctx.timeSource)
	cCtx = context.WithValue(cCtx, currentResultSetContextKey, ctx.ctxResultSet)
	cCtx = context.WithValue(cCtx, currentPasteboardControllerKey, ctx.pasteboardController)
	val, err := builtinFn(cCtx, irValues)
	if err != nil {
		return nil, err
	}

	return irValue{value: val}, nil
}

func (a *astFunctionCall) evalItem(ctx *evalContext, item models.Item) (exprValue, error) {
	if !a.IsCall {
		return a.Caller.evalItem(ctx, item)
	}

	name, isName := a.Caller.unqualifiedName()
	if !isName {
		return nil, OperandNotANameError(a.Args[0].String())
	}
	fn, isFn := nativeFuncs[name]
	if !isFn {
		return nil, UnrecognisedFunctionError{Name: name}
	}

	args, err := sliceutils.MapWithError(a.Args, func(a *astExpr) (exprValue, error) {
		return a.evalItem(ctx, item)
	})
	if err != nil {
		return nil, err
	}

	cCtx := context.WithValue(context.Background(), timeSourceContextKey, ctx.timeSource)
	cCtx = context.WithValue(cCtx, currentResultSetContextKey, ctx.ctxResultSet)
	cCtx = context.WithValue(cCtx, currentPasteboardControllerKey, ctx.pasteboardController)
	return fn(cCtx, args)
}

func (a *astFunctionCall) canModifyItem(ctx *evalContext, item models.Item) bool {
	// TODO: Should a function vall return an item?
	if a.IsCall {
		return false
	}
	return a.Caller.canModifyItem(ctx, item)
}

func (a *astFunctionCall) setEvalItem(ctx *evalContext, item models.Item, value exprValue) error {
	// TODO: Should a function vall return an item?
	if a.IsCall {
		return PathNotSettableError{}
	}
	return a.Caller.setEvalItem(ctx, item, value)
}

func (a *astFunctionCall) deleteAttribute(ctx *evalContext, item models.Item) error {
	// TODO: Should a function vall return an item?
	if a.IsCall {
		return PathNotSettableError{}
	}
	return a.Caller.deleteAttribute(ctx, item)
}

func (a *astFunctionCall) String() string {
	var sb strings.Builder

	sb.WriteString(a.Caller.String())
	if a.IsCall {
		sb.WriteRune('(')
		for i, q := range a.Args {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(q.String())
		}
		sb.WriteRune(')')
	}
	return sb.String()
}

type irSizeFn struct {
	arg nameIRAtom
}

func (i irSizeFn) calcQueryForScan(info *models.TableInfo) (expression.ConditionBuilder, error) {
	return expression.ConditionBuilder{}, errors.New("cannot run as scan")
}

func (i irSizeFn) calcOperand(info *models.TableInfo) expression.OperandBuilder {
	name := i.arg.calcName(info)
	return name.Size()
}

type irRangeFn struct {
	fromIdx int64
	toIdx   int64
}

func (i irRangeFn) calcQueryForScan(info *models.TableInfo) (expression.ConditionBuilder, error) {
	return expression.ConditionBuilder{}, errors.New("cannot run as scan")
}

func (i irRangeFn) calcGoValues(info *models.TableInfo) ([]any, error) {
	xs := make([]any, 0)
	for x := i.fromIdx; x <= i.toIdx; x++ {
		xs = append(xs, x)
	}
	return xs, nil
}

type multiValueFnResult struct {
	items []any
}

func (i multiValueFnResult) calcQueryForScan(info *models.TableInfo) (expression.ConditionBuilder, error) {
	return expression.ConditionBuilder{}, errors.New("cannot run as scan")
}

func (i multiValueFnResult) calcGoValues(info *models.TableInfo) ([]any, error) {
	return i.items, nil
}
