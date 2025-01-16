package queryexpr

import (
	"context"
	"encoding/csv"
	"github.com/lmika/dynamo-browse/internal/common/sliceutils"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
)

type nativeFunc func(ctx context.Context, args []exprValue) (exprValue, error)

var nativeFuncs = map[string]nativeFunc{
	"size": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) != 1 {
			return nil, InvalidArgumentNumberError{Name: "size", Expected: 1, Actual: len(args)}
		}

		var l int
		switch t := args[0].(type) {
		case stringExprValue:
			l = len(t)
		case mappableExprValue:
			l = t.len()
		case slicableExprValue:
			l = t.len()
		default:
			return nil, errors.New("cannot take size of arg")
		}
		return int64ExprValue(l), nil
	},

	"range": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) != 2 {
			return nil, InvalidArgumentNumberError{Name: "range", Expected: 2, Actual: len(args)}
		}

		xVal, isXNum := args[0].(numberableExprValue)
		if !isXNum {
			return nil, InvalidArgumentTypeError{Name: "range", ArgIndex: 0, Expected: "N"}
		}
		yVal, isYNum := args[1].(numberableExprValue)
		if !isYNum {
			return nil, InvalidArgumentTypeError{Name: "range", ArgIndex: 1, Expected: "N"}
		}

		xInt, _ := xVal.asBigFloat().Int64()
		yInt, _ := yVal.asBigFloat().Int64()
		xs := make([]exprValue, 0)
		for x := xInt; x <= yInt; x++ {
			xs = append(xs, int64ExprValue(x))
		}
		return listExprValue(xs), nil
	},

	"csv": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) < 2 {
			return nil, InvalidArgumentNumberError{Name: "csv", Expected: 2, Actual: len(args)}
		}

		filename, ok := args[0].(stringableExprValue)
		if !ok {
			return nil, InvalidArgumentTypeError{Name: "csv", ArgIndex: 0, Expected: "string"}
		}
		fieldName, ok := args[1].(stringExprValue)
		if !ok {
			return nil, InvalidArgumentTypeError{Name: "csv", ArgIndex: 1, Expected: "string"}
		}

		startIndex := 0
		if len(args) == 3 {
			if idx, ok := args[2].(numberableExprValue); ok {
				startIndex = int(idx.asInt())
			}
		}

		f, err := os.Open(filename.asString())
		if err != nil {
			return nil, err
		}
		defer f.Close()

		cr := csv.NewReader(f)

		header, err := cr.Read()
		if err != nil {
			return nil, err
		}

		headerIndex := -1
		for i, h := range header {
			if h == fieldName.asString() {
				headerIndex = i
				break
			}
		}
		if headerIndex == -1 {
			return nil, errors.New("csv header not found")
		}

		count := 0
		newList := make([]exprValue, 0)
		for count < 100 {
			record, err := cr.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			if startIndex > 0 {
				startIndex--
				continue
			}

			newList = append(newList, stringExprValue(record[headerIndex]))
			count += 1
		}

		return listExprValue(newList), nil
	},

	"marked": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) != 1 {
			return nil, InvalidArgumentNumberError{Name: "marked", Expected: 1, Actual: len(args)}
		}

		fieldName, ok := args[0].(stringableExprValue)
		if !ok {
			return nil, InvalidArgumentTypeError{Name: "marked", ArgIndex: 0, Expected: "S"}
		}

		rs := currentResultSetFromContext(ctx)
		if rs == nil {
			return listExprValue{}, nil
		}

		var items = []exprValue{}
		for i, itm := range rs.Items() {
			if !rs.Marked(i) {
				continue
			}

			attr, hasAttr := itm[fieldName.asString()]
			if !hasAttr {
				continue
			}

			exprAttrValue, err := newExprValueFromAttributeValue(attr)
			if err != nil {
				return nil, errors.Wrapf(err, "marked(): item %d, attr %v", i, fieldName.asString())
			}

			items = append(items, exprAttrValue)
		}
		return listExprValue(items), nil
	},

	"mapstr": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) != 2 {
			return nil, InvalidArgumentNumberError{Name: "map", Expected: 2, Actual: len(args)}
		}

		fromList, ok := args[0].(slicableExprValue)
		if !ok {
			return nil, InvalidArgumentTypeError{Name: "map", ArgIndex: 0, Expected: "L"}
		}
		pattern, ok := args[1].(stringableExprValue)
		if !ok {
			return nil, InvalidArgumentTypeError{Name: "map", ArgIndex: 1, Expected: "S"}
		}

		newList := make([]exprValue, fromList.len())
		for i := 0; i < fromList.len(); i++ {
			itm, err := fromList.valueAt(i)
			if err != nil {
				return nil, err
			}

			strItem, ok := itm.(stringableExprValue)
			if !ok {
				newList[i] = strItem
				continue
			}

			newList[i] = stringExprValue(strings.Replace(pattern.asString(), "{}", strItem.asString(), -1))
		}
		return listExprValue(newList), nil
	},

	"pasteboard": func(ctx context.Context, args []exprValue) (exprValue, error) {
		pbc := currentPasteboardControllerFromContext(ctx)
		if pbc == nil {
			return listExprValue{}, nil
		}

		txt, ok := pbc.ReadText()
		if !ok {
			return listExprValue{}, nil
		}

		lines := strings.Split(txt, "\n")
		return listExprValue(sliceutils.Map(lines, func(l string) exprValue { return stringExprValue(l) })), nil
	},

	"_x_now": func(ctx context.Context, args []exprValue) (exprValue, error) {
		now := timeSourceFromContext(ctx).now().Unix()
		return int64ExprValue(now), nil
	},

	"_x_add": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) != 2 {
			return nil, InvalidArgumentNumberError{Name: "_x_add", Expected: 2, Actual: len(args)}
		}

		xVal, isXNum := args[0].(numberableExprValue)
		if !isXNum {
			return nil, InvalidArgumentTypeError{Name: "_x_add", ArgIndex: 0, Expected: "N"}
		}
		yVal, isYNum := args[1].(numberableExprValue)
		if !isYNum {
			return nil, InvalidArgumentTypeError{Name: "_x_add", ArgIndex: 1, Expected: "N"}
		}

		return bigNumExprValue{num: xVal.asBigFloat().Add(xVal.asBigFloat(), yVal.asBigFloat())}, nil
	},

	"_x_concat": func(ctx context.Context, args []exprValue) (exprValue, error) {
		if len(args) != 2 {
			return nil, InvalidArgumentNumberError{Name: "_x_concat", Expected: 2, Actual: len(args)}
		}

		xVal, isXNum := args[0].(stringableExprValue)
		if !isXNum {
			return nil, InvalidArgumentTypeError{Name: "_x_concat", ArgIndex: 0, Expected: "S"}
		}
		yVal, isYNum := args[1].(stringableExprValue)
		if !isYNum {
			return nil, InvalidArgumentTypeError{Name: "_x_concat", ArgIndex: 1, Expected: "S"}
		}

		return stringExprValue(xVal.asString() + yVal.asString()), nil
	},
}
