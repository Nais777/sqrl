package sqrl

import (
	"bytes"
	"fmt"
)

type wherePart part

func newWherePart(pred interface{}, args ...interface{}) sqlWriter {
	if pred == nil {
		return nil
	}

	return &wherePart{pred: pred, args: args}
}

func (p wherePart) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	switch pred := p.pred.(type) {
	case sqlWriter:
		return pred.toSQL(b)
	case map[string]interface{}:
		return Eq(pred).toSQL(b)
	case string:
		_, err = b.WriteString(pred)
		args = p.args
	default:
		err = fmt.Errorf("expected string-keyed map or string, not %T", pred)
	}
	return
}
