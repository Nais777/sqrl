package bufRewrite

import "fmt"

type wherePart part

func newWherePart(pred interface{}, args ...interface{}) sqlWriter {
	return &wherePart{pred: pred, args: args}
}

func (p wherePart) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case sqlWriter:
		return pred.toSQL(b)
	case map[string]interface{}:
		return Eq(pred).toSQL(b)
	case string:
		_, err = b.WriteString(pred)
		written = true
		args = p.args
	default:
		err = fmt.Errorf("expected string-keyed map or string, not %T", pred)
	}
	return
}
