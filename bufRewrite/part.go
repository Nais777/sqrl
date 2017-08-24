package bufRewrite

import "fmt"

type part struct {
	pred interface{}
	args []interface{}
}

func newPart(pred interface{}, args ...interface{}) sqlWriter {
	return &part{pred, args}
}

func (p part) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case sqlWriter:
		written, args, err = pred.toSQL(b)
	case string:
		b.WriteString(pred)
		written = true
		args = p.args
	default:
		err = fmt.Errorf("expected string or Sqlizer, not %T", pred)
	}
	return
}
