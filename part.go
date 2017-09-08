package sqrl

import (
	"bytes"
	"fmt"
)

type part struct {
	pred interface{}
	args []interface{}
}

func newPart(pred interface{}, args ...interface{}) sqlWriter {
	return &part{pred, args}
}

func (p part) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	switch pred := p.pred.(type) {
	case sqlWriter:
		args, err = pred.toSQL(b)
	case string:
		b.WriteString(pred)
		args = p.args
	default:
		err = fmt.Errorf("expected string or Sqlizer, not %T", pred)
	}
	return
}

func appendToSQL(parts []sqlWriter, b *bytes.Buffer, sep string, args []interface{}) ([]interface{}, error) {
	for i, p := range parts {
		if i > 0 {
			if _, err := b.WriteString(sep); err != nil {
				return nil, err
			}
		}

		partArgs, err := p.toSQL(b)
		if err != nil {
			return nil, err
		}

		if len(partArgs) != 0 {
			args = append(args, partArgs...)
		}
	}
	return args, nil
}
