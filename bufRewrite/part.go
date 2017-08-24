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

func appendToSQL(parts []sqlWriter, b sqlBuffer, sep string, args []interface{}) ([]interface{}, error) {
	sepWritten := false
	for i, p := range parts {
		if i > 0 && !sepWritten {
			if _, err := b.WriteString(sep); err != nil {
				return nil, err
			}

			sepWritten = true
		}

		written, partArgs, err := p.toSQL(b)
		if err != nil {
			return nil, err
		} else if !written {
			continue
		}

		sepWritten = false

		if len(partArgs) != 0 {
			args = append(args, partArgs...)
		}
	}
	return args, nil
}
