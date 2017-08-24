package bufRewrite

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"reflect"
)

type expr struct {
	sql  string
	args []interface{}
}

// Expr builds value expressions for InsertBuilder and UpdateBuilder.
//
// Ex:
//     .Values(Expr("FROM_UNIXTIME(?)", t))
func Expr(sql string, args ...interface{}) expr {
	return expr{sql: sql, args: args}
}

func (e expr) toSQL(b sqlBuffer) (bool, []interface{}, error) {
	if !hasSQLWriter(e.args) {
		b.WriteString(e.sql)
		return true, e.args, nil
	}

	args := make([]interface{}, 0, len(e.args))
	sql, err := replacePlaceholders(e.sql, func(buf *bytes.Buffer, i int) error {
		if i > len(e.args) {
			buf.WriteRune('?')
			return nil
		}
		switch arg := e.args[i-1].(type) {
		case sqlWriter:
			_, vs, err := arg.toSQL(buf)
			if err != nil {
				return err
			}
			args = append(args, vs...)
		default:
			args = append(args, arg)
			buf.WriteRune('?')
		}
		return nil
	})

	if err != nil {
		return false, nil, err
	}

	b.WriteString(sql)

	return true, args, nil
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Eq{"id": 1})
type Eq map[string]interface{}

func (eq Eq) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	return equalityToSQL(eq, b, false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(NotEq{"id": 1}) == "id <> 1"
type Neq map[string]interface{}

func (neq Neq) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	return equalityToSQL(neq, b, true)
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Lt{"id": 1})
type Lt map[string]interface{}

func (lt Lt) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	return comparisonToSQL(lt, b, false, false)
}

// Lte is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(LtOrEq{"id": 1}) == "id <= 1"
type Lte map[string]interface{}

func (lte Lte) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	return comparisonToSQL(lte, b, false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Gt{"id": 1}) == "id > 1"
type Gt map[string]interface{}

func (gt Gt) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	return comparisonToSQL(gt, b, true, false)
}

// Gte is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(GtOrEq{"id": 1}) == "id >= 1"
type Gte map[string]interface{}

func (gte Gte) toSQL(b sqlBuffer) (written bool, args []interface{}, err error) {
	return comparisonToSQL(gte, b, true, true)
}

func equalityToSQL(m map[string]interface{}, b sqlBuffer, useNotOpr bool) (written bool, args []interface{}, err error) {
	var (
		equalOpr = "="
		inOpr    = "IN"
		nullOpr  = "IS"
	)

	if useNotOpr {
		equalOpr = "<>"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
	}

	first := true
	for key, val := range m {
		if !first {
			b.WriteString(" AND ")
		}

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			b.WriteString(key)
			b.WriteByte(' ')
			b.WriteString(nullOpr)
			b.WriteString(" NULL")
		} else {
			valVal := reflect.ValueOf(val)
			if valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice {
				if valVal.Len() == 0 {
					err = errors.New("equality condition must contain at least one paramater")
					return
				}

				for i := 0; i < valVal.Len(); i++ {
					args = append(args, valVal.Index(i).Interface())
				}

				b.WriteString(key)
				b.WriteByte(' ')
				b.WriteString(inOpr)
				b.WriteString(" (")
				b.WriteString(Placeholders(valVal.Len()))
				b.WriteString(")")
			} else {
				b.WriteString(key)
				b.WriteByte(' ')
				b.WriteString(equalOpr)
				b.WriteString(" ?")

				args = append(args, val)
			}
		}

		if first {
			first = false
		}
	}

	written = true

	return
}

func comparisonToSQL(m map[string]interface{}, b sqlBuffer, opposite, orEq bool) (written bool, args []interface{}, err error) {
	opr := "<"

	if opposite {
		opr = ">"
	}

	if orEq {
		opr += "="
	}

	first := true
	for key, val := range m {
		if !first {
			b.WriteString(" AND ")
		}

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = errors.New("cannot use null with less than or greater than operators")
			return
		}

		valVal := reflect.ValueOf(val)
		if valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice {
			err = errors.New("cannot use array or slice with less than or greater than operators")
			return
		}

		b.WriteString(key)
		b.WriteByte(' ')
		b.WriteString(opr)
		b.WriteString(" ?")

		args = append(args, val)

		if first {
			first = false
		}
	}

	written = true

	return
}

func hasSQLWriter(args []interface{}) bool {
	for _, arg := range args {
		_, ok := arg.(sqlWriter)
		if ok {
			return true
		}
	}
	return false
}
