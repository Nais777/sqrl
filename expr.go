package sqrl

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

func (e expr) toSQL(b *bytes.Buffer) ([]interface{}, error) {
	if !hasSQLWriter(e.args) {
		b.WriteString(e.sql)
		return e.args, nil
	}

	args := make([]interface{}, 0, len(e.args))
	sql, err := replacePlaceholders(e.sql, func(buf *bytes.Buffer, i int) error {
		if i > len(e.args) {
			buf.WriteRune('?')
			return nil
		}
		switch arg := e.args[i-1].(type) {
		case sqlWriter:
			vs, err := arg.toSQL(buf)
			if err != nil {
				return err
			}

			if len(vs) != 0 {
				args = append(args, vs...)
			}
		default:
			args = append(args, arg)
			buf.WriteRune('?')
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	b.WriteString(sql)

	return args, nil
}

func appendExpressionsToSQL(b *bytes.Buffer, exprs []expr, sep string, args []interface{}) ([]interface{}, error) {
	for i, e := range exprs {
		if i > 0 {
			_, err := b.WriteString(sep)
			if err != nil {
				return nil, err
			}
		}
		_, err := b.WriteString(e.sql)
		if err != nil {
			return nil, err
		}

		if len(e.args) != 0 {
			args = append(args, e.args...)
		}
	}
	return args, nil
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Eq{"id": 1})
type Eq map[string]interface{}

func (eq Eq) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	return equalityToSQL(eq, b, false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(NotEq{"id": 1}) == "id <> 1"
type Neq map[string]interface{}

func (neq Neq) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	return equalityToSQL(neq, b, true)
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Lt{"id": 1})
type Lt map[string]interface{}

func (lt Lt) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	return comparisonToSQL(lt, b, false, false)
}

// Lte is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(LtOrEq{"id": 1}) == "id <= 1"
type Lte map[string]interface{}

func (lte Lte) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	return comparisonToSQL(lte, b, false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(Gt{"id": 1}) == "id > 1"
type Gt map[string]interface{}

func (gt Gt) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	return comparisonToSQL(gt, b, true, false)
}

// Gte is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//     .Where(GtOrEq{"id": 1}) == "id >= 1"
type Gte map[string]interface{}

func (gte Gte) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	return comparisonToSQL(gte, b, true, true)
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr  sqlWriter
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//		.Column(Alias(caseStmt, "case_column"))
func Alias(expr sqlWriter, alias string) aliasExpr {
	return aliasExpr{expr, alias}
}

func (e aliasExpr) toSQL(b *bytes.Buffer) (args []interface{}, err error) {
	b.WriteByte('(')
	args, err = e.expr.toSQL(b)
	if err != nil {
		return
	}

	b.WriteString(") AS " + e.alias)

	return
}

type conj []sqlWriter

func (c conj) join(b *bytes.Buffer, sep string) (args []interface{}, err error) {
	b.WriteByte('(')

	var partArgs []interface{}
	for i, s := range c {
		if i > 0 {
			b.WriteString(sep)
		}

		partArgs, err = s.toSQL(b)
		if err != nil {
			return
		}

		args = append(args, partArgs...)
	}
	b.WriteByte(')')

	return
}

// And is syntactic sugar that glues where/having parts with AND clause
// Ex:
//     .Where(And{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type And conj

// ToSql builds the query into a SQL string and bound args.
func (a And) toSQL(b *bytes.Buffer) ([]interface{}, error) {
	return conj(a).join(b, " AND ")
}

// Or is syntactic sugar that glues where/having parts with OR clause
// Ex:
//     .Where(Or{Expr("a > ?", 15), Expr("b < ?", 20), Expr("c is TRUE")})
type Or conj

// ToSql builds the query into a SQL string and bound args.
func (o Or) toSQL(b *bytes.Buffer) ([]interface{}, error) {
	return conj(o).join(b, " OR ")
}

func equalityToSQL(m map[string]interface{}, b *bytes.Buffer, useNotOpr bool) (args []interface{}, err error) {
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
			b.WriteString(key + " " + nullOpr + " NULL")
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

				b.WriteString(key + " " + inOpr + " (" + Placeholders(valVal.Len()) + ")")
			} else {
				b.WriteString(key + " " + equalOpr + " ?")

				args = append(args, val)
			}
		}

		if first {
			first = false
		}
	}

	return
}

func comparisonToSQL(m map[string]interface{}, b *bytes.Buffer, opposite, orEq bool) (args []interface{}, err error) {
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

		b.WriteString(key + " " + opr + " ?")

		args = append(args, val)

		if first {
			first = false
		}
	}

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
