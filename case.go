package sqrl

import "errors"

// sqlizerBuffer is a helper that allows to write many Sqlizers one by one
// without constant checks for errors that may come from Sqlizer
type sqlizerBuffer struct {
	b       sqlBuffer
	written bool
	args    []interface{}
	err     error
}

// WriteSQL converts Sqlizer to SQL strings and writes it to buffer
func (b *sqlizerBuffer) WriteSQL(item sqlWriter) {
	if b.err != nil {
		return
	}

	var args []interface{}
	written, args, err := item.toSQL(b.b)

	if err != nil {
		b.err = err
		return
	}

	if written {
		b.written = written
	}

	b.b.WriteByte(' ')
	b.args = append(b.args, args...)
}

// whenPart is a helper structure to describe SQLs "WHEN ... THEN ..." expression
type whenPart struct {
	when sqlWriter
	then sqlWriter
}

func newWhenPart(when interface{}, then interface{}) whenPart {
	return whenPart{newPart(when), newPart(then)}
}

// CaseBuilder builds SQL CASE construct which could be used as parts of queries.
type CaseBuilder struct {
	whatPart  sqlWriter
	whenParts []whenPart
	elsePart  sqlWriter
}

// toSql implements sqlWriter
func (b *CaseBuilder) toSQL(s sqlBuffer) (bool, []interface{}, error) {
	if len(b.whenParts) == 0 {
		return false, nil, errors.New("case expression must contain at lease one WHEN clause")
	}

	sql := sqlizerBuffer{b: s}

	s.WriteString("CASE ")
	if b.whatPart != nil {
		sql.WriteSQL(b.whatPart)
	}

	for _, p := range b.whenParts {
		s.WriteString("WHEN ")
		sql.WriteSQL(p.when)
		s.WriteString("THEN ")
		sql.WriteSQL(p.then)
	}

	if b.elsePart != nil {
		s.WriteString("ELSE ")
		sql.WriteSQL(b.elsePart)
	}

	s.WriteString("END")

	return sql.written, sql.args, sql.err
}

// what sets optional value for CASE construct "CASE [value] ..."
func (b *CaseBuilder) what(expr interface{}) *CaseBuilder {
	b.whatPart = newPart(expr)
	return b
}

// When adds "WHEN ... THEN ..." part to CASE construct
func (b *CaseBuilder) When(when interface{}, then interface{}) *CaseBuilder {
	b.whenParts = append(b.whenParts, newWhenPart(when, then))
	return b
}

// Else sets optional "ELSE ..." part for CASE construct
func (b *CaseBuilder) Else(expr interface{}) *CaseBuilder {
	b.elsePart = newPart(expr)
	return b

}
