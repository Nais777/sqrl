package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

type setClause struct {
	column string
	value  interface{}
}

// Builder

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder struct {
	StatementBuilderType

	prefixes   []expr
	table      string
	setClauses map[string]interface{}
	whereParts []sqlWriter
	orderBys   []string

	limit       uint64
	limitValid  bool
	offset      uint64
	offsetValid bool

	suffixes []expr
}

// NewUpdateBuilder creates new instance of UpdateBuilder
func NewUpdateBuilder(b StatementBuilderType) *UpdateBuilder {
	return &UpdateBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *UpdateBuilder) RunWith(runner BaseRunner) *UpdateBuilder {
	b.runWith = runner
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *UpdateBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// ExecContext builds and Execs the query with the Runner set by RunWith using given context.
func (b *UpdateBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWithContext(ctx, b.runWith, b)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) *UpdateBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *UpdateBuilder) ToSQL() (sqlStr string, args []interface{}, err error) {
	if len(b.table) == 0 {
		err = errors.New("update statements must specify a table")
		return
	}
	if len(b.setClauses) == 0 {
		err = errors.New("update statements must have at least one Set clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = appendExpressionsToSQL(sql, b.prefixes, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("UPDATE ")
	sql.WriteString(b.table)

	sql.WriteString(" SET ")
	i := 0
	for column, value := range b.setClauses {
		if i > 0 {
			sql.WriteString(", ")
		}

		sql.WriteString(column + " = ")

		switch typedVal := value.(type) {
		case sqlWriter:
			var valArgs []interface{}
			_, valArgs, err = typedVal.toSQL(sql)
			if err != nil {
				return
			}
			if len(valArgs) != 0 {
				args = append(args, valArgs...)
			}
		default:
			sql.WriteString("?")
			args = append(args, typedVal)
		}

		i++
	}

	if len(b.whereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSQL(b.whereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(b.orderBys, ", "))
	}

	if b.limitValid {
		sql.WriteString(" LIMIT ")
		sql.WriteString(strconv.FormatUint(b.limit, 10))
	}

	if b.offsetValid {
		sql.WriteString(" OFFSET ")
		sql.WriteString(strconv.FormatUint(b.offset, 10))
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = appendExpressionsToSQL(sql, b.suffixes, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}

// SQL methods

// Prefix adds an expression to the beginning of the query
func (b *UpdateBuilder) Prefix(sql string, args ...interface{}) *UpdateBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Table sets the table to be updateb.
func (b *UpdateBuilder) Table(table string) *UpdateBuilder {
	b.table = table
	return b
}

// Set adds SET clauses to the query.
func (b *UpdateBuilder) Set(column string, value interface{}) *UpdateBuilder {
	if b.setClauses == nil {
		b.setClauses = map[string]interface{}{column: value}
	} else {
		b.setClauses[column] = value
	}

	return b
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b *UpdateBuilder) SetMap(clauses map[string]interface{}) *UpdateBuilder {
	if b.setClauses == nil {
		b.setClauses = clauses
	} else {
		for k, v := range clauses {
			b.setClauses[k] = v
		}
	}

	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b *UpdateBuilder) Where(pred interface{}, args ...interface{}) *UpdateBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b *UpdateBuilder) OrderBy(orderBys ...string) *UpdateBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Limit sets a LIMIT clause on the query.
func (b *UpdateBuilder) Limit(limit uint64) *UpdateBuilder {
	b.limit = limit
	b.limitValid = true
	return b
}

// Offset sets a OFFSET clause on the query.
func (b *UpdateBuilder) Offset(offset uint64) *UpdateBuilder {
	b.offset = offset
	b.offsetValid = true
	return b
}

// Suffix adds an expression to the end of the query
func (b *UpdateBuilder) Suffix(sql string, args ...interface{}) *UpdateBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))

	return b
}
