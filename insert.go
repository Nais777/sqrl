package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
)

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder struct {
	StatementBuilderType

	prefixes []expr
	options  []string
	into     string
	columns  []string
	values   [][]interface{}
	suffixes []expr
}

// NewInsertBuilder creates new instance of InsertBuilder
func NewInsertBuilder(b StatementBuilderType) *InsertBuilder {
	return &InsertBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *InsertBuilder) RunWith(runner BaseRunner) *InsertBuilder {
	b.runWith = runner
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *InsertBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// Exec builds and Execs the query with the Runner set by RunWith using given context.
func (b *InsertBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWithContext(ctx, b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *InsertBuilder) Query() (*sql.Rows, error) {
	return b.QueryContext(context.Background())
}

// QueryContext builds and runs the query using given context and Query command.
func (b *InsertBuilder) QueryContext(ctx context.Context) (*sql.Rows, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWithContext(ctx, b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *InsertBuilder) QueryRow() RowScanner {
	return b.QueryRowContext(context.Background())
}

// QueryRowContext builds and runs the query using given context.
func (b *InsertBuilder) QueryRowContext(ctx context.Context) RowScanner {
	if b.runWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := b.runWith.(QueryRowerContext)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunnerContext}
	}
	return QueryRowWithContext(ctx, queryRower, b)
}

// Scan is a shortcut for QueryRow().Scan.
func (b *InsertBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *InsertBuilder) PlaceholderFormat(f PlaceholderFormat) *InsertBuilder {
	b.placeholderFormat = f
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *InsertBuilder) ToSQL() (sqlStr string, args []interface{}, err error) {
	if len(b.into) == 0 {
		err = errors.New("insert statements must specify a table")
		return
	}
	if len(b.values) == 0 {
		err = errors.New("insert statements must have at least one set of values")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = appendExpressionsToSQL(sql, b.prefixes, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("INSERT ")

	if len(b.options) > 0 {
		sql.WriteString(strings.Join(b.options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(b.into)
	sql.WriteString(" ")

	if len(b.columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(b.columns, ","))
		sql.WriteString(") ")
	}

	sql.WriteString("VALUES ")

	for r, row := range b.values {
		if r > 0 {
			sql.WriteString(",")
		}

		sql.WriteString("(")

		for v, val := range row {
			if v > 0 {
				sql.WriteString(",")
			}

			switch typedVal := val.(type) {
			case sqlWriter:
				var valArgs []interface{}

				_, valArgs, err = typedVal.toSQL(sql)
				if err != nil {
					return
				}

				if len(valArgs) > 0 {
					args = append(args, valArgs...)
				}
			default:
				sql.WriteString("?")
				args = append(args, val)
			}
		}

		sql.WriteString(")")
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = appendExpressionsToSQL(sql, b.suffixes, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Prefix adds an expression to the beginning of the query
func (b *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Options adds keyword options before the INTO clause of the query.
func (b *InsertBuilder) Options(options ...string) *InsertBuilder {
	b.options = append(b.options, options...)
	return b
}

// Into sets the INTO clause of the query.
func (b *InsertBuilder) Into(into string) *InsertBuilder {
	b.into = into
	return b
}

// Columns adds insert columns to the query.
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Values adds a single row's values to the query.
func (b *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	b.values = append(b.values, values)
	return b
}

// Suffix adds an expression to the end of the query
func (b *InsertBuilder) Suffix(sql string, args ...interface{}) *InsertBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))
	return b
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b *InsertBuilder) SetMap(clauses map[string]interface{}) *InsertBuilder {
	b.columns = make([]string, len(clauses))
	vals := make([]interface{}, len(clauses))

	i := 0
	for col, val := range clauses {
		b.columns[i] = col
		vals[i] = val
		i++
	}

	b.values = [][]interface{}{vals}

	return b
}
