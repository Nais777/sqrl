package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
)

// ErrRunnerNotSet is returned by methods that need a Runner if it isn't set.
var ErrRunnerNotSet = errors.New("cannot run; no Runner set (RunWith)")

// ErrRunnerNotQueryRunner is returned by QueryRow if the RunWith value doesn't implement QueryRower.
var ErrRunnerNotQueryRunner = errors.New("cannot QueryRow; Runner is not a QueryRower")

// ErrRunnerNotQueryRunnerContext is returned by QueryRowContext if the RunWith value doesn't implement QueryRowerContext.
var ErrRunnerNotQueryRunnerContext = errors.New("cannot QueryRow; Runner is not a QueryRowerContext")

// Execer is the interface that wraps the Exec method.
//
// Exec executes the given query as implemented by database/sql.Exec.
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// ExecerContext is the interface that wraps the Exec method.
//
// ExecContext executes the given query using given context as implemented by database/sql.ExecContext.
type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Queryer is the interface that wraps the Query method.
//
// Query executes the given query as implemented by database/sql.Query.
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// QueryerContext is the interface that wraps the Query method.
//
// QueryerContext executes the given query using given context as implemented by database/sql.QueryContext.
type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// QueryRower is the interface that wraps the QueryRow method.
//
// QueryRow executes the given query as implemented by database/sql.QueryRow.
type QueryRower interface {
	QueryRow(query string, args ...interface{}) RowScanner
}

// QueryRowerContext is the interface that wraps the QueryRow method.
//
// QueryRowContext executes the given query using given context as implemented by database/sql.QueryRowContext.
type QueryRowerContext interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) RowScanner
}

// BaseRunner groups the Execer and Queryer interfaces.
type BaseRunner interface {
	Execer
	ExecerContext
	Queryer
	QueryerContext
}

type sqlWriter interface {
	toSQL(b *bytes.Buffer) (args []interface{}, err error)
}

type sqlBuilder interface {
	ToSQL() (string, []interface{}, error)
}

// ExecWith Execs the SQL returned by s with db.
func ExecWith(db Execer, s sqlBuilder) (res sql.Result, err error) {
	query, args, err := s.ToSQL()
	if err != nil {
		return
	}
	return db.Exec(query, args...)
}

// ExecWithContext Execs the SQL returned by s with db.
func ExecWithContext(ctx context.Context, db ExecerContext, s sqlBuilder) (res sql.Result, err error) {
	query, args, err := s.ToSQL()
	if err != nil {
		return
	}
	return db.ExecContext(ctx, query, args...)
}

// QueryWith Querys the SQL returned by s with db.
func QueryWith(db Queryer, s sqlBuilder) (rows *sql.Rows, err error) {
	query, args, err := s.ToSQL()
	if err != nil {
		return
	}
	return db.Query(query, args...)
}

// QueryWithContext Querys the SQL returned by s with db.
func QueryWithContext(ctx context.Context, db QueryerContext, s sqlBuilder) (rows *sql.Rows, err error) {
	query, args, err := s.ToSQL()
	if err != nil {
		return
	}
	return db.QueryContext(ctx, query, args...)
}

// QueryRowWith QueryRows the SQL returned by s with db.
func QueryRowWith(db QueryRower, s sqlBuilder) RowScanner {
	query, args, err := s.ToSQL()
	return &Row{RowScanner: db.QueryRow(query, args...), err: err}
}

// QueryRowWithContext QueryRows the SQL returned by s with db.
func QueryRowWithContext(ctx context.Context, db QueryRowerContext, s sqlBuilder) RowScanner {
	query, args, err := s.ToSQL()
	return &Row{RowScanner: db.QueryRowContext(ctx, query, args...), err: err}
}
