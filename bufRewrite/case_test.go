package bufRewrite

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkCaseToSQL(b *testing.B) {
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	for n := 0; n < b.N; n++ {
		caseStmt.toSQL(&bytes.Buffer{})
	}
}

func TestCaseWithVal(t *testing.T) {
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	b := &bytes.Buffer{}
	written, args, err := caseStmt.toSQL(b)

	assert.NoError(t, err)
	assert.True(t, written)

	expectedSQL := "CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE ? " +
		"END"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{"big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	caseStmt := Case("? > ?", 10, 5).
		When("true", "'T'")

	b := &bytes.Buffer{}
	written, args, err := caseStmt.toSQL(b)

	assert.NoError(t, err)
	assert.True(t, written)

	expectedSQL := "CASE ? > ? " +
		"WHEN true THEN 'T' " +
		"END"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{10, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	caseStmt := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	b := &bytes.Buffer{}
	written, args, err := caseStmt.toSQL(b)

	assert.NoError(t, err)
	assert.True(t, written)

	expectedSQL := "CASE " +
		"WHEN x = ? THEN x is zero " +
		"WHEN x > ? THEN CONCAT('x is greater than ', ?) " +
		"END"

	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{0, 1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithExpr(t *testing.T) {
	caseStmt := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	b := &bytes.Buffer{}
	written, args, err := caseStmt.toSQL(b)

	assert.NoError(t, err)
	assert.True(t, written)

	expectedSQL := "CASE x = ? " +
		"WHEN true THEN ? " +
		"ELSE 42 " +
		"END"

	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{true, "it's true!"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	caseStmt := Case("something").
		Else("42")

	_, _, err := caseStmt.toSQL(&bytes.Buffer{})

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}
