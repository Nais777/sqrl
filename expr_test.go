package sqrl

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqToSql(t *testing.T) {
	e := Eq{"id": 1}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id = ?"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInToSql(t *testing.T) {
	e := Eq{"id": []int{1, 2, 3}}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id IN (?,?,?)"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNeqToSql(t *testing.T) {
	e := Neq{"id": 1}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id <> ?"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestNeqInToSql(t *testing.T) {
	e := Neq{"id": []int{1, 2, 3}}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestExprNilToSql(t *testing.T) {
	var e sqlWriter
	e = Neq{"name": nil}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSQL := "name IS NOT NULL"
	assert.Equal(t, expectedSQL, b.String())

	e = Eq{"name": nil}

	b = &bytes.Buffer{}
	args, err = e.toSQL(b)
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSQL = "name IS NULL"
	assert.Equal(t, expectedSQL, b.String())
}

func TestLtToSql(t *testing.T) {
	e := Lt{"id": 1}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id < ?"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestLteToSql(t *testing.T) {
	e := Lte{"id": 1}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id <= ?"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtToSql(t *testing.T) {
	e := Gt{"id": 1}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id > ?"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGteToSql(t *testing.T) {
	e := Gte{"id": 1}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	expectedSQL := "id >= ?"
	assert.Equal(t, expectedSQL, b.String())

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestNullTypeString(t *testing.T) {
	var e sqlWriter
	var name sql.NullString

	e = Eq{"name": name}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)
	assert.Empty(t, args)

	assert.Equal(t, "name IS NULL", b.String())

	name.Scan("Name")
	e = Eq{"name": name}

	b = &bytes.Buffer{}
	args, err = e.toSQL(b)
	assert.NoError(t, err)

	assert.Equal(t, []interface{}{"Name"}, args)
	assert.Equal(t, "name = ?", b.String())
}

func TestNullTypeInt64(t *testing.T) {
	var userID sql.NullInt64
	userID.Scan(nil)
	e := Eq{"user_id": userID}

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)
	assert.Empty(t, args)

	assert.Equal(t, "user_id IS NULL", b.String())

	userID.Scan(10)
	e = Eq{"user_id": userID}

	b = &bytes.Buffer{}
	args, err = e.toSQL(b)
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(10)}, args)
	assert.Equal(t, "user_id = ?", b.String())
}

type dummySqlizer int

func (d dummySqlizer) toSQL(b *bytes.Buffer) ([]interface{}, error) {
	b.WriteString("DUMMY(?, ?)")
	return []interface{}{int(d), int(d)}, nil
}

func TestExprSqlizer(t *testing.T) {
	e := Expr("EXISTS(?)", dummySqlizer(42))

	b := &bytes.Buffer{}
	args, err := e.toSQL(b)
	assert.NoError(t, err)

	assert.Equal(t, "EXISTS(DUMMY(?, ?))", b.String())
	assert.Equal(t, []interface{}{42, 42}, args)

}

func BenchmarkEqToSql(b *testing.B) {
	b.Run("Nil", func(b *testing.B) {
		eq := Eq{"test": nil}

		for n := 0; n < b.N; n++ {
			eq.toSQL(&bytes.Buffer{})
		}
	})

	b.Run("EQ", func(b *testing.B) {
		eq := Eq{"test": 5}

		for n := 0; n < b.N; n++ {
			eq.toSQL(&bytes.Buffer{})
		}
	})

	b.Run("IN", func(b *testing.B) {
		eq := Eq{"test": []int{1, 2, 3, 4, 5}}

		for n := 0; n < b.N; n++ {
			eq.toSQL(&bytes.Buffer{})
		}
	})
}

func BenchmarkLtToSql(b *testing.B) {
	lt := Lt{"test": 5}

	for n := 0; n < b.N; n++ {
		lt.toSQL(&bytes.Buffer{})
	}
}
