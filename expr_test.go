package sqrl

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqToSql(t *testing.T) {
	b := Eq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInToSql(t *testing.T) {
	b := Eq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqToSql(t *testing.T) {
	b := NotEq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id <> ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqNotInToSql(t *testing.T) {
	b := NotEq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestExprNilToSql(t *testing.T) {
	var b Sqlizer
	b = NotEq{"name": nil}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql := "name IS NOT NULL"
	assert.Equal(t, expectedSql, sql)

	b = Eq{"name": nil}
	sql, args, err = b.ToSql()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql = "name IS NULL"
	assert.Equal(t, expectedSql, sql)
}

func TestLtToSql(t *testing.T) {
	b := Lt{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id < ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestLtOrEqToSql(t *testing.T) {
	b := LtOrEq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id <= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtToSql(t *testing.T) {
	b := Gt{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id > ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtOrEqToSql(t *testing.T) {
	b := GtOrEq{"id": 1}
	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "id >= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestNullTypeString(t *testing.T) {
	var b Sqlizer
	var name sql.NullString

	b = Eq{"name": name}
	sql, args, err := b.ToSql()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	name.Scan("Name")
	b = Eq{"name": name}
	sql, args, err = b.ToSql()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Name"}, args)
	assert.Equal(t, "name = ?", sql)
}

func TestNullTypeInt64(t *testing.T) {
	var userID sql.NullInt64
	userID.Scan(nil)
	b := Eq{"user_id": userID}
	sql, args, err := b.ToSql()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "user_id IS NULL", sql)

	userID.Scan(10)
	b = Eq{"user_id": userID}
	sql, args, err = b.ToSql()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(10)}, args)
	assert.Equal(t, "user_id = ?", sql)
}

type dummySqlizer int

func (d dummySqlizer) ToSql() (string, []interface{}, error) {
	return "DUMMY(?, ?)", []interface{}{int(d), int(d)}, nil
}

func TestExprSqlizer(t *testing.T) {
	b := Expr("EXISTS(?)", dummySqlizer(42))
	sql, args, err := b.ToSql()

	if assert.NoError(t, err) {
		assert.Equal(t, "EXISTS(DUMMY(?, ?))", sql)
		assert.Equal(t, []interface{}{42, 42}, args)
	}
}

func BenchmarkEqToSql(b *testing.B) {
	b.Run("Nil", func(b *testing.B) {
		eq := Eq{"test": nil}

		for n := 0; n < b.N; n++ {
			eq.ToSql()
		}
	})

	b.Run("EQ", func(b *testing.B) {
		eq := Eq{"test": 5}

		for n := 0; n < b.N; n++ {
			eq.ToSql()
		}
	})

	b.Run("IN", func(b *testing.B) {
		eq := Eq{"test": []int{1, 2, 3, 4, 5}}

		for n := 0; n < b.N; n++ {
			eq.ToSql()
		}
	})
}
