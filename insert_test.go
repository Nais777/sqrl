package sqrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSql()
	assert.Error(t, err)
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Insert("test").Values(1).RunWith(db)

	expectedSql := "INSERT INTO test VALUES (?)"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.ExecContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastExecSql)

}

func TestInsertBuilderNoRunner(t *testing.T) {
	b := Insert("test").Values(1)

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.ExecContext(context.TODO())
	assert.Equal(t, ErrRunnerNotSet, err)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "INSERT INTO table (field1) VALUES (?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func BenchmarkInsertSetMap(b *testing.B) {
	m := map[string]interface{}{
		"test":   3,
		"test2":  3,
		"test3":  3,
		"test4":  3,
		"test5":  3,
		"test6":  3,
		"test7":  3,
		"test8":  3,
		"test9":  3,
		"test10": 3}

	for n := 0; n < b.N; n++ {
		Insert("table").SetMap(m)
	}
}

func BenchmarkInsertToSQL(b *testing.B) {
	qb := Insert("test").
		Prefix("Awesome Prefix").
		Into("temp").
		SetMap(map[string]interface{}{
			"test":   3,
			"test2":  3,
			"test3":  3,
			"test4":  3,
			"test5":  3,
			"test6":  3,
			"test7":  3,
			"test8":  3,
			"test9":  3,
			"test10": 3}).
		Suffix("Awesome Suffix")

	for n := 0; n < b.N; n++ {
		qb.ToSql()
	}
}
