package sqrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilderToSql(t *testing.T) {
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"UPDATE a SET b = ? + 1, c = ? WHERE d = ? " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderZeroOffsetLimit(t *testing.T) {
	qb := Update("a").
		Set("b", true).
		Limit(0).
		Offset(0)

	sql, args, err := qb.ToSql()
	assert.NoError(t, err)

	expectedSql := "UPDATE a SET b = ? LIMIT 0 OFFSET 0"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{true}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	_, _, err := Update("").Set("x", 1).ToSql()
	assert.Error(t, err)

	_, _, err = Update("x").ToSql()
	assert.Error(t, err)
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	b := Update("test").SetMap(Eq{"x": 1, "y": 2, "z": 3})

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Contains(t, sql, "x = ?")
	assert.Contains(t, sql, "y = ?")
	assert.Contains(t, sql, "z = ?")

	_, _, err := b.PlaceholderFormat(Dollar).ToSql()
	assert.Nil(t, err)
}

func TestUpdateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Update("test").Set("x", 1).RunWith(db)

	expectedSql := "UPDATE test SET x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.ExecContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestUpdateBuilderNoRunner(t *testing.T) {
	b := Update("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.ExecContext(context.TODO())
	assert.Equal(t, ErrRunnerNotSet, err)
}

func BenchmarkUpdateSetMap(b *testing.B) {
	for n := 0; n < b.N; n++ {
		Update("test").SetMap(map[string]interface{}{
			"test":   3,
			"test2":  3,
			"test3":  3,
			"test4":  3,
			"test5":  3,
			"test6":  3,
			"test7":  3,
			"test8":  3,
			"test9":  3,
			"test10": 3})
	}
}
