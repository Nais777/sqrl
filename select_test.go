package sqrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectBuilderToSql(t *testing.T) {
	subQ := Select("aa", "bb").From("dd")
	b := Select("a", "b").
		Prefix("WITH prefix AS ?", 0).
		Distinct().
		Columns("c").
		Column("IF(d IN ("+Placeholders(3)+"), 1, 0) as stat_column", 1, 2, 3).
		Column(Expr("a > ?", 100)).
		Column(Alias(Eq{"b": []int{101, 102, 103}}, "b_alias")).
		Column(Alias(subQ, "subq")).
		From("e").
		JoinClause("CROSS JOIN j1").
		Join("j2").
		LeftJoin("j3").
		RightJoin("j4").
		Where("f = ?", 4).
		Where(Eq{"g": 5}).
		Where(map[string]interface{}{"h": 6}).
		Where(Eq{"i": []int{7, 8, 9}}).
		Where(Or{Expr("j = ?", 10), And{Eq{"k": 11}, Expr("true")}}).
		GroupBy("l").
		Having("m = n").
		OrderBy("o ASC", "p DESC").
		Limit(12).
		Offset(13).
		Suffix("FETCH FIRST ? ROWS ONLY", 14)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS ? " +
			"SELECT DISTINCT a, b, c, IF(d IN (?,?,?), 1, 0) as stat_column, a > ?, " +
			"(b IN (?,?,?)) AS b_alias, " +
			"(SELECT aa, bb FROM dd) AS subq " +
			"FROM e " +
			"CROSS JOIN j1 JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 " +
			"WHERE f = ? AND g = ? AND h = ? AND i IN (?,?,?) AND (j = ? OR (k = ? AND true)) " +
			"GROUP BY l HAVING m = n ORDER BY o ASC, p DESC LIMIT 12 OFFSET 13 " +
			"FETCH FIRST ? ROWS ONLY"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 100, 101, 102, 103, 4, 5, 6, 7, 8, 9, 10, 11, 14}
	assert.Equal(t, expectedArgs, args)
}

func BenchmarkSelectBuilderToSqlWithArguements(b *testing.B) {
	qb := Select().
		Column("IF(d IN ("+Placeholders(3)+"), 1, 0) as stat_column", 1, 2, 3).
		Prefix("WITH prefix AS ?", 0).
		Distinct().
		From("e").
		JoinClause("CROSS JOIN j1").
		Where("f = ?", 4).
		GroupBy("l").
		Having("m = ?", 5).
		OrderBy("o ASC", "p DESC").
		Limit(12).
		Offset(13).
		Suffix("FETCH FIRST ? ROWS ONLY", 14)

	for i := 0; i < b.N; i++ {
		qb.ToSQL()
	}
}

func BenchmarkSelectBuilderToSql(b *testing.B) {
	qb := Select("a", "b").
		Prefix("WITH prefix AS 0").
		Distinct().
		From("e").
		JoinClause("CROSS JOIN j1").
		Where("m = n").
		GroupBy("l").
		Having("m = n").
		OrderBy("o ASC", "p DESC").
		Limit(12).
		Offset(13).
		Suffix("FETCH FIRST 10 ROWS ONLY")

	for i := 0; i < b.N; i++ {
		qb.ToSQL()
	}
}

func TestSelectBuilderZeroOffsetLimit(t *testing.T) {
	qb := Select("a").
		From("b").
		Limit(0).
		Offset(0)

	sql, _, err := qb.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT a FROM b LIMIT 0 OFFSET 0"
	assert.Equal(t, expectedSQL, sql)
}

func TestSelectBuilderToSqlErr(t *testing.T) {
	_, _, err := Select().From("x").ToSQL()
	assert.Error(t, err)
}

func TestSelectBuilderPlaceholders(t *testing.T) {
	b := Select("test").Where("x = ? AND y = ?")

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = $1 AND y = $2", sql)
}

func TestSelectBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Select("test").RunWith(db)

	expectedSql := "SELECT test"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)

	b.Query()
	assert.Equal(t, expectedSql, db.LastQuerySql)

	b.QueryRow()
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	b.ExecContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	b.QueryContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	b.QueryRowContext(context.TODO())
	assert.Equal(t, expectedSql, db.LastQueryRowSql)

	err := b.Scan()
	assert.NoError(t, err)
}

func TestSelectBuilderNoRunner(t *testing.T) {
	b := Select("test")

	_, err := b.Exec()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.Query()
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.QueryContext(context.TODO())
	assert.Equal(t, ErrRunnerNotSet, err)

	_, err = b.ExecContext(context.TODO())
	assert.Equal(t, ErrRunnerNotSet, err)

	err = b.Scan()
	assert.Equal(t, ErrRunnerNotSet, err)
}
