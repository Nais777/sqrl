package sqrl

import (
	"errors"
	"testing"

	"bytes"

	"github.com/stretchr/testify/assert"
)

func TestWherePartsAppendToSql(t *testing.T) {
	parts := []sqlWriter{
		newWherePart("x = ?", 1),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSQL(parts, sql, " AND ", []interface{}{})
	assert.Equal(t, "x = ? AND y = ?", sql.String())
	assert.Equal(t, []interface{}{1, 2}, args)
}

func TestWherePartsAppendToSqlErr(t *testing.T) {
	parts := []sqlWriter{newWherePart(1)}
	_, err := appendToSQL(parts, &bytes.Buffer{}, "", []interface{}{})
	assert.Error(t, err)
}

func TestWherePartErr(t *testing.T) {
	_, err := newWherePart(1).toSQL(&bytes.Buffer{})
	assert.Error(t, err)
}

func TestWherePartString(t *testing.T) {
	b := &bytes.Buffer{}
	args, err := newWherePart("x = ?", 1).toSQL(b)
	assert.NoError(t, err)
	assert.Equal(t, "x = ?", b.String())
	assert.Equal(t, []interface{}{1}, args)
}

func TestWherePartMap(t *testing.T) {
	test := func(pred interface{}) {
		b := &bytes.Buffer{}
		_, err := newWherePart(pred).toSQL(b)
		assert.NoError(t, err)

		sql := b.String()
		expect := []string{"x = ? AND y = ?", "y = ? AND x = ?"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]interface{}{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}

func TestWherePartNoArgs(t *testing.T) {
	_, err := newWherePart(Eq{"test": []string{}}).toSQL(&bytes.Buffer{})
	assert.Equal(t, err, errors.New("equality condition must contain at least one paramater"))
}
