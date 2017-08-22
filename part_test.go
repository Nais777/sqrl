package sqrl

import "testing"
import "bytes"

func BenchmarkPartAppendToSQL(b *testing.B) {
	parts := []Sqlizer{
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test"),
		newPart("test")}

	for n := 0; n < b.N; n++ {
		sql := &bytes.Buffer{}
		appendToSql(parts, sql, ", ", make([]interface{}, 0))
	}
}
