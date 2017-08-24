package bufRewrite

import "testing"
import "bytes"

func BenchmarkPartAppendToSQL(b *testing.B) {
	parts := []sqlWriter{
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
		appendToSQL(parts, sql, ", ", make([]interface{}, 0))
	}
}

func BenchmarkPartWithArguementAppendToSQL(b *testing.B) {
	parts := []sqlWriter{
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1),
		newPart("test", 1)}

	for n := 0; n < b.N; n++ {
		sql := &bytes.Buffer{}
		appendToSQL(parts, sql, ", ", make([]interface{}, 0))
	}
}
