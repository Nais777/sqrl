package bufRewrite

type sqlBuffer interface {
	WriteString(s string) (int, error)
	WriteByte(c byte) error
	WriteRune(r rune) (n int, err error)
}

type sqlWriter interface {
	toSQL(b sqlBuffer) (written bool, args []interface{}, err error)
}
