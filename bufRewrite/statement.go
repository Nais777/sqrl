package bufRewrite

// Case returns a new CaseBuilder
// "what" represents case value
func Case(what ...interface{}) *CaseBuilder {
	b := &CaseBuilder{}

	switch len(what) {
	case 0:
	case 1:
		b = b.what(what[0])
	default:
		b = b.what(newPart(what[0], what[1:]...))

	}
	return b
}
