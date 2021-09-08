package strings

// Contain returns true if the search value is within the list of input values.
func Contain(values []string, search string) bool {
	for _, v := range values {
		if search == v {
			return true
		}
	}

	return false
}

// Map returns a map where keys are input values.
func Map(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}
