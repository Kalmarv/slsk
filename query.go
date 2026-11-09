package slsk

import (
	"slices"
	"strings"
	"unicode"
)

// Query is a parsed Soulseek search query, for matching our shared files
// against other users' searches the way other Soulseek clients do.
//
// Every plain word of the query must equal a whole word of the path.
// "-word" excludes paths containing word, and "*suffix" matches paths with a
// word ending in suffix. Words are runs of letters and digits, compared
// case-insensitively, so "AC/DC" is the two words "ac" and "dc".
type Query struct {
	include  []string // words that must each appear as a whole path word
	suffixes []string // "*suffix" terms: some path word must end with it
	exclude  []string // "-word" terms: no path word may equal any
}

// ParseQuery parses a search query.
func ParseQuery(query string) Query {
	var q Query
	for _, term := range strings.Fields(query) {
		switch term[0] {
		case '-':
			q.exclude = append(q.exclude, Words(term[1:])...)
		case '*':
			if words := Words(term[1:]); len(words) > 0 {
				q.suffixes = append(q.suffixes, words[0])
				q.include = append(q.include, words[1:]...)
			}
		default:
			q.include = append(q.include, Words(term)...)
		}
	}
	return q
}

// Empty reports whether the query has nothing to match on: no plain words
// and no suffixes. An empty query matches every path, so a share index
// should answer it with nothing.
func (q Query) Empty() bool {
	return len(q.include) == 0 && len(q.suffixes) == 0
}

// Match reports whether a path, split into words with Words, matches the
// query. Splitting each shared path once and keeping the words makes matching
// a large share cheap.
func (q Query) Match(words []string) bool {
	for _, w := range q.include {
		if !slices.Contains(words, w) {
			return false
		}
	}
	for _, suf := range q.suffixes {
		if !slices.ContainsFunc(words, func(w string) bool { return strings.HasSuffix(w, suf) }) {
			return false
		}
	}
	for _, w := range q.exclude {
		if slices.Contains(words, w) {
			return false
		}
	}
	return true
}

// Words lowercases s and splits it into words of letters and digits, the
// unit Query matches on.
func Words(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsDigit(c)
	})
}
