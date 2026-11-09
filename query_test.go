package slsk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func matchPaths(query string, paths ...string) []string {
	q := ParseQuery(query)
	var out []string
	for _, p := range paths {
		if !q.Empty() && q.Match(Words(p)) {
			out = append(out, p)
		}
	}
	return out
}

func TestQuery_AllWordsRequiredCaseInsensitive(t *testing.T) {
	paths := []string{`Artist\Album\01 - Track One.flac`, `Artist\Album\02 - Track Two.flac`, `Other\Song.mp3`}

	assert.Len(t, matchPaths("artist album", paths...), 2)
	assert.Len(t, matchPaths("ARTIST", paths...), 2)
	assert.Len(t, matchPaths("track", paths...), 2)
	assert.Empty(t, matchPaths("artist song", paths...), "no single path holds both words")
	assert.Empty(t, matchPaths("nonexistent", paths...))
}

func TestQuery_WholeWordsOnly(t *testing.T) {
	paths := []string{`Artist\Album\01 - Intro.flac`}

	assert.Empty(t, matchPaths("in", paths...), "'in' is a substring of 'Intro' but not a word")
	assert.Empty(t, matchPaths("a", paths...))
	assert.Len(t, matchPaths("flac", paths...), 1, "the extension is a word of the filename")
	assert.Len(t, matchPaths("intro", paths...), 1)
}

func TestQuery_PunctuationSplitsWords(t *testing.T) {
	paths := []string{`Some_Artist\Ok-Computer (2007)\a.b.flac`}

	assert.Len(t, matchPaths("artist computer", paths...), 1)
	assert.Len(t, matchPaths("ok-computer", paths...), 1, "queries split into words like paths")
	assert.Len(t, matchPaths("2007", paths...), 1)
	assert.Equal(t, []string{"ac", "dc"}, Words("AC/DC"))
}

func TestQuery_Exclude(t *testing.T) {
	paths := []string{`A\live.flac`, `A\studio.flac`}

	assert.Equal(t, []string{`A\studio.flac`}, matchPaths("a -live", paths...))
	assert.True(t, ParseQuery("-live").Empty(), "an exclusion alone is not a query")
}

func TestQuery_WildcardSuffix(t *testing.T) {
	paths := []string{`Radiohead\Paranoid Android.flac`, `Other\Droid.flac`, `Other\Android Tv.flac`, `Other\Drum.flac`}

	assert.Len(t, matchPaths("*roid", paths...), 3)
	assert.Len(t, matchPaths("*roid other", paths...), 2, "a suffix term combines with plain words")
	assert.Len(t, matchPaths("*rank ocean", `Frank Ocean\Blonde\Nikes.flac`), 1, "the wildcard form Search sends matches")
}

func TestQuery_Empty(t *testing.T) {
	for _, q := range []string{"", "   ", "-x", "*", "!!"} {
		assert.True(t, ParseQuery(q).Empty(), "%q", q)
	}
	assert.False(t, ParseQuery("*x").Empty())
}
