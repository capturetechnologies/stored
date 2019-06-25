package stored

import (
	"regexp"
	"strings"
)

var searchWordSplit = regexp.MustCompile("[,. ]+")

// searchGetInputWords will fetch list of words found in input fields
func searchGetInputWords(index *Index, input *Struct) (words []string) {
	for _, field := range index.fields {
		str := input.Get(field).(string)
		words = append(words, searchSplit(str)...)
	}
	return
}

func searchSplit(str string) (words []string) {
	words = searchWordSplit.Split(str, -1)
	for k, word := range words {
		words[k] = strings.ToLower(word)
	}
	return
}
