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

func searchWordGeneralize(word string) string {
	word = strings.ReplaceAll(word, "ё", "е")
	return strings.ToLower(word)
}

func searchSplit(str string) (words []string) {
	splits := searchWordSplit.Split(str, -1)

	for _, word := range splits {
		if word == "" || word == "." || word == "," || word == "!" {
			continue
		}
		words = append(words, searchWordGeneralize(word))
	}
	return
}
