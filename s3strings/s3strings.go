package s3strings

import "strings"

func RemoveObjectPrefix(key string) string {
	index := lastIndex(key, "/")
	if index > -1 {
		return key[index+1:]
	} else {
		return key
	}
}

func lastIndex(s string, sep string) int {
	index := strings.Index(s, sep)
	if index > -1 {
		nextIndex := lastIndex(s[index+1:], sep)
		if nextIndex > -1 {
			index += nextIndex + 1
		}
	}

	return index
}
