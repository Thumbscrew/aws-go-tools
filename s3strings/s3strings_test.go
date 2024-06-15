package s3strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type RemoveObjectPrefixTest struct {
	Name string
	Key  string
	Exp  string
}

func TestRemoveObjectPrefix(t *testing.T) {
	tests := []RemoveObjectPrefixTest{
		{
			Name: "should remove prefix from object",
			Key:  "some/prefix/object.zip",
			Exp:  "object.zip",
		},
		{
			Name: "key with no prefix should be unchanged",
			Key:  "object.zip",
			Exp:  "object.zip",
		},
		{
			Name: "should remove long prefix from object",
			Key:  "some/very/long/prefix/for/an/object.zip",
			Exp:  "object.zip",
		},
		{
			Name: "blank string should be unchanged",
			Key:  "",
			Exp:  "",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			assert.Equal(t, test.Exp, RemoveObjectPrefix(test.Key))
		})
	}
}
