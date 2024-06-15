package s3strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveObjectPrefix(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should remove prefix from object",
			args: args{
				key: "some/prefix/object.zip",
			},
			want: "object.zip",
		},
		{
			name: "key with no prefix should be unchanged",
			args: args{
				key: "object.zip",
			},
			want: "object.zip",
		},
		{
			name: "should remove long prefix from object",
			args: args{
				key: "some/very/long/prefix/for/an/object.zip",
			},
			want: "object.zip",
		},
		{
			name: "blank string should be unchanged",
			args: args{
				key: "",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RemoveObjectPrefix(tt.args.key))
		})
	}
}
