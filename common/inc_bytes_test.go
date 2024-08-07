package common

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIncBigEndianBytes(t *testing.T) {
	tests := []struct {
		input  []byte
		output []byte
		err    bool
	}{
		{[]byte{0}, []byte{1}, false},
		{[]byte{0x01, 0x02}, []byte{0x01, 0x03}, false},
		{[]byte{0xAB, 0xCD}, []byte{0xAB, 0xCE}, false},
		{[]byte{0xAB, 0xFF}, []byte{0xAC, 0x00}, false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("Input: %v", test.input), func(t *testing.T) {
			if test.err {
				defer func() {
					recover()
				}()
				IncBigEndianBytes(test.input)
				t.Error("Expected panic")
			} else {
				output := IncBigEndianBytes(test.input)
				require.Equal(t, test.output, output)
			}
		})
	}
}
