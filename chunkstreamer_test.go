package chunkstreamer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteAndRead(t *testing.T) {
	chunks := [][]byte{
		[]byte("This is a chunk."),
		[]byte("This is a much longer chunk."),
		[]byte("This is an even longer chunk than you can imagine."),
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)

	l := 0
	for _, chunk := range chunks {
		_, err := w.Write(chunk)
		if err != nil {
			t.Fatal(err)
		}
		l += 4 + len(chunk)
		assert.Equal(t, l, buf.Len())
	}
}
