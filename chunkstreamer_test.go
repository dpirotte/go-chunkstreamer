package chunkstreamer

import (
	"bytes"
	"io"
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

	r := NewReader(&buf)

	for i := 0; i < len(chunks); i++ {
		b, err := r.ReadChunk()
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, b, chunks[i])
	}

	_, err := r.ReadChunk()
	if assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}
}
