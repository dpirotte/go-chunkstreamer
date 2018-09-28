package chunkstreamer

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHappyPath(t *testing.T) {
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
		l += 4 + len(chunk) + 8
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

func TestInvalidChecksum(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	w.Write([]byte("This message will be garbled"))

	bytes := buf.Bytes()
	bytes[5] += 1 // mangle the first character of the chunk

	r := NewReader(&buf)
	b, err := r.ReadChunk()
	assert.Nil(t, b)
	if assert.Error(t, err) {
		assert.Equal(t, ErrChecksum, err)
	}
}

func benchmarkWrite(size int, b *testing.B) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	bytes := make([]byte, size)
	rand.Read(bytes)
	for i := 0; i < b.N; i++ {
		w.Write(bytes)
	}
}

func BenchmarkWrite1(b *testing.B)  { benchmarkWrite(1, b) }
func BenchmarkWrite10(b *testing.B) { benchmarkWrite(10, b) }
