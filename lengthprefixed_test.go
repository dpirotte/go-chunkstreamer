package lengthprefixed_test

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"io"
	"testing"

	"github.com/dpirotte/go-lengthprefixed"
	"github.com/stretchr/testify/assert"
)

func TestHappyPath(t *testing.T) {
	chunks := [][]byte{
		[]byte("This is a chunk."),
		[]byte("This is a much longer chunk."),
		[]byte("This is an even longer chunk than you can imagine."),
	}

	var buf bytes.Buffer
	w := lengthprefixed.NewWriter(&buf)

	l := 0
	for _, chunk := range chunks {
		_, err := w.Write(chunk)
		if err != nil {
			t.Fatal(err)
		}

		varintBuf := make([]byte, binary.MaxVarintLen64)
		varintLen := binary.PutVarint(varintBuf, int64(len(chunk)))

		l += varintLen + len(chunk) + 8
		assert.Equal(t, l, buf.Len())
	}

	r := lengthprefixed.NewReader(&buf)

	for i := 0; i < len(chunks); i++ {
		b, err := r.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, b, chunks[i])
	}

	_, err := r.ReadFrame()
	if assert.Error(t, err) {
		assert.Equal(t, io.EOF, err)
	}
}

func TestInvalidChecksum(t *testing.T) {
	var buf bytes.Buffer
	w := lengthprefixed.NewWriter(&buf)

	w.Write([]byte("This message will be garbled"))

	bytes := buf.Bytes()
	bytes[5]++ // mangle the first character of the chunk

	r := lengthprefixed.NewReader(&buf)
	b, err := r.ReadFrame()
	assert.Nil(t, b)
	if assert.Error(t, err) {
		assert.Equal(t, lengthprefixed.ErrChecksum, err)
	}
}

func benchmarkWrite(size int, b *testing.B) {
	var buf bytes.Buffer
	w := lengthprefixed.NewWriter(&buf)

	bytes := make([]byte, size)
	rand.Read(bytes)
	for i := 0; i < b.N; i++ {
		w.Write(bytes)
	}
}

func BenchmarkWrite1(b *testing.B)  { benchmarkWrite(1, b) }
func BenchmarkWrite10(b *testing.B) { benchmarkWrite(10, b) }

func benchmarkReadWriteBatch(size int, b *testing.B) {
	var (
		msgs [][]byte
		msg  = []byte("This is a relatively short string message.")
	)

	for i := 0; i < size; i++ {
		msgs = append(msgs, msg)
	}

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := lengthprefixed.NewWriter(&buf)

		for _, s := range msgs {
			w.Write([]byte(s))
		}

		r := lengthprefixed.NewReader(&buf)
		for {
			_, err := r.ReadFrame()
			if err == io.EOF {
				break
			}
		}
	}
}

func BenchmarkReadWriteBatch10(b *testing.B)      { benchmarkReadWriteBatch(10, b) }
func BenchmarkReadWriteBatch1000(b *testing.B)    { benchmarkReadWriteBatch(1000, b) }
func BenchmarkReadWriteBatch100000(b *testing.B)  { benchmarkReadWriteBatch(100000, b) }
func BenchmarkReadWriteBatch1000000(b *testing.B) { benchmarkReadWriteBatch(1000000, b) }

func benchmarkReadWriteBatchJSON(size int, b *testing.B) {
	var (
		msgs [][]byte
		msg  = []byte("This is a relatively short string message.")
	)

	for i := 0; i < size; i++ {
		msgs = append(msgs, msg)
	}

	for i := 0; i < b.N; i++ {
		j, err := json.Marshal(msgs)
		if err != nil {
			panic(err)
		}

		var buf bytes.Buffer
		buf.Write(j)

		var parsedMsgs [][]byte
		err = json.Unmarshal(buf.Bytes(), &parsedMsgs)
		if err != nil {
			panic(err)
		}

		for range parsedMsgs {
		}
	}
}
func BenchmarkReadWriteBatchJSON10(b *testing.B)      { benchmarkReadWriteBatchJSON(10, b) }
func BenchmarkReadWriteBatchJSON1000(b *testing.B)    { benchmarkReadWriteBatchJSON(1000, b) }
func BenchmarkReadWriteBatchJSON100000(b *testing.B)  { benchmarkReadWriteBatchJSON(100000, b) }
func BenchmarkReadWriteBatchJSON1000000(b *testing.B) { benchmarkReadWriteBatchJSON(1000000, b) }
