// Package lengthprefixed implements a Reader and Writer for reading,
// and writing, data in length-prefixed frames.
//
// Each frame contains three parts: a varint representing the length of
// the data, N bytes of data, and 8 bytes (uint64) of checksum for the data.
package lengthprefixed

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash"
	"io"

	"github.com/cespare/xxhash"
)

// ErrChecksum is returned when frame data's computed checksum
// match the provided checksum
var ErrChecksum = errors.New("lengthprefixed: invalid checksum")

// Writer implements an io.Writer
type Writer struct {
	wr     io.Writer
	hasher hash.Hash64
}

// NewWriter returns a new Writer writing to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		wr:     w,
		hasher: xxhash.New(),
	}
}

// Write implements the io.Writer interface, but instead of just
// writing b to the underlying io.Writer, it writes a length-prefixed
// frame containing a varint (length of b) + b + uint64 (xxhash checksum
// of b).
func (w *Writer) Write(b []byte) (int, error) {
	varintBuf := make([]byte, binary.MaxVarintLen64)
	varintLen := binary.PutUvarint(varintBuf, uint64(len(b)))

	if n, err := w.wr.Write(varintBuf[:varintLen]); err != nil {
		return n, err
	}

	n, err := w.wr.Write(b)
	if err != nil {
		return n, err
	}

	w.hasher.Write(b)
	if binary.Write(w.wr, binary.BigEndian, w.hasher.Sum64()); err != nil {
		return n, err
	}
	w.hasher.Reset()

	return n, nil
}

// A Reader is a wrapper designed to read frames from an underlying io.Reader.
type Reader struct {
	rd     *bufio.Reader
	hasher hash.Hash64
}

// NewReader returns a new Reader wrapping r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		rd:     bufio.NewReader(r),
		hasher: xxhash.New(),
	}
}

// ReadFrame returns the data component of length-prefixed frames.
// It first reads a varint from the io.Reader to determine how
// many bytes to return in the byteslice. Then, it reads that
// many bytes into a byteslice and checksums against the following
// int64 via xxhash. If the checksum matches, the byteslice will be
// returned to the caller. Otherwise, an ErrChecksum will be returned.
//
// Note: The caller takes responsibility for watching for io.EOF,
// which will be bubbled up from the underlying reader.
func (r *Reader) ReadFrame() (b []byte, err error) {
	l, err := binary.ReadUvarint(r.rd)
	if err != nil {
		return nil, err
	}

	b = make([]byte, l)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return nil, err
	}

	var checksum uint64
	err = binary.Read(r.rd, binary.BigEndian, &checksum)
	if err != nil {
		return nil, err
	}

	r.hasher.Write(b)
	if checksum != r.hasher.Sum64() {
		return nil, ErrChecksum
	}
	r.hasher.Reset()

	return b, nil
}
