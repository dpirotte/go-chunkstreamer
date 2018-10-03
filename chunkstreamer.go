// Package chunkstreamer implements a Reader and Writer for reading,
// and writing, streams of bytes containing irregularly sized "chunks".
//
// Each chunk contains three parts: 4 bytes (uint32) representing the
// following part's size, N bytes of data, and 8 bytes (uint64) of
// checksum for the previous part.
package chunkstreamer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash"
	"io"

	"github.com/cespare/xxhash"
)

// ErrChecksum is returned when a chunk's computed data checksum
// match the checksum recorded after the data
var ErrChecksum = errors.New("chunkstreamer: invalid checksum")

// Writer implements an io.Writer that first writes the length
// of the provided byte slice as a uint32, then writes the byte
// slice, then writes a uint64 xxhash checksum of the byte slice.
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
// writing b to the underlying io.Writer, it first writes the length
// of b, then writes b, then writes the xxhash checksum of b.
func (w *Writer) Write(b []byte) (int, error) {
	err := binary.Write(w.wr, binary.BigEndian, uint32(len(b)))
	if err != nil {
		return 0, err
	}

	n, err := w.wr.Write(b)
	if err != nil {
		return n, err
	}

	w.hasher.Write(b)
	err = binary.Write(w.wr, binary.BigEndian, w.hasher.Sum64())
	if err != nil {
		return n, err
	}
	w.hasher.Reset()

	return n, nil
}

// A Reader is a wrapper designed to read chunks from an
// underlying io.Reader
type Reader struct {
	rd     *bufio.Reader
	hasher hash.Hash64
}

// NewReader returns a new Reader reading from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		rd:     bufio.NewReader(r),
		hasher: xxhash.New(),
	}
}

// ReadFrame returns byte slices from the underlying io.Reader.
// It first reads a uint32 from the io.Reader to determine how
// many bytes to return in the byteslice. Then, it reads that
// many bytes (the "data") into a byteslice and checksums against
// the following uint64 via xxhash. If the checksum matches, the
// byteslice will be returned to the caller. Otherwise, an
// ErrChecksum will be returned.
//
// Note: The caller takes responsibility for watching for io.EOF,
// which will be bubbled up from the underlying reader.
func (r *Reader) ReadFrame() (b []byte, err error) {
	var l uint32

	err = binary.Read(r.rd, binary.BigEndian, &l)
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
