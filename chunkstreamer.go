package chunkstreamer

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/cespare/xxhash"
)

var ErrChecksum = errors.New("chunkstreamer: invalid checksum")

type Writer struct {
	wr io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{wr: w}
}

func (w *Writer) Write(b []byte) (int, error) {
	err := binary.Write(w.wr, binary.BigEndian, uint32(len(b)))
	if err != nil {
		return 0, err
	}

	n, err := w.wr.Write(b)
	if err != nil {
		return n, err
	}

	err = binary.Write(w.wr, binary.BigEndian, xxhash.Sum64(b))
	return n, nil
}

type Reader struct {
	rd io.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{rd: r}
}

func (r *Reader) ReadChunk() (b []byte, err error) {
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

	if checksum != xxhash.Sum64(b) {
		return nil, ErrChecksum
	}

	return b, nil
}
