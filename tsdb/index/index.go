// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"bufio"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/labels"
)

const (
	// MagicIndex 4 bytes at the head of an index file.
	MagicIndex = 0xBAAAD700
	// HeaderLen represents number of bytes reserved of index for header.
	HeaderLen = 5

	// FormatV1 represents 1 version of index.
	FormatV1 = 1
	// FormatV2 represents 2 version of index.
	FormatV2 = 2
	// FormatV3 represents 3 version of index.
	FormatV3 = 3

	labelNameSeperator = "\xff"

	indexFilename = "index"
)

type indexWriterSeries struct {
	labels labels.Labels
	chunks []chunks.Meta // series file offset of chunks
}

type indexWriterSeriesSlice []*indexWriterSeries

func (s indexWriterSeriesSlice) Len() int      { return len(s) }
func (s indexWriterSeriesSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s indexWriterSeriesSlice) Less(i, j int) bool {
	return labels.Compare(s[i].labels, s[j].labels) < 0
}

type indexWriterStage uint8

const (
	idxStageNone indexWriterStage = iota
	idxStageSeries
	idxStageDone
)

func (s indexWriterStage) String() string {
	switch s {
	case idxStageNone:
		return "none"
	case idxStageSeries:
		return "series"
	case idxStageDone:
		return "done"
	}
	return "<unknown>"
}

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// Writer implements the IndexWriter interface for the standard
// serialization format.
type Writer struct {
	f    *os.File
	fbuf *bufio.Writer
	pos  uint64

	toc   TOC
	stage indexWriterStage

	// Reusable memory.
	buf1    encoding.Encbuf
	buf2    encoding.Encbuf
	uint32s []uint32

	symbols       map[string]uint32     // symbol offsets
	seriesOffsets map[uint64]uint64     // offsets of series
	labelIndexes  []labelIndexHashEntry // label index offsets
	postings      []postingsHashEntry   // postings lists offsets

	// Hold last series to validate that clients insert new series in order.
	lastSeries labels.Tsid

	crc32 hash.Hash

	Version int
}

// TOC represents index Table Of Content that states where each section of index starts.
type TOC struct {
	Series            uint64
	LabelIndicesTable uint64
}

// NewTOCFromByteSlice return parsed TOC from given index byte slice.
func NewTOCFromByteSlice(bs ByteSlice) (*TOC, error) {
	if bs.Len() < indexTOCLen {
		return nil, encoding.ErrInvalidSize
	}
	b := bs.Range(bs.Len()-indexTOCLen, bs.Len())

	expCRC := binary.BigEndian.Uint32(b[len(b)-4:])
	d := encoding.Decbuf{B: b[:len(b)-4]}

	if d.Crc32(castagnoliTable) != expCRC {
		return nil, errors.Wrap(encoding.ErrInvalidChecksum, "read TOC")
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &TOC{
		Series:            d.Be64(),
		LabelIndicesTable: d.Be64(),
	}, nil
}

// NewWriter returns a new Writer to the given filename. It serializes data in format version 2.
func NewWriter(fn string) (*Writer, error) {
	dir := filepath.Dir(fn)

	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	defer df.Close() // Close for platform windows.

	if err := os.RemoveAll(fn); err != nil {
		return nil, errors.Wrap(err, "remove any existing index at path")
	}

	f, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	iw := &Writer{
		f:     f,
		fbuf:  bufio.NewWriterSize(f, 1<<22),
		pos:   0,
		stage: idxStageNone,

		// Reusable memory.
		buf1:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		buf2:    encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		uint32s: make([]uint32, 0, 1<<15),

		// Caches.
		symbols:       make(map[string]uint32, 1<<13),
		seriesOffsets: make(map[uint64]uint64, 1<<16),
		crc32:         newCRC32(),
	}
	if err := iw.writeMeta(); err != nil {
		return nil, err
	}
	return iw, nil
}

func (w *Writer) write(bufs ...[]byte) error {
	for _, b := range bufs {
		n, err := w.fbuf.Write(b)
		w.pos += uint64(n)
		if err != nil {
			return err
		}
		// For now the index file must not grow beyond 64GiB. Some of the fixed-sized
		// offset references in v1 are only 4 bytes large.
		// Once we move to compressed/varint representations in those areas, this limitation
		// can be lifted.
		if w.pos > 16*math.MaxUint32 {
			return errors.Errorf("exceeding max size of 64GiB")
		}
	}
	return nil
}

// addPadding adds zero byte padding until the file size is a multiple size.
func (w *Writer) addPadding(size int) error {
	p := w.pos % uint64(size)
	if p == 0 {
		return nil
	}
	p = uint64(size) - p
	return errors.Wrap(w.write(make([]byte, p)), "add padding")
}

// ensureStage handles transitions between write stages and ensures that IndexWriter
// methods are called in an order valid for the implementation.
func (w *Writer) ensureStage(s indexWriterStage) error {
	if w.stage == s {
		return nil
	}
	if w.stage > s {
		return errors.Errorf("invalid stage %q, currently at %q", s, w.stage)
	}

	// Mark start of sections in table of contents.
	switch s {
	case idxStageSeries:
		w.toc.Series = w.pos

	case idxStageDone:
		w.toc.LabelIndicesTable = w.pos
		if err := w.writeLabelIndexesOffsetTable(); err != nil {
			return err
		}

		if err := w.writeTOC(); err != nil {
			return err
		}
	}

	w.stage = s
	return nil
}

func (w *Writer) writeMeta() error {
	w.buf1.Reset()
	w.buf1.PutBE32(MagicIndex)
	w.buf1.PutByte(FormatV3)

	return w.write(w.buf1.Get())
}

// AddSeries adds the series one at a time along with its chunks.
func (w *Writer) AddSeries(tsid labels.Tsid, chunks ...chunks.Meta) error {
	if err := w.ensureStage(idxStageSeries); err != nil {
		return err
	}
	if tsid <= w.lastSeries {
		return errors.Errorf("out-of-order series added with TSID %d", tsid)
	}

	if _, ok := w.seriesOffsets[uint64(tsid)]; ok {
		return errors.Errorf("series with reference %d already added", tsid)
	}
	// We add padding to 16 bytes to increase the addressable space we get through 4 byte
	// series references.
	if err := w.addPadding(16); err != nil {
		return errors.Errorf("failed to write padding bytes: %v", err)
	}

	if w.pos%16 != 0 {
		return errors.Errorf("series write not 16-byte aligned at %d", w.pos)
	}
	w.seriesOffsets[uint64(tsid)] = w.pos / 16

	w.buf2.Reset()
	w.buf2.PutUvarint(len(chunks))

	if len(chunks) > 0 {
		c := chunks[0]
		w.buf2.PutVarint64(c.MinTime)
		w.buf2.PutUvarint64(uint64(c.MaxTime - c.MinTime))
		w.buf2.PutUvarint64(c.Ref)
		t0 := c.MaxTime
		ref0 := int64(c.Ref)

		for _, c := range chunks[1:] {
			w.buf2.PutUvarint64(uint64(c.MinTime - t0))
			w.buf2.PutUvarint64(uint64(c.MaxTime - c.MinTime))
			t0 = c.MaxTime

			w.buf2.PutVarint64(int64(c.Ref) - ref0)
			ref0 = int64(c.Ref)
		}
	}

	w.buf1.Reset()
	w.buf1.PutUvarint(w.buf2.Len())

	w.buf2.PutHash(w.crc32)

	if err := w.write(w.buf1.Get(), w.buf2.Get()); err != nil {
		return errors.Wrap(err, "write series data")
	}

	w.lastSeries = tsid

	return nil
}

// writeLabelIndexesOffsetTable writes the label indices offset table.
func (w *Writer) writeLabelIndexesOffsetTable() error {
	w.buf2.Reset()
	w.buf2.PutBE32int(len(w.seriesOffsets))

	for k, v := range w.seriesOffsets {
		w.buf2.PutUvarint64(k)
		w.buf2.PutUvarint64(v)
	}

	w.buf1.Reset()
	w.buf1.PutBE32int(w.buf2.Len())
	w.buf2.PutHash(w.crc32)

	return w.write(w.buf1.Get(), w.buf2.Get())
}

const indexTOCLen = 2*8 + 4

func (w *Writer) writeTOC() error {
	w.buf1.Reset()

	w.buf1.PutBE64(w.toc.Series)
	w.buf1.PutBE64(w.toc.LabelIndicesTable)

	w.buf1.PutHash(w.crc32)

	return w.write(w.buf1.Get())
}


type uint32slice []uint32

func (s uint32slice) Len() int           { return len(s) }
func (s uint32slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s uint32slice) Less(i, j int) bool { return s[i] < s[j] }

type labelIndexHashEntry struct {
	keys   []string
	offset uint64
}

type postingsHashEntry struct {
	name, value string
	offset      uint64
}

func (w *Writer) Close() error {
	if err := w.ensureStage(idxStageDone); err != nil {
		return err
	}
	if err := w.fbuf.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// StringTuples provides access to a sorted list of string tuples.
type StringTuples interface {
	// Total number of tuples in the list.
	Len() int
	// At returns the tuple at position i.
	At(i int) ([]string, error)
}

type Reader struct {
	b ByteSlice

	// Close that releases the underlying resources of the byte slice.
	c io.Closer

	// Cached hashmaps of section offsets.
	labels map[string]uint64
	// LabelName to LabelValue to offset map.
	postings map[string]map[string]uint64
	// Cache of read symbols. Strings that are returned when reading from the
	// block are always backed by true strings held in here rather than
	// strings that are backed by byte slices from the mmap'd index file. This
	// prevents memory faults when applications work with read symbols after
	// the block has been unmapped. The older format has sparse indexes so a map
	// must be used, but the new format is not so we can use a slice.
	symbolsV1        map[uint32]string
	symbolsV2        []string
	symbolsTableSize uint64
	offsetTable map[uint64]uint64

	dec *Decoder

	version int
}

// ByteSlice abstracts a byte slice.
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) ByteSlice {
	return b[start:end]
}

// NewReader returns a new index reader on the given byte slice. It automatically
// handles different format versions.
func NewReader(b ByteSlice) (*Reader, error) {
	return newReader(b, ioutil.NopCloser(nil))
}

// NewFileReader returns a new index reader against the given index file.
func NewFileReader(path string) (*Reader, error) {
	f, err := fileutil.OpenMmapFile(path)
	if err != nil {
		return nil, err
	}
	r, err := newReader(realByteSlice(f.Bytes()), f)
	if err != nil {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(f.Close())
		return nil, merr
	}

	return r, nil
}

func newReader(b ByteSlice, c io.Closer) (*Reader, error) {
	r := &Reader{
		b:        b,
		c:        c,
		labels:   map[string]uint64{},
		postings: map[string]map[string]uint64{},
		offsetTable: map[uint64]uint64{},
	}

	// Verify header.
	if r.b.Len() < HeaderLen {
		return nil, errors.Wrap(encoding.ErrInvalidSize, "index header")
	}
	if m := binary.BigEndian.Uint32(r.b.Range(0, 4)); m != MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}
	r.version = int(r.b.Range(4, 5)[0])

	if r.version != FormatV3 {
		return nil, errors.Errorf("unknown index file version %d", r.version)
	}

	toc, err := NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	if err := ReadOffsetTable(r.b, toc.LabelIndicesTable, func(tsid labels.Tsid, off uint64) error {
		r.offsetTable[uint64(tsid)] = off
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read label index table")
	}

	r.dec = &Decoder{}

	return r, nil
}

// Version returns the file format version of the underlying index.
func (r *Reader) Version() int {
	return r.version
}

// Range marks a byte range.
type Range struct {
	Start, End int64
}

// ReadOffsetTable reads an offset table and at the given position calls f for each
// found entry. If f returns an error it stops decoding and returns the received error.
func ReadOffsetTable(bs ByteSlice, off uint64, f func(labels.Tsid, uint64) error) error {
	d := encoding.NewDecbufAt(bs, int(off), castagnoliTable)
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		tsid := labels.Tsid(d.Uvarint64())
		offset := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(tsid, offset); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

// Close the reader and its underlying resources.
func (r *Reader) Close() error {
	return r.c.Close()
}

type emptyStringTuples struct{}

func (emptyStringTuples) At(i int) ([]string, error) { return nil, nil }
func (emptyStringTuples) Len() int                   { return 0 }

// Series reads the series with the given ID and writes its labels and chunks into lbls and chks.
func (r *Reader) Series(id labels.Tsid, lbls *labels.Labels, chks *[]chunks.Meta) error {
	*lbls = nil
	offset := r.offsetTable[uint64(id)]
	if r.version == FormatV3 {
		offset = offset * 16
	}
	d := encoding.NewDecbufUvarintAt(r.b, int(offset), castagnoliTable)
	if d.Err() != nil {
		return d.Err()
	}
	return errors.Wrap(r.dec.Series(d.Get(), chks), "read series")
}

func (r *Reader) AllPostings() (Postings, error) {
	list := make([]uint64, len(r.offsetTable))
	i := 0

	for k, _ := range r.offsetTable {
		list[i] = k
		i++
	}

	sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })

	return NewListPostings(list), nil
}

// Size returns the size of an index file.
func (r *Reader) Size() int64 {
	return int64(r.b.Len())
}

type stringTuples struct {
	length  int      // tuple length
	entries []string // flattened tuple entries
	swapBuf []string
}

func NewStringTuples(entries []string, length int) (*stringTuples, error) {
	if len(entries)%length != 0 {
		return nil, errors.Wrap(encoding.ErrInvalidSize, "string tuple list")
	}
	return &stringTuples{
		entries: entries,
		length:  length,
	}, nil
}

func (t *stringTuples) Len() int                   { return len(t.entries) / t.length }
func (t *stringTuples) At(i int) ([]string, error) { return t.entries[i : i+t.length], nil }

func (t *stringTuples) Swap(i, j int) {
	if t.swapBuf == nil {
		t.swapBuf = make([]string, t.length)
	}
	copy(t.swapBuf, t.entries[i:i+t.length])
	for k := 0; k < t.length; k++ {
		t.entries[i+k] = t.entries[j+k]
		t.entries[j+k] = t.swapBuf[k]
	}
}

func (t *stringTuples) Less(i, j int) bool {
	for k := 0; k < t.length; k++ {
		d := strings.Compare(t.entries[i+k], t.entries[j+k])

		if d < 0 {
			return true
		}
		if d > 0 {
			return false
		}
	}
	return false
}

type serializedStringTuples struct {
	idsCount int
	idsBytes []byte // bytes containing the ids pointing to the string in the lookup table.
	lookup   func(uint32) (string, error)
}

func (t *serializedStringTuples) Len() int {
	return len(t.idsBytes) / (4 * t.idsCount)
}

func (t *serializedStringTuples) At(i int) ([]string, error) {
	if len(t.idsBytes) < (i+t.idsCount)*4 {
		return nil, encoding.ErrInvalidSize
	}
	res := make([]string, 0, t.idsCount)

	for k := 0; k < t.idsCount; k++ {
		offset := binary.BigEndian.Uint32(t.idsBytes[(i+k)*4:])

		s, err := t.lookup(offset)
		if err != nil {
			return nil, errors.Wrap(err, "symbol lookup")
		}
		res = append(res, s)
	}

	return res, nil
}

// Decoder provides decoding methods for the v1 and v2 index file format.
//
// It currently does not contain decoding methods for all entry types but can be extended
// by them if there's demand.
type Decoder struct {
}

// Series decodes a series entry from the given byte slice into lset and chks.
func (dec *Decoder) Series(b []byte, chks *[]chunks.Meta) error {
	*chks = (*chks)[:0]

	d := encoding.Decbuf{B: b}

	// Read the chunks meta data.
	k := d.Uvarint()

	if k == 0 {
		return nil
	}

	t0 := d.Varint64()
	maxt := int64(d.Uvarint64()) + t0
	ref0 := int64(d.Uvarint64())

	*chks = append(*chks, chunks.Meta{
		Ref:     uint64(ref0),
		MinTime: t0,
		MaxTime: maxt,
	})
	t0 = maxt

	for i := 1; i < k; i++ {
		mint := int64(d.Uvarint64()) + t0
		maxt := int64(d.Uvarint64()) + mint

		ref0 += d.Varint64()
		t0 = maxt

		if d.Err() != nil {
			return errors.Wrapf(d.Err(), "read meta for chunk %d", i)
		}

		*chks = append(*chks, chunks.Meta{
			Ref:     uint64(ref0),
			MinTime: mint,
			MaxTime: maxt,
		})
	}
	return d.Err()
}
