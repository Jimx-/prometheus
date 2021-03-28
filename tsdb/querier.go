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

package tsdb

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/labels"
)

// Querier provides querying access over time series data of a fixed
// time range.
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	Select(...labels.Matcher) (SeriesSet, error)

	// LabelValues returns all potential values for a label name.
	LabelValues(string) ([]string, error)

	// LabelValuesFor returns all potential values for a label name.
	// under the constraint of another label.
	LabelValuesFor(string, labels.Label) ([]string, error)

	// LabelNames returns all the unique label names present in the block in sorted order.
	LabelNames() ([]string, error)

	// Close releases the resources of the Querier.
	Close() error
}

type RawQuerier interface {
	// Select returns a set of series with the given IDs.
	Select(...labels.Tsid) (RawSeriesSet, error)

	// Close releases the resources of the Querier.
	Close() error
}

type RawSeries interface {
	Tsid() labels.Tsid
	Iterator() SeriesIterator
}

// Series exposes a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
}

// querier aggregates querying results from time blocks within
// a single partition.
type rawQuerier struct {
	blocks []RawQuerier
}

func (q *rawQuerier) Select(tsids ...labels.Tsid) (RawSeriesSet, error) {
	return q.sel(q.blocks, tsids)
}

func (q *rawQuerier) sel(qs []RawQuerier, tsids []labels.Tsid) (RawSeriesSet, error) {
	if len(qs) == 0 {
		return EmptyRawSeriesSet(), nil
	}
	if len(qs) == 1 {
		return qs[0].Select(tsids...)
	}
	l := len(qs) / 2

	a, err := q.sel(qs[:l], tsids)
	if err != nil {
		return nil, err
	}
	b, err := q.sel(qs[l:], tsids)
	if err != nil {
		return nil, err
	}
	return newMergedRawSeriesSet(a, b), nil
}

func (q *rawQuerier) Close() error {
	var merr tsdb_errors.MultiError

	for _, bq := range q.blocks {
		merr.Add(bq.Close())
	}
	return merr.Err()
}

// verticalQuerier aggregates querying results from time blocks within
// a single partition. The block time ranges can be overlapping.
type verticalQuerier struct {
	rawQuerier
}

func (q *verticalQuerier) Select(tsids ...labels.Tsid) (RawSeriesSet, error) {
	return q.sel(q.blocks, tsids)
}

func (q *verticalQuerier) sel(qs []RawQuerier, tsids []labels.Tsid) (RawSeriesSet, error) {
	if len(qs) == 0 {
		return EmptyRawSeriesSet(), nil
	}
	if len(qs) == 1 {
		return qs[0].Select(tsids...)
	}
	l := len(qs) / 2

	a, err := q.sel(qs[:l], tsids)
	if err != nil {
		return nil, err
	}
	b, err := q.sel(qs[l:], tsids)
	if err != nil {
		return nil, err
	}
	return newMergedVerticalRawSeriesSet(a, b), nil
}

// NewBlockQuerier returns a querier against the reader.
func NewBlockQuerier(b BlockReader, mint, maxt int64) (RawQuerier, error) {
	indexr, err := b.Index()
	if err != nil {
		return nil, errors.Wrapf(err, "open index reader")
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, errors.Wrapf(err, "open chunk reader")
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, errors.Wrapf(err, "open tombstone reader")
	}
	return &blockQuerier{
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

// blockQuerier provides querying access to a single block database.
type blockQuerier struct {
	index      IndexReader
	chunks     ChunkReader
	tombstones TombstoneReader

	closed bool

	mint, maxt int64
}

func (q *blockQuerier) Select(tsids ...labels.Tsid) (RawSeriesSet, error) {
	base, err := LookupChunkSeries(q.index, q.tombstones, tsids...)
	if err != nil {
		return nil, err
	}
	return &blockRawSeriesSet{
		set: &populatedChunkSeries{
			set:    base,
			chunks: q.chunks,
			mint:   q.mint,
			maxt:   q.maxt,
		},

		mint: q.mint,
		maxt: q.maxt,
	}, nil
}

func (q *blockQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	var merr tsdb_errors.MultiError
	merr.Add(q.index.Close())
	merr.Add(q.chunks.Close())
	merr.Add(q.tombstones.Close())
	q.closed = true
	return merr.Err()
}

// SeriesSet contains a set of series.
type RawSeriesSet interface {
	Next() bool
	At() RawSeries
	Err() error
}

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

var emptyRawSeriesSet = errRawSeriesSet{}

// EmptySeriesSet returns a series set that's always empty.
func EmptyRawSeriesSet() RawSeriesSet {
	return emptyRawSeriesSet
}

// mergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
type mergedRawSeriesSet struct {
	a, b RawSeriesSet

	cur          RawSeries
	adone, bdone bool
}

// NewMergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
func NewMergedRawSeriesSet(a, b RawSeriesSet) RawSeriesSet {
	return newMergedRawSeriesSet(a, b)
}

func newMergedRawSeriesSet(a, b RawSeriesSet) *mergedRawSeriesSet {
	s := &mergedRawSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedRawSeriesSet) At() RawSeries {
	return s.cur
}

func (s *mergedRawSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedRawSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	return int(int64(s.a.At().Tsid()) - int64(s.b.At().Tsid()))
}

func (s *mergedRawSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.cur = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.cur = s.a.At()
		s.adone = !s.a.Next()
	} else {
		s.cur = &chainedRawSeries{series: []RawSeries{s.a.At(), s.b.At()}}
		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

type mergedVerticalRawSeriesSet struct {
	a, b         RawSeriesSet
	cur          RawSeries
	adone, bdone bool
}

// NewMergedVerticalSeriesSet takes two series sets as a single series set.
// The input series sets must be sorted and
// the time ranges of the series can be overlapping.
func NewMergedVerticalRawSeriesSet(a, b RawSeriesSet) RawSeriesSet {
	return newMergedVerticalRawSeriesSet(a, b)
}

func newMergedVerticalRawSeriesSet(a, b RawSeriesSet) *mergedVerticalRawSeriesSet {
	s := &mergedVerticalRawSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedVerticalRawSeriesSet) At() RawSeries {
	return s.cur
}

func (s *mergedVerticalRawSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedVerticalRawSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	return int(int64(s.a.At().Tsid()) - int64(s.b.At().Tsid()))
}

func (s *mergedVerticalRawSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.cur = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.cur = s.a.At()
		s.adone = !s.a.Next()
	} else {
		s.cur = &verticalChainedRawSeries{series: []RawSeries{s.a.At(), s.b.At()}}
		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

// ChunkSeriesSet exposes the chunks and intervals of a series instead of the
// actual series itself.
type ChunkSeriesSet interface {
	Next() bool
	At() (labels.Tsid, []chunks.Meta, Intervals)
	Err() error
}

// baseChunkSeries loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be unset.
type baseChunkSeries struct {
	p          index.Postings
	index      IndexReader
	tombstones TombstoneReader

	tsid      labels.Tsid
	chks      []chunks.Meta
	intervals Intervals
	err       error
}

// LookupChunkSeries retrieves all series for the given matchers and returns a ChunkSeriesSet
// over them. It drops chunks based on tombstones in the given reader.
func LookupChunkSeries(ir IndexReader, tr TombstoneReader, tsids ...labels.Tsid) (ChunkSeriesSet, error) {
	if tr == nil {
		tr = newMemTombstones()
	}
	list := make([]uint64, len(tsids))
	for i, tsid := range tsids {
		list[i] = uint64(tsid)
	}
	p := index.NewListPostings(list)
	return &baseChunkSeries{
		p:          p,
		index:      ir,
		tombstones: tr,
	}, nil
}

func (s *baseChunkSeries) At() (labels.Tsid, []chunks.Meta, Intervals) {
	return s.tsid, s.chks, s.intervals
}

func (s *baseChunkSeries) Err() error { return s.err }

func (s *baseChunkSeries) Next() bool {
	var (
		chkMetas = make([]chunks.Meta, len(s.chks))
		err      error
	)

	for s.p.Next() {
		ref := labels.Tsid(s.p.At())
		if err := s.index.Series(ref, &chkMetas); err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Cause(err) == ErrNotFound {
				continue
			}
			s.err = err
			return false
		}

		s.tsid = ref
		s.chks = chkMetas
		s.intervals, err = s.tombstones.Get(labels.Tsid(s.p.At()))
		if err != nil {
			s.err = errors.Wrap(err, "get tombstones")
			return false
		}

		if len(s.intervals) > 0 {
			// Only those chunks that are not entirely deleted.
			chks := make([]chunks.Meta, 0, len(s.chks))
			for _, chk := range s.chks {
				if !(Interval{chk.MinTime, chk.MaxTime}.isSubrange(s.intervals)) {
					chks = append(chks, chk)
				}
			}

			s.chks = chks
		}

		return true
	}
	if err := s.p.Err(); err != nil {
		s.err = err
	}
	return false
}

// populatedChunkSeries loads chunk data from a store for a set of series
// with known chunk references. It filters out chunks that do not fit the
// given time range.
type populatedChunkSeries struct {
	set        ChunkSeriesSet
	chunks     ChunkReader
	mint, maxt int64

	err       error
	chks      []chunks.Meta
	tsid labels.Tsid
	intervals Intervals
}

func (s *populatedChunkSeries) At() (labels.Tsid, []chunks.Meta, Intervals) {
	return s.tsid, s.chks, s.intervals
}

func (s *populatedChunkSeries) Err() error { return s.err }

func (s *populatedChunkSeries) Next() bool {
	for s.set.Next() {
		tsid, chks, dranges := s.set.At()

		for len(chks) > 0 {
			if chks[0].MaxTime >= s.mint {
				break
			}
			chks = chks[1:]
		}

		// This is to delete in place while iterating.
		for i, rlen := 0, len(chks); i < rlen; i++ {
			j := i - (rlen - len(chks))
			c := &chks[j]

			// Break out at the first chunk that has no overlap with mint, maxt.
			if c.MinTime > s.maxt {
				chks = chks[:j]
				break
			}

			c.Chunk, s.err = s.chunks.Chunk(c.Ref)
			if s.err != nil {
				// This means that the chunk has be garbage collected. Remove it from the list.
				if s.err == ErrNotFound {
					s.err = nil
					// Delete in-place.
					s.chks = append(chks[:j], chks[j+1:]...)
				}
				return false
			}
		}

		if len(chks) == 0 {
			continue
		}

		s.tsid = tsid
		s.chks = chks
		s.intervals = dranges

		return true
	}
	if err := s.set.Err(); err != nil {
		s.err = err
	}
	return false
}

// blockSeriesSet is a set of series from an inverted index query.
type blockRawSeriesSet struct {
	set ChunkSeriesSet
	err error
	cur RawSeries

	mint, maxt int64
}

func (s *blockRawSeriesSet) Next() bool {
	for s.set.Next() {
		tsid, chunks, dranges := s.set.At()
		s.cur = &chunkRawSeries{
			tsid: tsid,
			chunks: chunks,
			mint:   s.mint,
			maxt:   s.maxt,

			intervals: dranges,
		}
		return true
	}
	if s.set.Err() != nil {
		s.err = s.set.Err()
	}
	return false
}

func (s *blockRawSeriesSet) At() RawSeries { return s.cur }
func (s *blockRawSeriesSet) Err() error { return s.err }

// chunkSeries is a series that is backed by a sequence of chunks holding
// time series data.
type chunkRawSeries struct {
	tsid labels.Tsid
	chunks []chunks.Meta // in-order chunk refs

	mint, maxt int64

	intervals Intervals
}

func (s *chunkRawSeries) Tsid() labels.Tsid {
	return s.tsid
}

func (s *chunkRawSeries) Iterator() SeriesIterator {
	return newChunkSeriesIterator(s.chunks, s.intervals, s.mint, s.maxt)
}

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the given timestamp.
	// If there's no value exactly at t, it advances to the first value
	// after t.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

// chainedSeries implements a series for a list of time-sorted series.
// They all must have the same labels.
type chainedRawSeries struct {
	series []RawSeries
}

func (s *chainedRawSeries) Tsid() labels.Tsid {
	return s.series[0].Tsid()
}

func (s *chainedRawSeries) Iterator() SeriesIterator {
	return newChainedSeriesIterator(s.series...)
}

// chainedSeriesIterator implements a series iterater over a list
// of time-sorted, non-overlapping iterators.
type chainedSeriesIterator struct {
	series []RawSeries // series in time order

	i   int
	cur SeriesIterator
}

func newChainedSeriesIterator(s ...RawSeries) *chainedSeriesIterator {
	return &chainedSeriesIterator{
		series: s,
		i:      0,
		cur:    s[0].Iterator(),
	}
}

func (it *chainedSeriesIterator) Seek(t int64) bool {
	// We just scan the chained series sequentially as they are already
	// pre-selected by relevant time and should be accessed sequentially anyway.
	for i, s := range it.series[it.i:] {
		cur := s.Iterator()
		if !cur.Seek(t) {
			continue
		}
		it.cur = cur
		it.i += i
		return true
	}
	return false
}

func (it *chainedSeriesIterator) Next() bool {
	if it.cur.Next() {
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.series)-1 {
		return false
	}

	it.i++
	it.cur = it.series[it.i].Iterator()

	return it.Next()
}

func (it *chainedSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chainedSeriesIterator) Err() error {
	return it.cur.Err()
}

// verticalChainedSeries implements a series for a list of time-sorted, time-overlapping series.
// They all must have the same labels.
type verticalChainedRawSeries struct {
	series []RawSeries
}

func (s *verticalChainedRawSeries) Tsid() labels.Tsid {
	return s.series[0].Tsid()
}

func (s *verticalChainedRawSeries) Iterator() SeriesIterator {
	return newVerticalMergeSeriesIterator(s.series...)
}

// verticalMergeSeriesIterator implements a series iterater over a list
// of time-sorted, time-overlapping iterators.
type verticalMergeSeriesIterator struct {
	a, b                  SeriesIterator
	aok, bok, initialized bool

	curT int64
	curV float64
}

func newVerticalMergeSeriesIterator(s ...RawSeries) SeriesIterator {
	if len(s) == 1 {
		return s[0].Iterator()
	} else if len(s) == 2 {
		return &verticalMergeSeriesIterator{
			a: s[0].Iterator(),
			b: s[1].Iterator(),
		}
	}
	return &verticalMergeSeriesIterator{
		a: s[0].Iterator(),
		b: newVerticalMergeSeriesIterator(s[1:]...),
	}
}

func (it *verticalMergeSeriesIterator) Seek(t int64) bool {
	it.aok, it.bok = it.a.Seek(t), it.b.Seek(t)
	it.initialized = true
	return it.Next()
}

func (it *verticalMergeSeriesIterator) Next() bool {
	if !it.initialized {
		it.aok = it.a.Next()
		it.bok = it.b.Next()
		it.initialized = true
	}

	if !it.aok && !it.bok {
		return false
	}

	if !it.aok {
		it.curT, it.curV = it.b.At()
		it.bok = it.b.Next()
		return true
	}
	if !it.bok {
		it.curT, it.curV = it.a.At()
		it.aok = it.a.Next()
		return true
	}

	acurT, acurV := it.a.At()
	bcurT, bcurV := it.b.At()
	if acurT < bcurT {
		it.curT, it.curV = acurT, acurV
		it.aok = it.a.Next()
	} else if acurT > bcurT {
		it.curT, it.curV = bcurT, bcurV
		it.bok = it.b.Next()
	} else {
		it.curT, it.curV = bcurT, bcurV
		it.aok = it.a.Next()
		it.bok = it.b.Next()
	}
	return true
}

func (it *verticalMergeSeriesIterator) At() (t int64, v float64) {
	return it.curT, it.curV
}

func (it *verticalMergeSeriesIterator) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunks.Meta

	i          int
	cur        chunkenc.Iterator
	bufDelIter *deletedIterator

	maxt, mint int64

	intervals Intervals
}

func newChunkSeriesIterator(cs []chunks.Meta, dranges Intervals, mint, maxt int64) *chunkSeriesIterator {
	csi := &chunkSeriesIterator{
		chunks: cs,
		i:      0,

		mint: mint,
		maxt: maxt,

		intervals: dranges,
	}
	csi.resetCurIterator()

	return csi
}

func (it *chunkSeriesIterator) resetCurIterator() {
	if len(it.intervals) == 0 {
		it.cur = it.chunks[it.i].Chunk.Iterator(it.cur)
		return
	}
	if it.bufDelIter == nil {
		it.bufDelIter = &deletedIterator{
			intervals: it.intervals,
		}
	}
	it.bufDelIter.it = it.chunks[it.i].Chunk.Iterator(it.bufDelIter.it)
	it.cur = it.bufDelIter
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	for ; it.chunks[it.i].MaxTime < t; it.i++ {
		if it.i == len(it.chunks)-1 {
			return false
		}
	}

	it.resetCurIterator()

	for it.cur.Next() {
		t0, _ := it.cur.At()
		if t0 >= t {
			return true
		}
	}
	return false
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) Next() bool {
	if it.cur.Next() {
		t, _ := it.cur.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.chunks)-1 {
		return false
	}

	it.i++
	it.resetCurIterator()

	return it.Next()
}

func (it *chunkSeriesIterator) Err() error {
	return it.cur.Err()
}

// deletedIterator wraps an Iterator and makes sure any deleted metrics are not
// returned.
type deletedIterator struct {
	it chunkenc.Iterator

	intervals Intervals
}

func (it *deletedIterator) At() (int64, float64) {
	return it.it.At()
}

func (it *deletedIterator) Next() bool {
Outer:
	for it.it.Next() {
		ts, _ := it.it.At()

		for _, tr := range it.intervals {
			if tr.inBounds(ts) {
				continue Outer
			}

			if ts > tr.Maxt {
				it.intervals = it.intervals[1:]
				continue
			}

			return true
		}

		return true
	}

	return false
}

func (it *deletedIterator) Err() error {
	return it.it.Err()
}

type errRawSeriesSet struct {
	err error
}

func (s errRawSeriesSet) Next() bool { return false }
func (s errRawSeriesSet) At() RawSeries { return nil }
func (s errRawSeriesSet) Err() error { return s.err }
