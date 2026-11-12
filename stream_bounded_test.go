package jsonextract

import (
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
)

// infiniteReader synthesizes an endless stream of JSON objects on demand.
// It never reaches EOF, so any implementation that reads the whole input
// would never return.
type infiniteReader struct {
	buf    []byte
	pos    int
	reads  int
	object string
}

func newInfiniteReader(objectSize int) *infiniteReader {
	return &infiniteReader{
		object: `{"pad":"` + strings.Repeat("z", objectSize) + `"}` + " ",
	}
}

func (r *infiniteReader) Read(p []byte) (int, error) {
	r.reads++
	if len(r.buf) == 0 {
		r.buf = []byte(r.object)
		r.pos = 0
	}
	n := copy(p, r.buf[r.pos:])
	r.pos += n
	if r.pos == len(r.buf) {
		r.buf = nil
	}
	return n, nil
}

var _ io.Reader = (*infiniteReader)(nil)

// TestStreamBoundedMemory proves Stream returns from an infinite input once
// MaxObjects is reached and never buffers more than the current fragment.
func TestStreamBoundedMemory(t *testing.T) {
	const objectSize = 8 * 1024 // 8 KiB per object
	const limit = 5

	r := newInfiniteReader(objectSize)

	var count int
	err := Stream(r, func(o Object) error {
		count++
		return nil
	}, nil, StreamOption{MaxObjects: limit})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if count != limit {
		t.Errorf("got %d objects, want %d", count, limit)
	}

	// The reader must not have been drained (it can never drain). With a 64 KiB
	// buffer and 8 KiB objects, only a handful of reads are needed for five
	// objects. A whole-input implementation would loop here forever.
	if r.reads > 40 {
		t.Errorf("underlying reader was read %d times after limit, expected bounded readahead", r.reads)
	}

	// Heap usage must stay in the order of a few objects, not proportional to
	// an unbounded number of them.
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if ms.HeapAlloc > 2*1024*1024 {
		t.Errorf("heap allocated %d bytes, expected bounded memory for %d small objects", ms.HeapAlloc, limit)
	}
}

// chunkedSource serves fixed chunks once and then reports clean EOF.
type chunkedSource struct {
	chunks [][]byte
	pos    int
}

func (r *chunkedSource) Read(p []byte) (int, error) {
	if r.pos >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.pos])
	if n == len(r.chunks[r.pos]) {
		r.pos++
	} else {
		r.chunks[r.pos] = r.chunks[r.pos][n:]
	}
	return n, nil
}

// TestStreamFragmentBoundedMemory checks that a fragment that never terminates
// only keeps its own bytes in memory and that its diagnostic is delivered once.
func TestStreamFragmentBoundedMemory(t *testing.T) {
	// An opening brace followed by a long string that never closes, spread
	// across many reads and terminated by EOF.
	chunks := make([][]byte, 64)
	for i := range chunks {
		chunks[i] = []byte(strings.Repeat("a", 4096))
	}
	r := &chunkedSource{
		chunks: append([][]byte{[]byte(`{"s":"`)}, chunks...),
	}

	var fragErrs int
	err := Stream(r, nil, func(fe FragmentError) error {
		fragErrs++
		return nil
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if fragErrs != 1 {
		t.Errorf("expected exactly one fragment error, got %d", fragErrs)
	}

	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if ms.HeapAlloc > 2*1024*1024 {
		t.Errorf("heap allocated %d bytes, expected the failed fragment to be released", ms.HeapAlloc)
	}
}

// TestStreamReaderErrorPropagation confirms the documented contract: a non-EOF
// error from the underlying reader is returned to the caller, while objects
// extracted before the error and the diagnostic of the interrupted fragment
// are not rolled back.
type errorAfterReadsReader struct {
	data  []byte
	pos   int
	reads int
	fail  int
}

func (r *errorAfterReadsReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads > r.fail {
		return 0, io.ErrUnexpectedEOF
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestStreamReaderErrorPropagation(t *testing.T) {
	r := &errorAfterReadsReader{
		data: []byte(`{"ok":1} {"open": [1, 2`),
		fail: 2,
	}

	var objs int
	var fragErrs int
	err := Stream(r, func(o Object) error {
		objs++
		return nil
	}, func(fe FragmentError) error {
		fragErrs++
		return nil
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected the reader error to be returned, got %v", err)
	}
	if objs != 1 {
		t.Errorf("expected the object before the error to be delivered, got %d", objs)
	}
	// The interrupted fragment is reported at least once; replaying its interior
	// can produce additional diagnostics for still-unterminated parts.
	if fragErrs < 1 {
		t.Errorf("expected at least one fragment diagnostic for the interrupted fragment, got %d", fragErrs)
	}
}
