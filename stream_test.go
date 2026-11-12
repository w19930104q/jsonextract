package jsonextract

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

// chunkReader returns at most size bytes per Read call, regardless of the
// requested buffer size. It forces every construct of the streaming scanner
// to span multiple read boundaries.
type chunkReader struct {
	data []byte
	pos  int
	size int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := r.size
	if n > len(p) {
		n = len(p)
	}
	if rest := len(r.data) - r.pos; n > rest {
		n = rest
	}
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// chunkSizes are all used in table tests. A size of one means every byte is a
// separate read; larger sizes shift boundary positions through the input.
var chunkSizes = []int{1, 2, 3, 5, 7, 16, 64, 4096}

func streamObjects(r io.Reader, opts ...StreamOption) (objs []Object, fragErrs []FragmentError, err error) {
	err = Stream(r, func(o Object) error {
		// Keep independent copies as the documented slice lifetime only
		// covers the callback.
		o.Raw = append([]byte(nil), o.Raw...)
		o.Source = append([]byte(nil), o.Source...)
		objs = append(objs, o)
		return nil
	}, func(fe FragmentError) error {
		fe.Fragment = append([]byte(nil), fe.Fragment...)
		fragErrs = append(fragErrs, fe)
		return nil
	}, opts...)
	return
}

// TestStreamChunkBoundaries runs the same extraction through readers with many
// different chunk sizes and requires identical results, including one-byte reads.
func TestStreamChunkBoundaries(t *testing.T) {
	const input = `prefix {"a": 1, "b": [2, 3], "c": {"d": true}} middle [1, "2", {e: null}] tail`

	want := []string{
		`{"a":1,"b":[2,3],"c":{"d":true}}`,
		`[1,"2",{"e":null}]`,
	}

	baseline, _, err := streamObjects(strings.NewReader(input))
	if err != nil {
		t.Fatalf("baseline Stream error: %v", err)
	}
	if len(baseline) != len(want) {
		t.Fatalf("baseline got %d objects, want %d", len(baseline), len(want))
	}

	for _, size := range chunkSizes {
		t.Run(fmt.Sprintf("chunksize=%d", size), func(t *testing.T) {
			objs, fragErrs, err := streamObjects(&chunkReader{data: []byte(input), size: size})
			if err != nil {
				t.Fatalf("Stream error: %v", err)
			}
			if len(fragErrs) != 0 {
				t.Errorf("expected no fragment errors, got %d: %v", len(fragErrs), fragErrs)
			}
			if len(objs) != len(want) {
				t.Fatalf("got %d objects, want %d", len(objs), len(want))
			}
			for i, o := range objs {
				if string(o.Raw) != want[i] {
					t.Errorf("object %d raw = %q, want %q", i, o.Raw, want[i])
				}
				if string(input[o.Start:o.End]) != string(o.Source) {
					t.Errorf("object %d offsets [%d:%d] do not cover source %q", i, o.Start, o.End, o.Source)
				}
				if int(o.End-o.Start) != len(o.Source) {
					t.Errorf("object %d offset interval length %d != source length %d", i, o.End-o.Start, len(o.Source))
				}
			}
		})
	}
}

// TestStreamOneByteReaderParity makes sure the new streaming implementation
// extracts exactly the same objects as the public Reader API for every case in
// the existing test table, even when every byte arrives separately.
func TestStreamOneByteReaderParity(t *testing.T) {
	for _, tt := range testData {
		t.Run("", func(t *testing.T) {
			objs, _, err := streamObjects(iotest.OneByteReader(strings.NewReader(tt.arg)))
			if err != nil {
				t.Fatalf("Stream error for %q: %v", tt.arg, err)
			}

			var got []string
			for _, o := range objs {
				got = append(got, string(o.Raw))
			}
			var want []string
			for _, w := range tt.want {
				want = append(want, string(w))
			}
			if len(got) != len(want) {
				t.Fatalf("input %q: got %v, want %v", tt.arg, got, want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("input %q object %d = %q, want %q", tt.arg, i, got[i], want[i])
				}
			}
		})
	}
}

// TestStreamUTF8Multibyte checks that multi-byte UTF-8 characters outside and
// inside strings do not corrupt byte offsets, including when a rune is split
// across multiple reads.
func TestStreamUTF8Multibyte(t *testing.T) {
	// The leading text contains runes of two, three and four bytes.
	input := "日本語🙂 " + `{"name": "漢字", "emoji": "🎉"}` + " 日本語🙂 " + `["Ω", "å"]`

	var objs []Object
	var fragErrs []FragmentError

	// A chunk size of three splits the four-byte emoji and the three-byte
	// kanji runes in the middle of their encoding.
	err := Stream(&chunkReader{data: []byte(input), size: 3}, func(o Object) error {
		objs = append(objs, o)
		return nil
	}, func(fe FragmentError) error {
		fragErrs = append(fragErrs, fe)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if len(fragErrs) != 0 {
		t.Fatalf("unexpected fragment errors: %v", fragErrs)
	}
	if len(objs) != 2 {
		t.Fatalf("got %d objects, want 2", len(objs))
	}

	wantRaw := []string{
		`{"name":"漢字","emoji":"🎉"}`,
		`["Ω","å"]`,
	}
	for i, o := range objs {
		if string(o.Raw) != wantRaw[i] {
			t.Errorf("object %d raw = %q, want %q", i, o.Raw, wantRaw[i])
		}
		// Offsets are byte offsets and must point exactly at the source bytes.
		if got := input[o.Start:o.End]; got != string(o.Source) {
			t.Errorf("object %d source at [%d:%d] = %q, want %q", i, o.Start, o.End, got, o.Source)
		}
		// Valid UTF-8 must survive chunking.
		if !bytes.Equal([]byte(input[o.Start:o.End]), o.Source) {
			t.Errorf("object %d source bytes mismatch", i)
		}
	}

	// Explicit byte offsets for the first object, checked against the known
	// length of the prefix: "日本語🙂 " is 3*3 + 4 + 1 = 14 bytes.
	if objs[0].Start != 14 {
		t.Errorf("first object start = %d, want 14", objs[0].Start)
	}
	if objs[0].End != 14+int64(len(objs[0].Source)) {
		t.Errorf("first object end = %d, want %d", objs[0].End, 14+len(objs[0].Source))
	}
}

// TestStreamNestedObjects verifies deeply nested objects and arrays across
// chunk boundaries and that every nested object gets its own offsets.
func TestStreamNestedObjects(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"outer": {"inner": {"deep": [1, 2, {"leaf": true}]}}, "arr": [[1], [2, [3]]]}`)
	input := b.String()

	for _, size := range chunkSizes {
		t.Run(fmt.Sprintf("chunksize=%d", size), func(t *testing.T) {
			objs, fragErrs, err := streamObjects(&chunkReader{data: []byte(input), size: size})
			if err != nil {
				t.Fatalf("Stream error: %v", err)
			}
			if len(fragErrs) != 0 {
				t.Fatalf("unexpected fragment errors: %v", fragErrs)
			}
			if len(objs) != 1 {
				t.Fatalf("got %d top-level objects, want 1", len(objs))
			}
			o := objs[0]
			if o.Raw[0] != '{' || o.Raw[len(o.Raw)-1] != '}' {
				t.Errorf("extracted object does not span full braces: %q", o.Raw)
			}
			if string(input[o.Start:o.End]) != input {
				t.Errorf("offsets [%d:%d] do not cover the whole input of %d bytes", o.Start, o.End, len(input))
			}
		})
	}

	// Many consecutive nested objects, each emitted separately in order.
	const repeated = 200
	var many strings.Builder
	for i := 0; i < repeated; i++ {
		fmt.Fprintf(&many, `{"i": %d, "v": [1, {"x": 2}]} `, i)
	}

	objs, _, err := streamObjects(iotest.OneByteReader(strings.NewReader(many.String())))
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if len(objs) != repeated {
		t.Fatalf("got %d objects, want %d", len(objs), repeated)
	}
	for i, o := range objs {
		want := fmt.Sprintf(`{"i":%d,"v":[1,{"x":2}]}`, i)
		if string(o.Raw) != want {
			t.Errorf("object %d = %q, want %q", i, o.Raw, want)
		}
		if o.Start >= o.End {
			t.Errorf("object %d has non-positive interval [%d:%d]", i, o.Start, o.End)
		}
	}
}

// TestStreamConsecutiveBadFragments ensures that several unparseable fragments
// in a row are each reported, do not roll back objects already delivered, and
// that valid objects between and after them are still extracted.
func TestStreamConsecutiveBadFragments(t *testing.T) {
	// Two unterminated fragments, a valid object, another bad fragment and a
	// valid array at the end.
	input := `{ "a": [1, 2 { "b": {"good": 1} {"c": } [10, 20]`

	var (
		objs     []Object
		fragErrs []FragmentError
	)
	err := Stream(iotest.OneByteReader(strings.NewReader(input)), func(o Object) error {
		objs = append(objs, o)
		return nil
	}, func(fe FragmentError) error {
		fragErrs = append(fragErrs, fe)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	wantObjs := []string{`{"good":1}`, `[10,20]`}
	if len(objs) != len(wantObjs) {
		t.Fatalf("got objects %v, want %v (fragErrs: %v)", objs, wantObjs, fragErrs)
	}
	for i, o := range objs {
		if string(o.Raw) != wantObjs[i] {
			t.Errorf("object %d = %q, want %q", i, o.Raw, wantObjs[i])
		}
	}

	// At least the two clearly broken fragments must be reported. The nested
	// recovery can report additional diagnostics, so check the lower bound and
	// the invariants that matter for recovery.
	if len(fragErrs) < 2 {
		t.Fatalf("expected at least 2 fragment errors, got %d: %v", len(fragErrs), fragErrs)
	}

	for i, fe := range fragErrs {
		if fe.Err == nil {
			t.Errorf("fragment error %d has no underlying error", i)
		}
		if fe.Start < 0 || fe.End <= fe.Start {
			t.Errorf("fragment error %d has invalid offsets [%d:%d]", i, fe.Start, fe.End)
		}
		if len(fe.Fragment) == 0 {
			t.Errorf("fragment error %d has empty fragment bytes", i)
		}
		// The cause must be reachable through the Unwrap chain.
		if !errors.Is(&fe, fe.Err) {
			t.Errorf("fragment error %d cause is not reachable via Unwrap", i)
		}
	}

	// A bad fragment followed immediately by another bad fragment: both must be
	// recovered from without losing the valid object afterwards.
	input2 := `{"unterminated" {"also bad" {"ok": 1}`
	objs2, fragErrs2, err := streamObjects(iotest.OneByteReader(strings.NewReader(input2)))
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if len(fragErrs2) < 2 {
		t.Errorf("expected at least 2 fragment errors, got %d: %v", len(fragErrs2), fragErrs2)
	}
	if len(objs2) != 1 || string(objs2[0].Raw) != `{"ok":1}` {
		t.Errorf("got objects %v, want only {\"ok\":1}", objs2)
	}
}

// TestStreamFragmentErrorUnwrap checks the documented error wrapping contract.
func TestStreamFragmentErrorUnwrap(t *testing.T) {
	sentinel := errors.New("sentinel")
	fe := FragmentError{Start: 1, End: 2, Err: sentinel}
	if !errors.Is(&fe, sentinel) {
		t.Error("errors.Is does not find the wrapped error")
	}
	if msg := fe.Error(); !strings.Contains(msg, "sentinel") || !strings.Contains(msg, "1-2") {
		t.Errorf("Error() = %q, want it to contain offsets and cause", msg)
	}
}

// TestStreamEarlyStop verifies that MaxObjects stops extraction before the
// remaining input is read, and that callbacks returning ErrStop do the same.
type countingReader struct {
	remaining []byte
	reads     int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if len(r.remaining) == 0 {
		return 0, io.EOF
	}
	r.reads++
	n := copy(p, r.remaining)
	r.remaining = r.remaining[n:]
	return n, nil
}

func TestStreamEarlyStopMaxObjects(t *testing.T) {
	const objects = 200
	var b strings.Builder
	for i := 0; i < objects; i++ {
		// Each object is about 4 KiB, so the whole input greatly exceeds the
		// internal 64 KiB read buffer. This lets us observe that extraction
		// stops touching the underlying reader after the limit is reached.
		fmt.Fprintf(&b, `{"i":%d,"pad":%q} `, i, strings.Repeat("x", 4000))
	}
	data := []byte(b.String())

	const limit = 3
	cr := &countingReader{remaining: data}

	var got int
	err := Stream(cr, func(o Object) error {
		got++
		return nil
	}, nil, StreamOption{MaxObjects: limit})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	if got != limit {
		t.Errorf("callback called %d times, want %d", got, limit)
	}

	// After three objects only a small prefix (a few internal buffer fills)
	// may have been read; the majority of the stream must remain untouched.
	if len(cr.remaining) == 0 {
		t.Error("input was fully read even though MaxObjects was reached")
	}
	consumed := len(data) - len(cr.remaining)
	if consumed >= len(data)/4 {
		t.Errorf("consumed %d of %d bytes after only %d objects, expected early stop", consumed, len(data), limit)
	}
}

func TestStreamEarlyStopCallback(t *testing.T) {
	// More than 64 KiB of objects so buffered readahead cannot hide whether
	// scanning continues after ErrStop.
	var b strings.Builder
	for i := 0; i < 30; i++ {
		fmt.Fprintf(&b, `{"i":%d,"pad":%q} `, i, strings.Repeat("x", 4000))
	}
	data := []byte(b.String())

	var raws []Object
	cr := &countingReader{remaining: data}
	err := Stream(cr, func(o Object) error {
		raws = append(raws, o)
		if len(raws) == 2 {
			return ErrStop
		}
		return nil
	}, nil)
	if err != nil {
		t.Errorf("Stream returned %v, want nil for ErrStop", err)
	}
	if len(raws) != 2 {
		t.Errorf("got %d objects, want 2", len(raws))
	}
	if len(cr.remaining) == 0 {
		t.Error("remaining input was read after ErrStop")
	}
	// Only the initial buffered readahead (64 KiB) plus the stopped objects
	// may have been consumed; most of the stream must remain untouched.
	if consumed := len(data) - len(cr.remaining); consumed > 70*1024 {
		t.Errorf("consumed %d of %d bytes after ErrStop, expected only buffered readahead", consumed, len(data))
	}
}

// TestStreamReaderEquivalence checks that Reader stays a thin Stream wrapper
// producing the same normalized bytes and preserving ErrStop semantics.
func TestStreamReaderEquivalence(t *testing.T) {
	const input = `{a: 1,} [1, 2] {"b": NaN} bad {x: 'y'}`

	var viaReader, viaStream []string
	if err := Reader(strings.NewReader(input), func(b []byte) error {
		viaReader = append(viaReader, string(b))
		return nil
	}); err != nil {
		t.Fatalf("Reader error: %v", err)
	}
	if err := Stream(strings.NewReader(input), func(o Object) error {
		viaStream = append(viaStream, string(o.Raw))
		return nil
	}, nil); err != nil {
		t.Fatalf("Stream error: %v", err)
	}

	if strings.Join(viaReader, "|") != strings.Join(viaStream, "|") {
		t.Errorf("Reader %v != Stream %v", viaReader, viaStream)
	}
	want := []string{`{"a":1}`, `[1,2]`, `{"b":null}`, `{"x":"y"}`}
	if strings.Join(viaReader, "|") != strings.Join(want, "|") {
		t.Errorf("got %v, want %v", viaReader, want)
	}
}

// TestStreamLexicalAwareness verifies that brackets inside comments, strings
// and template literals do not terminate fragments, and that regex literals
// are handled across chunk boundaries.
func TestStreamLexicalAwareness(t *testing.T) {
	cases := []struct {
		name  string
		input string
		objs  []string
	}{
		{
			name:  "braces in comments and strings",
			input: `{/* comment {with braces} and [brackets] */ "k": "value } { [", n: 1} tail {after: 2}`,
			objs:  []string{`{"k":"value } { [","n":1}`, `{"after":2}`},
		},
		{
			name:  "regex literal and flags",
			input: `let re = [/ab+c/gi]; var d = {re: /x*/i}`,
			objs:  []string{`["/ab+c/gi"]`, `{"re":"/x*/i"}`},
		},
		{
			name: "line comment hides bracket until newline",
			input: `{a: 1} // { not closed
{b: 2}`,
			objs: []string{`{"a":1}`, `{"b":2}`},
		},
		{
			name:  "plain template literal",
			input: "{k: `plain {template} [text]`}",
			objs:  []string{`{"k":"plain {template} [text]"}`},
		},
		{
			name:  "nested object recovered from unsupported template substitution",
			input: "{k: `with ${ {a:1} } end`}",
			objs:  []string{`{"a":1}`},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, size := range []int{1, 2, 7, 64} {
				objs, _, err := streamObjects(&chunkReader{data: []byte(tc.input), size: size})
				if err != nil {
					t.Fatalf("chunksize %d: Stream error: %v", size, err)
				}
				if len(objs) != len(tc.objs) {
					t.Fatalf("chunksize %d: got %d objects %v, want %d %v", size, len(objs), objs, len(tc.objs), tc.objs)
				}
				for i, o := range objs {
					if string(o.Raw) != tc.objs[i] {
						t.Errorf("chunksize %d object %d = %q, want %q", size, i, o.Raw, tc.objs[i])
					}
					if string(tc.input[o.Start:o.End]) != string(o.Source) {
						t.Errorf("chunksize %d object %d offsets do not match source", size, i)
					}
				}
			}
		})
	}
}

// TestStreamObjectOffsets checks that Start/End point at the exact source
// bytes for objects separated by arbitrary text, across all chunk sizes.
func TestStreamObjectOffsets(t *testing.T) {
	parts := []string{
		"prefix text ",
		`{"a": 1}`,
		"  middle ",
		`[1, 2, 3]`,
		" suffix ",
		`{"b": {"c": 2}}`,
	}
	var input string
	for _, p := range parts {
		input += p
	}

	wantSources := []string{parts[1], parts[3], parts[5]}

	for _, size := range chunkSizes {
		objs, _, err := streamObjects(&chunkReader{data: []byte(input), size: size})
		if err != nil {
			t.Fatalf("chunksize %d: %v", size, err)
		}
		if len(objs) != len(wantSources) {
			t.Fatalf("chunksize %d: got %d objects, want %d", size, len(objs), len(wantSources))
		}
		var cursor int
		for i, want := range wantSources {
			idx := strings.Index(input[cursor:], want)
			if idx < 0 {
				t.Fatalf("could not locate %q", want)
			}
			wantStart := cursor + idx
			wantEnd := wantStart + len(want)
			cursor = wantEnd

			if objs[i].Start != int64(wantStart) || objs[i].End != int64(wantEnd) {
				t.Errorf("chunksize %d object %d offsets = [%d:%d], want [%d:%d]",
					size, i, objs[i].Start, objs[i].End, wantStart, wantEnd)
			}
		}
	}
}
