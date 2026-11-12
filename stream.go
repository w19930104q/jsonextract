package jsonextract

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// scanMode describes how the next bytes of a fragment should be interpreted.
type scanMode uint8

const (
	modeCode scanMode = iota
	modeLineComment
	modeBlockComment
	modeString
	modeTemplate
	modeRegex
)

// lexFrame is a lexical context of the scanner: a string, template literal,
// comment or regular expression. Bracket nesting is tracked separately by a
// root-type depth counter, so brackets of the non-root type never interfere
// with fragment termination.
type lexFrame struct {
	mode  scanMode
	quote byte // quote byte for modeString
}

// substitution tracks one active "${" template substitution. braces counts
// unmatched '{' brackets inside the substitution so that its closing '}'
// can be recognized even when it contains nested objects.
type substitution struct {
	braces int
}

// Object is one successfully extracted JSON object or array.
//
// Raw contains valid JSON bytes converted from the original JavaScript notation,
// it is exactly what Reader passes to its callback.
//
// Source contains the original bytes of the object or array as they appeared
// in the input stream. Start and End are byte offsets relative to the start of
// the input and describe the half-open interval [Start, End) of Source,
// so End-Start == len(Source).
//
// The byte slices are only valid until the callback returns; copy them if you
// need to retain them for longer.
type Object struct {
	Raw    []byte
	Source []byte
	Start  int64
	End    int64
}

// FragmentError describes a fragment that started with '{' or '[' but could not
// be extracted as a valid object or array, e.g. because it is never terminated,
// a string is not closed or the converted result is not valid JSON.
//
// A fragment error does not stop extraction: bytes after the failed opening
// bracket are rescanned and later valid objects are still returned.
//
// Start and End are byte offsets of the consumed input relative to the start
// of the stream. Fragment contains the original source bytes that were consumed
// while attempting the fragment. It is nil if the information is not available.
type FragmentError struct {
	Start    int64
	End      int64
	Fragment []byte
	Err      error
}

// Error implements the error interface.
func (e *FragmentError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("jsonextract: invalid fragment at bytes %d-%d", e.Start, e.End)
	}
	return fmt.Sprintf("jsonextract: invalid fragment at bytes %d-%d: %s", e.Start, e.End, e.Err.Error())
}

// Unwrap returns the underlying error so that errors.Is and errors.As work.
func (e *FragmentError) Unwrap() error {
	return e.Err
}

// StreamOption configures the Stream function.
type StreamOption struct {
	// MaxObjects limits the number of successfully extracted objects returned
	// from Stream. Once the limit is reached, extraction stops immediately and
	// the remaining input is not read or scanned. A value of zero means no limit.
	MaxObjects int
}

// Stream reads JSON and JavaScript objects and arrays from r in the order they
// appear and calls object for each successfully extracted one.
//
// Unlike reading the whole input into memory first, Stream only keeps the
// bytes of the fragment that is currently being scanned in memory. This makes
// it suitable for inputs that are much larger than available memory, as long as
// a single object or array still fits.
//
// object may return ErrStop to stop extraction without error. Any other error
// is returned and extraction stops. If object is nil, extracted objects are
// discarded.
//
// fragmentError, if not nil, is called for fragments that cannot be extracted.
// Already returned objects are not rolled back, and extraction continues by
// rescanning the bytes following the failed opening bracket. It may return
// ErrStop to stop.
//
// Reader is a backwards-compatible wrapper around Stream that only receives
// the normalized JSON bytes.
func Stream(r io.Reader, object func(Object) error, fragmentError func(FragmentError) error, opts ...StreamOption) error {
	var maxObjects int
	for _, opt := range opts {
		if opt.MaxObjects > maxObjects {
			maxObjects = opt.MaxObjects
		}
	}

	s := &streamer{
		br:         bufio.NewReaderSize(r, 64*1024),
		identStart: -1,
	}

	var objectCount int

	// streamErr stores a non-EOF error of the underlying reader. Buffered bytes
	// of a failed fragment are replayed first, once the replay is drained the
	// stored error is returned.
	var streamErr error

	for {
		// Once buffered replay bytes of a failed fragment are exhausted, the
		// underlying reader error that interrupted the stream is returned.
		if streamErr != nil && s.replay.Len() == 0 && len(s.pb) == 0 {
			return streamErr
		}

		b, err := s.next()
		if err != nil {
			if !s.inFragment() {
				// Outside a fragment, reader errors (including io.EOF) surface
				// directly to the caller as in the original Reader behavior.
				if err == io.EOF {
					return nil
				}
				return err
			}

			// Inside a fragment the current fragment cannot be completed. It is
			// reported as a fragment error and its bytes are replayed so that
			// complete nested objects can still be recovered.
			cause := err
			if err == io.EOF {
				cause = io.ErrUnexpectedEOF
			}
			if ferr := s.fail(cause, fragmentError); ferr != nil {
				if ferr == ErrStop {
					return nil
				}
				return ferr
			}

			// Remember the underlying failure. The replay buffer still holds the
			// fragment interior, which is scanned for complete nested objects.
			if err != io.EOF {
				streamErr = err
			}
			if s.replay.Len() > 0 {
				continue
			}
			return streamErr
		}

		if !s.inFragment() {
			// We're scanning for the next opening bracket. Anything else is
			// skipped; brackets inside strings of surrounding JavaScript are
			// attempted as fragments and rejected during validation, just like
			// the original implementation did.
			if b == openObject || b == openArray {
				s.startFragment(b)
			}
			continue
		}

		// Everything inside a fragment is retained so it can be replayed when
		// the fragment turns out to be invalid.
		s.cand.WriteByte(b)

		if ferr := s.handleByte(b, fragmentError); ferr != nil {
			if ferr == ErrStop {
				return nil
			}
			return ferr
		}

		if !s.inFragment() && s.cand != nil {
			// A complete candidate fragment was scanned, validate and emit it.
			frag := s.cand.Bytes()

			msg, verr := readJSObject(frag)
			if verr != nil || !json.Valid(msg) {
				if verr == nil {
					verr = errors.New("converted fragment is not valid JSON")
				}
				if ferr := s.fail(verr, fragmentError); ferr != nil {
					if ferr == ErrStop {
						return nil
					}
					return ferr
				}
				continue
			}

			if object != nil {
				obj := Object{
					Raw:    msg,
					Source: append([]byte(nil), frag...),
					Start:  s.start,
					End:    s.curPos,
				}

				if oerr := object(obj); oerr != nil {
					if oerr == ErrStop {
						return nil
					}
					return oerr
				}
			}

			objectCount++
			s.resetFragment()

			if maxObjects > 0 && objectCount >= maxObjects {
				// Do not scan any further input.
				return nil
			}
		}
	}
}

// streamer is the streaming fragment scanner.
type streamer struct {
	br *bufio.Reader

	// replay holds bytes of a failed fragment that must be scanned again.
	// Reads from replay are served before reading from br.
	replay bytes.Buffer

	// pb is a small pushback stack used for one-byte lookahead.
	pb []byte

	// curPos is the absolute byte position of the next byte to read,
	// absPos the number of bytes ever read from the underlying reader.
	curPos int64
	absPos int64

	// lexStack holds active string/template/comment/regex contexts.
	lexStack []lexFrame

	// root is the opening bracket of the current fragment. depth counts only
	// brackets of the root type, so brackets of the other type never close the
	// fragment. subs tracks active template substitutions.
	root  byte
	depth int
	subs  []substitution

	// cand accumulates the bytes of the fragment currently being scanned.
	cand  *bytes.Buffer
	start int64

	// afterValue is true when the last significant token ends a primary
	// expression, which means a following '/' is division rather than a regex.
	afterValue bool

	// identStart is the offset in cand where the current identifier starts,
	// or -1 when no identifier is being scanned.
	identStart int
}

// inFragment reports whether a fragment is currently being scanned.
func (s *streamer) inFragment() bool {
	return s.depth > 0
}

func (s *streamer) startFragment(b byte) {
	s.start = s.curPos - 1
	s.cand = new(bytes.Buffer)
	s.cand.WriteByte(b)
	s.root = b
	s.depth = 1
	s.lexStack = s.lexStack[:0]
	s.subs = s.subs[:0]
	s.afterValue = false
	s.identStart = -1
}

// next reads the next byte and advances the stream position.
func (s *streamer) next() (b byte, err error) {
	if n := len(s.pb); n > 0 {
		b = s.pb[n-1]
		s.pb = s.pb[:n-1]
		s.curPos++
		return b, nil
	}

	if s.replay.Len() > 0 {
		b, err = s.replay.ReadByte()
		if err == nil {
			s.curPos++
			return b, nil
		}
	}

	b, err = s.br.ReadByte()
	if err != nil {
		return 0, err
	}
	s.absPos++
	s.curPos = s.absPos
	return b, nil
}

// unread pushes b back so that the next call to next returns it again.
func (s *streamer) unread(b byte) {
	s.pb = append(s.pb, b)
	s.curPos--
}

// peek returns the next byte without consuming it.
func (s *streamer) peek() (byte, bool) {
	b, err := s.next()
	if err != nil {
		return 0, false
	}
	s.unread(b)
	return b, true
}

// consume reads one more byte of the current fragment and keeps the candidate
// buffer in sync. It returns false at end of input.
func (s *streamer) consume() bool {
	c, err := s.next()
	if err != nil {
		return false
	}
	s.cand.WriteByte(c)
	return true
}

func (s *streamer) pushMode(mode scanMode, quote byte) {
	s.lexStack = append(s.lexStack, lexFrame{mode: mode, quote: quote})
}

func (s *streamer) popMode() lexFrame {
	n := len(s.lexStack)
	f := s.lexStack[n-1]
	s.lexStack = s.lexStack[:n-1]
	return f
}

func (s *streamer) top() *lexFrame {
	return &s.lexStack[len(s.lexStack)-1]
}

// resetFragment drops all state of the fragment that was just processed.
func (s *streamer) resetFragment() {
	s.lexStack = s.lexStack[:0]
	s.root = 0
	s.depth = 0
	s.subs = s.subs[:0]
	s.cand = nil
	s.afterValue = false
	s.identStart = -1
}

// fail reports an invalid fragment and resynchronizes the scanner: all bytes
// after the opening bracket are replayed so that objects nested in or following
// the failed fragment can still be found.
func (s *streamer) fail(cause error, onError func(FragmentError) error) error {
	var frag []byte
	if s.cand != nil {
		frag = append([]byte(nil), s.cand.Bytes()...)
	}

	fe := FragmentError{
		Start:    s.start,
		End:      s.curPos,
		Fragment: frag,
		Err:      cause,
	}

	if onError != nil {
		if err := onError(fe); err != nil {
			return err
		}
	}

	if s.cand != nil && s.cand.Len() > 1 {
		// Re-scan everything after the opening bracket. The interior has to be
		// rescanned before any bytes still waiting in replay, which happen to be
		// the remainder of an outer failed fragment: appending it after them would
		// reorder the source bytes.
		interior := append([]byte(nil), s.cand.Bytes()[1:]...)
		remaining := append([]byte(nil), s.replay.Bytes()...)
		s.replay.Reset()
		s.replay.Write(interior)
		s.replay.Write(remaining)

		// The first replayed byte is the byte right after the opening bracket,
		// i.e. input position start+1. curPos holds the position of the next byte
		// to read, so it has to be one ahead of that.
		s.curPos = s.start + 1
	}

	s.resetFragment()
	return nil
}

// handleByte processes one byte of a fragment according to the lexical mode
// on top of the stack.
func (s *streamer) handleByte(b byte, onError func(FragmentError) error) error {
	if len(s.lexStack) == 0 {
		return s.handleCode(b, onError)
	}

	switch s.top().mode {
	case modeLineComment:
		if b == '\n' {
			s.popMode()
		}
		return nil
	case modeBlockComment:
		if b == '*' {
			if c, ok := s.peek(); ok && c == '/' {
				s.consume()
				s.popMode()
			}
		}
		return nil
	case modeString:
		return s.handleString(b)
	case modeTemplate:
		return s.handleTemplate(b)
	case modeRegex:
		return s.handleRegex(b, onError)
	}
	return nil
}

// handleString handles bytes inside a single or double quoted string.
func (s *streamer) handleString(b byte) error {
	if b == '\\' {
		// Consume the escaped byte, an escaped byte never closes the string.
		s.consume()
		return nil
	}

	if b == s.top().quote {
		s.popMode()
		s.afterValue = true
		s.identStart = -1
	}
	return nil
}

// handleTemplate handles bytes inside a backtick template literal.
func (s *streamer) handleTemplate(b byte) error {
	if b == '\\' {
		s.consume()
		return nil
	}

	if b == '`' {
		s.popMode()
		s.afterValue = true
		s.identStart = -1
		return nil
	}

	if b == '$' {
		if c, ok := s.peek(); ok && c == '{' {
			s.consume()
			// The substitution is scanned in code mode until its matching '}'.
			s.subs = append(s.subs, substitution{})
			s.afterValue = false
			s.identStart = -1
		}
	}
	return nil
}

// handleRegex handles bytes inside a regular expression literal.
func (s *streamer) handleRegex(b byte, onError func(FragmentError) error) error {
	if b == '\\' {
		s.consume()
		return nil
	}

	if b == '\n' || b == '\r' {
		// A line terminator terminates a regex without a closing slash.
		return s.fail(errors.New("unterminated regular expression literal"), onError)
	}

	if b == '/' {
		s.popMode()
		// Consume any regex flags, e.g. the "g" in /test/g.
		for {
			c, ok := s.peek()
			if !ok || !isIdentContinue(c) {
				break
			}
			s.consume()
		}
		s.afterValue = true
		s.identStart = -1
	}
	return nil
}

// handleCode handles a byte in normal JavaScript code context.
func (s *streamer) handleCode(b byte, onError func(FragmentError) error) error {
	// Finish a pending identifier before processing the new byte so that
	// keywords allowing a regex literal afterwards can be detected.
	if s.identStart >= 0 && !isIdentContinue(b) {
		word := s.cand.Bytes()[s.identStart:]
		s.afterValue = !isRegexAllowedKeyword(string(word))
		s.identStart = -1
	}

	switch b {
	case '"', '\'':
		s.pushMode(modeString, b)
		return nil
	case '`':
		s.pushMode(modeTemplate, b)
		return nil
	case '/':
		c, ok := s.peek()
		if ok && c == '/' {
			s.consume()
			s.pushMode(modeLineComment, 0)
			return nil
		}
		if ok && c == '*' {
			s.consume()
			s.pushMode(modeBlockComment, 0)
			return nil
		}
		if s.afterValue {
			// Division operator, nothing special to do.
			s.afterValue = false
			return nil
		}
		s.pushMode(modeRegex, 0)
		return nil
	case '{':
		if len(s.subs) > 0 {
			// Nested object inside a template substitution.
			s.subs[len(s.subs)-1].braces++
		} else if s.root == '{' {
			s.depth++
		}
		s.afterValue = false
		return nil
	case '[':
		if s.root == '[' {
			s.depth++
		}
		s.afterValue = false
		return nil
	case '}':
		if n := len(s.subs); n > 0 {
			if s.subs[n-1].braces > 0 {
				s.subs[n-1].braces--
			} else {
				// This closes the template substitution, return to template mode.
				s.subs = s.subs[:n-1]
			}
			s.afterValue = true
			return nil
		}
		if s.root == '{' {
			s.depth--
			s.afterValue = true
			s.identStart = -1
		}
		// A bracket of a different type never matches, keep scanning.
		return nil
	case ']':
		if s.root == '[' {
			s.depth--
			s.afterValue = true
			s.identStart = -1
		}
		return nil
	}

	switch {
	case b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\v' || b == '\f':
		// Whitespace does not change the significant-token state.
		return nil
	case isIdentStart(b):
		if s.identStart < 0 {
			s.identStart = s.cand.Len() - 1
		}
		s.afterValue = true
		return nil
	case b >= '0' && b <= '9':
		s.afterValue = true
		return nil
	case b == ')' || b == ']':
		s.afterValue = true
	default:
		// Operators and punctuation end primary expressions.
		s.afterValue = false
	}
	return nil
}

func isIdentStart(b byte) bool {
	return b == '$' || b == '_' || b == '\\' || b >= 0x80 || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isIdentContinue(b byte) bool {
	return isIdentStart(b) || (b >= '0' && b <= '9')
}

// isRegexAllowedKeyword reports whether an identifier is a keyword after which
// a regular expression literal is allowed in JavaScript.
func isRegexAllowedKeyword(word string) bool {
	switch word {
	case "break", "case", "catch", "continue", "debugger", "delete", "do",
		"else", "finally", "in", "instanceof", "new", "of", "return", "throw",
		"typeof", "void", "yield", "await", "as":
		return true
	}
	return false
}
