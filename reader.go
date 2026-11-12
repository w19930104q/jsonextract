package jsonextract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/tdewolff/parse/v2"
	"github.com/tdewolff/parse/v2/js"
)

// Opening Characters as defined by the JSON spec
const (
	openObject = '{'
	openArray  = '['
)

var matchingBracket = map[byte]byte{
	'{': '}',
	'[': ']',
}

// ErrStop can be returned from a JSONCallback function to indicate that processing should stop.
// When used with Reader, it will stop processing.
// When used with Objects, the callback function will never be called again (e.g. after it received the required data).
var ErrStop = errors.New("stop processing json")

// JSONCallback is the callback function passed to Reader and ObjectOptions.
//
// Any JSON objects will be passed to it as bytes as defined by the function.
//
// If this function returns an error, processing will stop and return that error.
// You can return ErrStop to make sure the function will not be called again.
type JSONCallback func([]byte) error

// Reader reads all JSON and JavaScript objects from the input and calls callback for each of them.
//
// The input is processed as a stream: at most the bytes of the object or array
// currently being scanned are held in memory. Use Stream if you additionally
// need the original bytes, byte offsets or diagnostics for invalid fragments.
//
// Errors returned from the callback will stop the method.
// The error will be returned, except if it is ErrStop which will cause the method to return nil.
//
// Please note that the reader must return UTF-8 bytes for this to work correctly.
func Reader(reader io.Reader, callback JSONCallback) error {
	if callback == nil {
		// Keep the old behaviour for a nil callback: still consume the whole
		// stream so that reader errors surface, but discard all objects.
		return Stream(reader, nil, nil)
	}

	return Stream(reader, func(o Object) error {
		return callback(o.Raw)
	}, nil)
}

var jsIdentifiers = map[string][]byte{
	"true":  []byte("true"),
	"false": []byte("false"),
	"null":  []byte("null"),
	// Special cases
	// treat undefined as null
	"undefined": []byte("null"),
	// treat NaN as null
	"NaN": []byte("null"),
}

// singleQuoteReplacer replaces a single quoted string to be double-quoted
var singleQuoteReplacer = strings.NewReplacer(
	// Replace single quotes with double, ' => "
	"'", "\"",
	// Escape quotes from before, " => \"
	"\"", "\\\"",
	// unescape single quotes from before, \' => '
	"\\'", "'",
)

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Template_literals
var templateQuoteReplacer = strings.NewReplacer(
	// Escaped quotes become normal characters
	"\\`", "`",
)

// readJSObject converts a single fragment of JavaScript object or array
// notation to JSON if possible.
// The fragment must contain exactly one complete or truncated object or array,
// as delimited by the streaming scanner.
// Please note that output might not be valid JSON and should be checked using json.Valid()
func readJSObject(fragment []byte) (output []byte, err error) {
	// Only this single fragment is materialized in memory; the surrounding
	// stream is never read into memory at once.
	lex := js.NewLexer(parse.NewInputBytes(fragment))

	// buf stores the bytes that should be returned in output
	var buf = new(bytes.Buffer)

	var (
		// since it's a dyck language, we just count the level of braces.
		// If we reach zero, we can stop parsing as we know this is the end of this object
		first      byte
		firstToken bool
		level      int
	)

	// lastByte stores the last byte we wrote to buf
	// It is used for detecting and correcting trailing commas
	var (
		lastByte  byte
		lastToken js.TokenType
	)
	var merr error
loop:
	for {
		tt, text := lex.Next()
		if tt == js.ErrorToken {
			err = lex.Err()
			break loop
		}

		// The following code assumes len(text) > 0

		// First will always be either '{' or '['
		if !firstToken {
			first = text[0]
			firstToken = true
		}

		switch {
		case isIgnoredToken(tt):
			// Ignore tokens that are not needed for JSON.
			// We must continue so they are not seen as last written byte
			continue
		case js.IsIdentifier(tt):
			// Certain keywords are reserved in JSON. As a special case,
			// we replace "undefined" with "null"
			if val, ok := jsIdentifiers[string(text)]; ok {
				// Another special case: this handles stuff like -NaN, which would
				// result in "-null", which is invalid JSON
				if lastByte == '+' || lastByte == '-' {
					buf.Truncate(buf.Len() - 1)
				}
				buf.Write(val)
			} else {
				// This is reached if we have an unquoted key in an object, e.g.
				//     { key: "value" }
				// We want to quote this identifier, as in marshal it into a string
				text, merr = json.Marshal(string(text))
				if merr != nil {
					err = merr
					break loop
				}
				buf.Write(text)
			}
		case tt == js.DivToken || tt == js.DivEqToken:
			// It is important that this comes before the IsPunctuator check
			// Basically if we find a '/', we suspect it's a regex
			tt, text = lex.RegExp()
			if tt != js.RegExpToken {
				err = fmt.Errorf("expected regex token when starting with '/', but was %s (lex err: %w)", tt.String(), lex.Err())
				break loop
			}

			// Regex patterns are just escaped and treated as strings,
			// no need to skip the entire object
			text, merr = json.Marshal(string(text))
			if merr != nil {
				err = merr
				break loop
			}
			buf.Write(text)
		case js.IsPunctuator(tt):
			if len(text) > 1 {
				err = fmt.Errorf("unexpected token %q in JS value", string(text))
				break loop
			}

			switch text[0] {
			case '{', '[':
				if text[0] == first {
					level++
				}

				if lastByte == '{' && text[0] == '{' {
					err = fmt.Errorf("opening brace { cannot come after another opening brace")
					break loop
				}

				buf.Write(text)
			case ']', '}':
				if text[0] == matchingBracket[first] {
					level--
				}

				// An array/object with trailing comma was found.
				// Example: [1, 2, 3, ]
				if lastByte == ',' {
					// We remove the comma to also support those objects.
					buf.Truncate(buf.Len() - 1)
				}

				buf.Write(text)

				// We finished the JS object that was started with `first`. Time to stop
				if level == 0 {
					break loop
				}
			case '+':
				if '0' <= lastByte && lastByte <= '9' {
					err = fmt.Errorf("cannot use '+' to add numbers/strings")
					break loop
				}
				// continue with buf.Write
				fallthrough
			default:
				// This could e.g. be a "-" in front of a number
				buf.Write(text)
			}
		case tt == js.StringToken:
			// Special quotes must be handled
			if text[0] == '\'' {
				buf.WriteString(singleQuoteReplacer.Replace(string(text)))
				// Break out of switch to continue with the lastByte assignment below
				break
			}

			if text[0] == '"' {
				// A normal string
				buf.Write(text)
				// Continue with lastByte assignment
				break
			}

			err = fmt.Errorf("unsupported string type (text: %s)", string(text))
			break loop
		case tt == js.TemplateToken:
			if len(text) <= 2 {
				err = fmt.Errorf("expected string to have at least quotes, but that didn't happen")
				break loop
			}

			var toEscape = templateQuoteReplacer.Replace(string(text[1 : len(text)-1]))

			text, merr = json.Marshal(string(toEscape))
			if merr != nil {
				err = merr
				break loop
			}

			buf.Write(text)
		case js.IsNumeric(tt):
			if js.IsNumeric(lastToken) {
				err = fmt.Errorf("invalid: writing two numbers directly after each other")
				break loop
			}
			// Not all JS numbers are valid JSON numbers, e.g. the following are valid in JS, but not JSON:
			// +5, 0x3, 0o4, 0b1001, -0x3, 8n

			// If the number starts with a '+', we already wrote it. Remove it again, as plus signs are not valid json numbers
			if lastByte == '+' {
				buf.Truncate(buf.Len() - 1)
			}

			switch tt {
			case js.IntegerToken:
				// BigIntegers can be written e.g. as "50n", "0x5n" etc.
				text = bytes.TrimSuffix(text, []byte("n"))
				fallthrough
			default:
				text = transformNumber(text)
			}

			buf.Write(text)
		default:
			// There shouldn't be much left. But in case it's valid JSON, we keep it
			buf.Write(text)
		}

		lastByte = text[len(text)-1]
		lastToken = tt
	}

	if err == nil || err == io.EOF {
		return buf.Bytes(), nil
	}
	return nil, err
}

func isIgnoredToken(tt js.TokenType) bool {
	return tt == js.WhitespaceToken || tt == js.LineTerminatorToken || tt == js.CommentToken || tt == js.CommentLineTerminatorToken
}

// transformNumber transforms the given number to a decimal number, if possible. Might return
// invalid JSON data
func transformNumber(number []byte) []byte {
	var out = make([]byte, 0, len(number))

	// "+"-Prefix is not valid JSON, just strip it
	if number[0] == '+' {
		number = number[1:]
	} else if number[0] == '-' {
		// Keep "-" sign
		number = number[1:]
		out = append(out, '-')
	}

	// Just parse the number. This also deals with leading zeros, all kinds of
	// number literals (e.g. 1_00 == 100) etc.
	ui, err := strconv.ParseUint(string(number), 0, 64)
	if err != nil {
		// If we have exactly one dot, and it is at the end
		// e.g. the number "15."" should be interpreted as "15.0"
		if number[len(number)-1] == '.' && bytes.IndexByte(number, '.') == len(number)-1 {
			return append(out, append(number, '0')...)
		}

		// this can happen if the number is a float. We just leave it as that, it should be accepted by JSON parsers
		return append(out, number...)
	}

	// Now convert to decimal
	return strconv.AppendUint(out, ui, 10)
}
