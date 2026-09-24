// Package canonjson canonicalizes JSON documents so that semantically equal
// payloads have byte-identical representations. Calibration contents are
// compared in canonical form when deciding whether adjacent segments merge.
package canonjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Normalize returns the canonical encoding of a single JSON document: object
// keys sorted, insignificant whitespace removed, number literals preserved.
// It returns an error for empty, malformed or trailing-garbage input.
func Normalize(raw []byte) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", errors.New("empty JSON document")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return "", errors.New("trailing data after JSON document")
		}
		return "", err
	}
	out, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
