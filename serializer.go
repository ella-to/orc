package orc

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Serializer encodes/decodes values that pass through the durable boundary
// (workflow inputs/outputs, step outputs, messages, events). The default
// implementation uses encoding/json with type information embedded in a
// wrapper struct so that round-tripping `any` values preserves their concrete
// type for known registered types.
//
// Custom serializers can be plugged in via Config.Serializer.
type Serializer interface {
	// Encode returns a string representation of v that can be persisted in a
	// TEXT column. Returning an empty string for nil values is fine.
	Encode(v any) (string, error)
	// Decode reverses Encode. The destination type is provided by the caller
	// (via a non-nil pointer) when known; pass nil to decode into `any`.
	Decode(s string, into any) (any, error)
}

// jsonSerializer is the default Serializer.
//
// It wraps the value in a small envelope of the form
//
//	{"v": <value>}
//
// to allow nil values and to keep room for future metadata (type tag, etc.)
// without breaking the wire format.
type jsonSerializer struct{}

type jsonEnvelope struct {
	V json.RawMessage `json:"v"`
}

func (jsonSerializer) Encode(v any) (string, error) {
	if v == nil {
		return `{"v":null}`, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return "", wrapError(ErrSerialization, err, "encode")
	}
	env := jsonEnvelope{V: raw}
	out, err := json.Marshal(env)
	if err != nil {
		return "", wrapError(ErrSerialization, err, "encode envelope")
	}
	return string(out), nil
}

func (jsonSerializer) Decode(s string, into any) (any, error) {
	if s == "" {
		return nil, nil
	}
	var env jsonEnvelope
	if err := json.Unmarshal([]byte(s), &env); err != nil {
		// Backwards-friendly: also accept a bare JSON value.
		if into != nil {
			if err2 := json.Unmarshal([]byte(s), into); err2 == nil {
				return into, nil
			}
		}
		var any2 any
		if err2 := json.Unmarshal([]byte(s), &any2); err2 == nil {
			return any2, nil
		}
		return nil, wrapError(ErrSerialization, err, "decode")
	}
	if len(env.V) == 0 || string(env.V) == "null" {
		return nil, nil
	}
	if into != nil {
		dec := json.NewDecoder(bytes.NewReader(env.V))
		if err := dec.Decode(into); err != nil {
			return nil, wrapError(ErrSerialization, err, "decode into %T", into)
		}
		return into, nil
	}
	var v any
	if err := json.Unmarshal(env.V, &v); err != nil {
		return nil, wrapError(ErrSerialization, err, "decode any")
	}
	return v, nil
}

// MustEncode panics on serialization failure; useful in tests/options.
func MustEncode(s Serializer, v any) string {
	out, err := s.Encode(v)
	if err != nil {
		panic(fmt.Sprintf("orc: serialize: %v", err))
	}
	return out
}
