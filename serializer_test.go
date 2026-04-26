package orc

import (
	"reflect"
	"testing"
)

func TestJSONSerializer_RoundtripPrimitives(t *testing.T) {
	s := jsonSerializer{}
	cases := []any{
		"hello",
		int(42),
		float64(3.14),
		true,
		[]string{"a", "b", "c"},
		map[string]int{"a": 1, "b": 2},
	}
	for _, v := range cases {
		enc, err := s.Encode(v)
		if err != nil {
			t.Fatalf("encode(%v): %v", v, err)
		}
		dst := reflect.New(reflect.TypeOf(v)).Interface()
		got, err := s.Decode(enc, dst)
		if err != nil {
			t.Fatalf("decode(%v): %v", v, err)
		}
		gotV := reflect.ValueOf(got).Elem().Interface()
		if !reflect.DeepEqual(gotV, v) {
			t.Errorf("roundtrip mismatch: got %#v want %#v", gotV, v)
		}
	}
}

func TestJSONSerializer_EncodeNil(t *testing.T) {
	s := jsonSerializer{}
	enc, err := s.Encode(nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	got, err := s.Decode(enc, nil)
	if err != nil {
		t.Fatalf("decode nil: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestJSONSerializer_DecodeWithoutDest(t *testing.T) {
	s := jsonSerializer{}
	enc, _ := s.Encode("hello")
	got, err := s.Decode(enc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %v want hello", got)
	}
}

func TestJSONSerializer_DecodeStruct(t *testing.T) {
	type Foo struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	s := jsonSerializer{}
	in := Foo{A: 1, B: "x"}
	enc, _ := s.Encode(in)
	var out Foo
	got, err := s.Decode(enc, &out)
	if err != nil {
		t.Fatal(err)
	}
	if got.(*Foo).A != 1 || got.(*Foo).B != "x" {
		t.Errorf("decoded mismatch: %+v", got)
	}
}

func TestJSONSerializer_BackwardsCompatBareJSON(t *testing.T) {
	s := jsonSerializer{}
	got, err := s.Decode(`"hello"`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %v want hello", got)
	}
}
