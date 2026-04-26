package orc

import (
	"errors"
	"testing"
)

func TestErrorIs(t *testing.T) {
	a := newError(ErrWorkflowNotFound, "x")
	if !errors.Is(a, ErrWorkflowNotFoundErr) {
		t.Fatal("expected Is to match by code")
	}
	if errors.Is(a, ErrInitializationFailed) {
		t.Fatal("must not match different code")
	}
}

func TestIsCode(t *testing.T) {
	wrapped := wrapError(ErrSerialization, errors.New("inner"), "outer")
	if !IsCode(wrapped, ErrSerialization) {
		t.Fatal("expected IsCode true")
	}
	if IsCode(wrapped, ErrUnknown) {
		t.Fatal("expected IsCode false")
	}
}

func TestErrorUnwrap(t *testing.T) {
	inner := errors.New("inner")
	wrapped := wrapError(ErrSerialization, inner, "x")
	if !errors.Is(wrapped, inner) {
		t.Fatal("expected wrapped error chain to contain inner")
	}
}
