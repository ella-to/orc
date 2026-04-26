package orc

import "github.com/google/uuid"

// newUUID returns a fresh UUIDv4 string.
func newUUID() string { return uuid.NewString() }
