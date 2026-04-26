package orc

import (
	"errors"
	"fmt"
)

// ErrorCode classifies all ORC errors so callers can match on `errors.Is`.
type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
	ErrInitialization
	ErrWorkflowNotRegistered
	ErrWorkflowNotFound
	ErrWorkflowConflict
	ErrWorkflowAlreadyTerminal
	ErrWorkflowAwaitTimeout
	ErrWorkflowCancelled
	ErrWorkflowTimedOut
	ErrMaxRecoveryAttemptsExceeded
	ErrStepNotFound
	ErrSerialization
	ErrDuplicate
	ErrQueueNotFound
	ErrConflictingInput
	ErrNotInWorkflow
	ErrAwaitedWorkflowFailed
)

// Error is the canonical ORC error type. It carries a code and a free-form
// message and supports errors.Is / errors.As for matching.
type Error struct {
	Code    ErrorCode
	Message string
	Wrapped error
}

func (e *Error) Error() string {
	if e.Wrapped != nil {
		return fmt.Sprintf("orc(%d): %s: %v", e.Code, e.Message, e.Wrapped)
	}
	return fmt.Sprintf("orc(%d): %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Wrapped }

// Is allows comparing by error code via sentinel errors below.
func (e *Error) Is(target error) bool {
	if other, ok := target.(*Error); ok {
		return e.Code == other.Code
	}
	return false
}

func newError(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func wrapError(code ErrorCode, err error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Wrapped: err}
}

// Sentinel errors usable with errors.Is.
var (
	ErrInitializationFailed       = &Error{Code: ErrInitialization, Message: "initialization failed"}
	ErrWorkflowNotRegisteredErr   = &Error{Code: ErrWorkflowNotRegistered, Message: "workflow is not registered"}
	ErrWorkflowNotFoundErr        = &Error{Code: ErrWorkflowNotFound, Message: "workflow not found"}
	ErrWorkflowConflictErr        = &Error{Code: ErrWorkflowConflict, Message: "workflow id already exists with different inputs"}
	ErrWorkflowAlreadyTerminalErr = &Error{Code: ErrWorkflowAlreadyTerminal, Message: "workflow is already in a terminal state"}
	ErrWorkflowAwaitTimeoutErr    = &Error{Code: ErrWorkflowAwaitTimeout, Message: "timed out awaiting workflow result"}
	ErrWorkflowCancelledErr       = &Error{Code: ErrWorkflowCancelled, Message: "workflow was cancelled"}
	ErrWorkflowTimedOutErr        = &Error{Code: ErrWorkflowTimedOut, Message: "workflow timed out"}
	ErrMaxAttemptsErr             = &Error{Code: ErrMaxRecoveryAttemptsExceeded, Message: "max recovery attempts exceeded"}
	ErrStepNotFoundErr            = &Error{Code: ErrStepNotFound, Message: "step not found"}
	ErrSerializationErr           = &Error{Code: ErrSerialization, Message: "serialization failed"}
	ErrDuplicateErr               = &Error{Code: ErrDuplicate, Message: "duplicate"}
	ErrQueueNotFoundErr           = &Error{Code: ErrQueueNotFound, Message: "queue not registered"}
	ErrConflictingInputErr        = &Error{Code: ErrConflictingInput, Message: "conflicting input for existing workflow"}
	ErrNotInWorkflowErr           = &Error{Code: ErrNotInWorkflow, Message: "operation must be called inside a workflow"}
	ErrAwaitedWorkflowFailedErr   = &Error{Code: ErrAwaitedWorkflowFailed, Message: "awaited workflow failed"}
)

// IsCode reports whether err is (or wraps) an *Error with the given code.
func IsCode(err error, code ErrorCode) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == code
	}
	return false
}
