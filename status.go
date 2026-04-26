package orc

import (
	"time"
)

// WorkflowStatusType represents the current execution state of a workflow.
type WorkflowStatusType string

const (
	WorkflowStatusPending                     WorkflowStatusType = "PENDING"
	WorkflowStatusEnqueued                    WorkflowStatusType = "ENQUEUED"
	WorkflowStatusDelayed                     WorkflowStatusType = "DELAYED"
	WorkflowStatusSuccess                     WorkflowStatusType = "SUCCESS"
	WorkflowStatusError                       WorkflowStatusType = "ERROR"
	WorkflowStatusCancelled                   WorkflowStatusType = "CANCELLED"
	WorkflowStatusMaxRecoveryAttemptsExceeded WorkflowStatusType = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
)

// IsTerminal reports whether the status is one a workflow can no longer leave
// without an explicit Resume / Fork operation.
func (s WorkflowStatusType) IsTerminal() bool {
	switch s {
	case WorkflowStatusSuccess,
		WorkflowStatusError,
		WorkflowStatusCancelled,
		WorkflowStatusMaxRecoveryAttemptsExceeded:
		return true
	}
	return false
}

// WorkflowStatus contains the durable state of a workflow.
type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`
	Status             WorkflowStatusType `json:"status"`
	Name               string             `json:"name"`
	Output             any                `json:"output,omitempty"`
	Error              error              `json:"error,omitempty"`
	ExecutorID         string             `json:"executor_id"`
	ApplicationVersion string             `json:"application_version"`
	ApplicationID      string             `json:"application_id,omitempty"`
	QueueName          string             `json:"queue_name,omitempty"`
	DeduplicationID    string             `json:"deduplication_id,omitempty"`
	Priority           int                `json:"priority,omitempty"`
	Timeout            time.Duration      `json:"timeout,omitempty"`
	Deadline           time.Time          `json:"deadline,omitempty"`
	DelayUntil         time.Time          `json:"delay_until,omitempty"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	StartedAt          time.Time          `json:"started_at,omitempty"`
	Attempts           int                `json:"attempts"`
	Input              any                `json:"input,omitempty"`
	ForkedFrom         string             `json:"forked_from,omitempty"`
	ParentWorkflowID   string             `json:"parent_workflow_id,omitempty"`
	CronSchedule       string             `json:"cron_schedule,omitempty"`
}

// StepInfo describes one checkpointed step.
type StepInfo struct {
	WorkflowID      string
	StepID          int
	StepName        string
	Output          any
	Error           error
	ChildWorkflowID string
	CreatedAt       time.Time
}
