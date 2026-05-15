package orc

import (
	"context"
	"time"
)

// systemDatabase is the persistence boundary that ORC sits on top of. Every
// operation that needs durability flows through one of these methods. Having
// it as an interface keeps the workflow engine independent of the storage
// backend (SQLite today, but could be swapped for an in-memory store in
// tests, or for a different SQL engine later).
type systemDatabase interface {
	// Lifecycle.
	close(ctx context.Context) error

	// Workflow status.
	insertWorkflow(ctx context.Context, in insertWorkflowInput) (insertWorkflowResult, error)
	getWorkflowStatus(ctx context.Context, workflowID string, loadIO bool) (*WorkflowStatus, error)
	listWorkflows(ctx context.Context, in listWorkflowsInput) ([]WorkflowStatus, error)
	countWorkflows(ctx context.Context, in listWorkflowsInput) (int, error)
	updateWorkflowStatus(ctx context.Context, in updateWorkflowStatusInput) error
	cancelWorkflow(ctx context.Context, workflowID string) error
	resumeWorkflow(ctx context.Context, workflowID string) error
	deleteWorkflows(ctx context.Context, ids []string) error
	gcWorkflows(ctx context.Context, in listWorkflowsInput) (int, error)
	forkWorkflow(ctx context.Context, in forkWorkflowInput) (string, error)

	// Steps.
	checkStepOutput(ctx context.Context, workflowID string, functionID int) (*stepRecord, error)
	recordStepOutput(ctx context.Context, in recordStepInput) error
	listSteps(ctx context.Context, workflowID string) ([]StepInfo, error)

	// Notifications & events.
	enqueueNotification(ctx context.Context, in enqueueNotificationInput) error
	popNotification(ctx context.Context, destinationID, topic string) (*notificationRecord, error)
	setEvent(ctx context.Context, in setEventInput) error
	getEvent(ctx context.Context, workflowID, key string) (*eventRecord, error)

	// Queue dispatching.
	dequeueWorkflows(ctx context.Context, in dequeueInput) ([]string, error)
	recordQueueDispatch(ctx context.Context, queueName string, workflowIDs []string) error
	countQueueDispatches(ctx context.Context, queueName string, since time.Time) (int, error)
	countActiveQueueWorkflows(ctx context.Context, queueName, executorID string) (int, error)
	purgeQueueDispatches(ctx context.Context, before time.Time) (int, error)

	// Recovery.
	listPendingWorkflows(ctx context.Context, executorID string) ([]WorkflowStatus, error)
}

// ----- input/output structs for systemDatabase methods -----

type insertWorkflowInput struct {
	Status             WorkflowStatus
	MaxRecoveryAttempt int
}

type insertWorkflowResult struct {
	// Status reflects the row that ended up in the DB. If a row already
	// existed for this ID, this is the existing row. Callers must inspect
	// AlreadyExisted to decide whether to schedule execution.
	Status         WorkflowStatus
	AlreadyExisted bool
}

type updateWorkflowStatusInput struct {
	WorkflowID    string
	Status        WorkflowStatusType
	Output        *string
	ErrorString   *string
	ResetAttempts bool
	BumpAttempts  bool
	StartedAtMs   int64
	Now           time.Time
}

type listWorkflowsInput struct {
	WorkflowIDs       []string
	Status            []WorkflowStatusType
	WorkflowName      string
	QueueName         string
	ExecutorID        string
	StartTime         time.Time
	EndTime           time.Time
	// UpdatedBefore filters by updated_at < UpdatedBefore. Distinct from
	// EndTime (which filters by created_at) because GC and "stale" queries
	// care about when a workflow last changed state, not when it was first
	// created.
	UpdatedBefore     time.Time
	Limit             int
	Offset            int
	SortDescending    bool
	LoadInputOutput   bool
	WithChildren      bool
	ExcludeQueueNames []string
}

type stepRecord struct {
	WorkflowID      string
	FunctionID      int
	FunctionName    string
	Output          string
	HasOutput       bool
	ErrorString     string
	ChildWorkflowID string
	CreatedAt       time.Time
}

type recordStepInput struct {
	WorkflowID      string
	FunctionID      int
	FunctionName    string
	Output          *string // nil means no output (e.g. error path)
	ErrorString     *string
	ChildWorkflowID string
}

type enqueueNotificationInput struct {
	DestinationID string
	Topic         string
	Message       string
	MessageUUID   string
}

type notificationRecord struct {
	Topic     string
	Message   string
	CreatedAt time.Time
}

type setEventInput struct {
	WorkflowID string
	Key        string
	Value      string
}

type eventRecord struct {
	Value     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type dequeueInput struct {
	QueueName    string
	Limit        int
	ExecutorID   string
	AppVersion   string
	WithPriority bool
}

type forkWorkflowInput struct {
	OriginalWorkflowID string
	StartFromStep      int
	NewWorkflowID      string
}
