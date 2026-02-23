// Package logstorage provides delete task management for VictoriaLogs.
//
// DELETE TASKS OVERVIEW:
//
// Delete tasks allow asynchronous deletion of log entries matching a LogsQL filter.
// They are designed to:
//   - Survive application restarts (persisted to delete_tasks.json)
//   - Process sequentially to limit resource usage
//   - Be cancellable via API
//   - Retry on transient failures
//
// WHY DELETE TASKS ARE ASYNC:
//
// Log deletion is expensive because it requires:
//  1. Scanning all matching log entries across partitions
//  2. Rewriting affected parts with the deleted entries removed
//  3. This merge operation can take minutes to hours for large datasets
//
// Making deletion synchronous would tie up HTTP connections and risk timeouts.
// The async model allows the caller to start a task and check progress later.
//
// DELETE TASK LIFECYCLE:
//
//  1. Client calls DELETE /delete/execute?filter=... via vlselect
//  2. vlselect creates a unique taskID and calls Storage.DeleteRunTask()
//  3. Task is persisted to delete_tasks.json (survives crashes)
//  4. Background watcher (watchDeleteTasks) picks up the task
//  5. Task is processed: scan partitions, merge parts with filter applied
//  6. On success: task removed from list, file updated
//  7. On failure: task moved to end of queue for retry
//
// PERSISTENCE:
//
// The delete_tasks.json file contains a JSON array of task definitions.
// It is updated atomically (via fs.MustWriteAtomic) after every modification.
// On startup, MustOpenStorage loads any pending tasks from this file.
//
// TIME BOUNDING:
//
// Each task records a StartTime. The deletion query is time-bounded to this time,
// meaning only logs that existed when the delete request was made are affected.
// This prevents accidentally deleting logs ingested after the delete request.
package logstorage

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// DeleteTask describes a task for logs' deletion.
//
// A delete task represents an asynchronous request to delete all log entries
// matching a specific LogsQL filter within certain tenant(s).
//
// FIELDS:
//   - TaskID: Unique identifier provided by the client. Used for status queries
//     and cancellation. Should be unique across all tasks.
//   - TenantIDs: The tenants whose data should be searched for matching logs.
//     In single-tenant mode, this is typically empty or contains a single ID.
//   - Filter: The LogsQL filter expression. All logs matching this filter
//     will be marked for deletion (actually removed during merge).
//   - StartTime: When the delete request was created. The deletion query
//     is bounded to this time, preventing deletion of logs ingested after
//     the request was made.
//   - ctx/cancel/doneCh: Internal fields used during task execution.
//     nil when task is pending (not yet started).
type DeleteTask struct {
	// TaskID is the id of the task
	TaskID string `json:"task_id"`

	// TenantIDs are tenant ids for the task
	TenantIDs []TenantID `json:"tenant_ids"`

	// Filter is the filter used for logs' deletion; Logs matching the given filter are deleted
	Filter string `json:"filter"`

	// StartTime is the time when the task has been created
	StartTime time.Time `json:"start_time"`

	// ctx is set to non-nil during task execution. Pending tasks have nil ctx.
	// It is derived from Storage.stopCh to allow graceful shutdown.
	ctx context.Context

	// cancel is set to non-nil during task execution. It is used for canceling the delete task.
	// Called when DeleteStopTask is invoked or when storage is shutting down.
	cancel func()

	// doneCh is used for waiting until the delete task is complete.
	// Closed when the task finishes (success, cancellation, or failure).
	// DeleteStopTask waits on this channel to block until task is fully stopped.
	doneCh chan struct{}
}

// String returns string representation for the dt
// This is used for logging and debugging.
func (dt *DeleteTask) String() string {
	data, err := json.Marshal(dt)
	if err != nil {
		logger.Panicf("BUG: cannot marshal DeleteTask: %s", err)
	}
	return string(data)
}

// newDeleteTask creates a new delete task with the given parameters.
//
// The startTime should be the Unix nanosecond timestamp when the delete
// request was received. This is used to bound the deletion query.
func newDeleteTask(taskID string, tenantIDs []TenantID, filter string, startTime int64) *DeleteTask {
	return &DeleteTask{
		TaskID:    taskID,
		TenantIDs: tenantIDs,
		Filter:    filter,
		StartTime: time.Unix(0, startTime).UTC(),
	}
}

// MarshalDeleteTasksToJSON marshals tasks into a JSON array and returns the result.
// Used for persisting tasks to delete_tasks.json
func MarshalDeleteTasksToJSON(tasks []*DeleteTask) []byte {
	data, err := json.Marshal(tasks)
	if err != nil {
		logger.Panicf("BUG: cannot marshal tasks: %s", err)
	}
	return data
}

// UnmarshalDeleteTasksFromJSON unmarshals DeleteTask slice from JSON array at data.
// Used for loading tasks from delete_tasks.json on startup.
func UnmarshalDeleteTasksFromJSON(data []byte) ([]*DeleteTask, error) {
	var tasks []*DeleteTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// mustReadDeleteTasksFromFile loads delete tasks from the given file path.
// Returns nil if the file doesn't exist (no pending tasks).
// Panics on read/parse errors since this indicates corruption.
func mustReadDeleteTasksFromFile(path string) []*DeleteTask {
	if !fs.IsPathExist(path) {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Panicf("FATAL: cannot read %s: %s", path, err)
	}
	dts, err := UnmarshalDeleteTasksFromJSON(data)
	if err != nil {
		logger.Panicf("FATAL: cannot parse delete tasks from %s: %s", path, err)
	}
	return dts
}

// mustWriteDeleteTasksToFile atomically writes the task list to the given file.
// Uses fs.MustWriteAtomic to ensure the file is never partially written.
// This is called after every task list modification (create, complete, cancel).
func mustWriteDeleteTasksToFile(path string, dts []*DeleteTask) {
	data := MarshalDeleteTasksToJSON(dts)
	fs.MustWriteAtomic(path, data, true)
}
