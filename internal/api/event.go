package api

type HandlerType string

// Event types for Tasks
const (
	OnTaskCreated   HandlerType = "on_task_created"
	OnTaskStarted   HandlerType = "on_task_started"
	OnTaskRestarted HandlerType = "on_task_restarted"
	OnSchedulerExit HandlerType = "on_scheduler_exit"
)

func (e HandlerType) String() string {
	return string(e)
}

// Event represents a task event (includes both command and task)
type Event struct {
	EventType HandlerType
	Command   Command
	Task      Task
}
