// Package events provides event tracking and telemetry
// Inspired by ADK's event system and OpenTelemetry integration
package events

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EventBus manages event publishing and subscription
type EventBus interface {
	// Subscribe registers a handler for an event type
	Subscribe(eventType EventType, handler EventHandler) error

	// Publish publishes an event to all subscribers
	Publish(event Event) error

	// Unsubscribe removes a handler
	Unsubscribe(eventType EventType, handler EventHandler) error

	// Close shuts down the event bus
	Close() error
}

// Event represents a system event
type Event interface {
	Type() EventType
	Timestamp() time.Time
	AgentID() string
	SessionID() string
	Data() interface{}
	Metadata() map[string]interface{}
}

// EventHandler processes events
type EventHandler func(ctx context.Context, event Event) error

// EventType defines the type of event
type EventType string

const (
	// Agent lifecycle events
	EventAgentRegistered EventType = "agent.registered"
	EventAgentStarted    EventType = "agent.started"
	EventAgentCompleted  EventType = "agent.completed"
	EventAgentFailed     EventType = "agent.failed"
	EventAgentPaused     EventType = "agent.paused"
	EventAgentResumed    EventType = "agent.resumed"

	// Session events
	EventSessionCreated EventType = "session.created"
	EventSessionUpdated EventType = "session.updated"
	EventSessionDeleted EventType = "session.deleted"
	EventSessionRewound EventType = "session.rewound"

	// Task scheduling events
	EventTaskScheduled EventType = "task.scheduled"
	EventTaskStarted   EventType = "task.started"
	EventTaskCompleted EventType = "task.completed"
	EventTaskFailed    EventType = "task.failed"
	EventTaskRetrying  EventType = "task.retrying"

	// Tool invocation events
	EventToolInvoked   EventType = "tool.invoked"
	EventToolCompleted EventType = "tool.completed"
	EventToolFailed    EventType = "tool.failed"

	// State management events
	EventCheckpointCreated  EventType = "checkpoint.created"
	EventCheckpointRestored EventType = "checkpoint.restored"
	EventMemoryStored       EventType = "memory.stored"
	EventMemoryRetrieved    EventType = "memory.retrieved"

	// Job events
	EventJobSubmitted EventType = "job.submitted"
	EventJobStarted   EventType = "job.started"
	EventJobCompleted EventType = "job.completed"
	EventJobFailed    EventType = "job.failed"

	// Stage events
	EventStageStarted   EventType = "stage.started"
	EventStageCompleted EventType = "stage.completed"
	EventStageFailed    EventType = "stage.failed"

	// Executor events
	EventExecutorRegistered   EventType = "executor.registered"
	EventExecutorUnregistered EventType = "executor.unregistered"
	EventExecutorHealthCheck  EventType = "executor.health_check"
)

// BaseEvent provides common event fields
type BaseEvent struct {
	EventType  EventType
	EventTime  time.Time
	Agent      string
	Session    string
	EventData  interface{}
	Meta       map[string]interface{}
}

func (b *BaseEvent) Type() EventType                     { return b.EventType }
func (b *BaseEvent) Timestamp() time.Time                { return b.EventTime }
func (b *BaseEvent) AgentID() string                     { return b.Agent }
func (b *BaseEvent) SessionID() string                   { return b.Session }
func (b *BaseEvent) Data() interface{}                   { return b.EventData }
func (b *BaseEvent) Metadata() map[string]interface{}    { return b.Meta }

// MemoryEventBus implements an in-memory event bus
type MemoryEventBus struct {
	subscribers map[EventType][]EventHandler
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewMemoryEventBus() *MemoryEventBus {
	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryEventBus{
		subscribers: make(map[EventType][]EventHandler),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (m *MemoryEventBus) Subscribe(eventType EventType, handler EventHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscribers[eventType] = append(m.subscribers[eventType], handler)
	return nil
}

func (m *MemoryEventBus) Publish(event Event) error {
	m.mu.RLock()
	handlers := m.subscribers[event.Type()]
	m.mu.RUnlock()

	// Execute handlers concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, len(handlers))

	for _, handler := range handlers {
		wg.Add(1)
		go func(h EventHandler) {
			defer wg.Done()
			if err := h(m.ctx, event); err != nil {
				errChan <- err
			}
		}(handler)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("event handling errors: %v", errors)
	}

	return nil
}

func (m *MemoryEventBus) Unsubscribe(eventType EventType, handler EventHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Note: This is a simplified implementation
	// In production, we'd need to compare function pointers
	// which is tricky in Go. Consider using handler IDs instead.

	delete(m.subscribers, eventType)
	return nil
}

func (m *MemoryEventBus) Close() error {
	m.cancel()
	return nil
}

// EventLogger is a simple event logger handler
type EventLogger struct {
	mu sync.Mutex
}

func NewEventLogger() *EventLogger {
	return &EventLogger{}
}

func (l *EventLogger) Log(ctx context.Context, event Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// In production, this would write to a proper logging system
	fmt.Printf("[%s] %s - Agent: %s, Session: %s, Data: %v\n",
		event.Timestamp().Format(time.RFC3339),
		event.Type(),
		event.AgentID(),
		event.SessionID(),
		event.Data(),
	)

	return nil
}

// EventMetrics tracks event metrics
type EventMetrics struct {
	counts map[EventType]int64
	mu     sync.RWMutex
}

func NewEventMetrics() *EventMetrics {
	return &EventMetrics{
		counts: make(map[EventType]int64),
	}
}

func (m *EventMetrics) Track(ctx context.Context, event Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.counts[event.Type()]++
	return nil
}

func (m *EventMetrics) GetCount(eventType EventType) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.counts[eventType]
}

func (m *EventMetrics) GetAllCounts() map[EventType]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := make(map[EventType]int64)
	for k, v := range m.counts {
		counts[k] = v
	}

	return counts
}

// EventStore persists events for auditing and replay
type EventStore interface {
	Store(ctx context.Context, event Event) error
	Load(ctx context.Context, filter EventFilter) ([]Event, error)
}

// EventFilter filters events for queries
type EventFilter struct {
	EventTypes []EventType
	AgentID    string
	SessionID  string
	StartTime  time.Time
	EndTime    time.Time
	Limit      int
}

// MemoryEventStore implements in-memory event persistence
type MemoryEventStore struct {
	events []Event
	mu     sync.RWMutex
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		events: make([]Event, 0),
	}
}

func (s *MemoryEventStore) Store(ctx context.Context, event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, event)
	return nil
}

func (s *MemoryEventStore) Load(ctx context.Context, filter EventFilter) ([]Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]Event, 0)

	for _, event := range s.events {
		if s.matchesFilter(event, filter) {
			results = append(results, event)
			if filter.Limit > 0 && len(results) >= filter.Limit {
				break
			}
		}
	}

	return results, nil
}

func (s *MemoryEventStore) matchesFilter(event Event, filter EventFilter) bool {
	// Check event types
	if len(filter.EventTypes) > 0 {
		match := false
		for _, et := range filter.EventTypes {
			if event.Type() == et {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}

	// Check agent ID
	if filter.AgentID != "" && event.AgentID() != filter.AgentID {
		return false
	}

	// Check session ID
	if filter.SessionID != "" && event.SessionID() != filter.SessionID {
		return false
	}

	// Check time range
	if !filter.StartTime.IsZero() && event.Timestamp().Before(filter.StartTime) {
		return false
	}

	if !filter.EndTime.IsZero() && event.Timestamp().After(filter.EndTime) {
		return false
	}

	return true
}

// EventRecorder combines event bus with storage
type EventRecorder struct {
	bus   EventBus
	store EventStore
}

func NewEventRecorder(bus EventBus, store EventStore) *EventRecorder {
	recorder := &EventRecorder{
		bus:   bus,
		store: store,
	}

	// Subscribe to all event types to store them
	recorder.bus.Subscribe("*", recorder.recordEvent)

	return recorder
}

func (r *EventRecorder) recordEvent(ctx context.Context, event Event) error {
	return r.store.Store(ctx, event)
}

func (r *EventRecorder) Publish(event Event) error {
	return r.bus.Publish(event)
}

func (r *EventRecorder) Query(ctx context.Context, filter EventFilter) ([]Event, error) {
	return r.store.Load(ctx, filter)
}

// Convenience functions for creating events

func NewAgentEvent(eventType EventType, agentID, sessionID string, data interface{}) *BaseEvent {
	return &BaseEvent{
		EventType:  eventType,
		EventTime:  time.Now(),
		Agent:      agentID,
		Session:    sessionID,
		EventData:  data,
		Meta:       make(map[string]interface{}),
	}
}

func NewTaskEvent(eventType EventType, taskID, jobID string, data interface{}) *BaseEvent {
	return &BaseEvent{
		EventType:  eventType,
		EventTime:  time.Now(),
		Agent:      "",
		Session:    "",
		EventData:  data,
		Meta: map[string]interface{}{
			"task_id": taskID,
			"job_id":  jobID,
		},
	}
}

func NewToolEvent(eventType EventType, toolName, agentID string, data interface{}) *BaseEvent {
	return &BaseEvent{
		EventType:  eventType,
		EventTime:  time.Now(),
		Agent:      agentID,
		Session:    "",
		EventData:  data,
		Meta: map[string]interface{}{
			"tool_name": toolName,
		},
	}
}
