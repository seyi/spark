// Package telemetry provides enhanced observability and monitoring
// Inspired by OpenTelemetry with traces, metrics, and logs
package telemetry

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Telemetry provides comprehensive observability
type TelemetryCollector struct {
	tracer  Tracer
	meter   Meter
	logger  Logger
	mu      sync.RWMutex
}

// NewTelemetryCollector creates a new telemetry collector
func NewTelemetryCollector() *TelemetryCollector {
	return &TelemetryCollector{
		tracer: NewInMemoryTracer(),
		meter:  NewInMemoryMeter(),
		logger: NewInMemoryLogger(),
	}
}

// Tracer provides distributed tracing
type Tracer interface {
	StartSpan(ctx context.Context, name string) (context.Context, Span)
	GetSpan(ctx context.Context) Span
}

// Span represents a trace span
type Span interface {
	SetAttribute(key string, value interface{})
	SetStatus(code StatusCode, message string)
	AddEvent(name string, attributes map[string]interface{})
	End()
	Context() context.Context
	TraceID() string
	SpanID() string
}

// StatusCode represents span status
type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

// Meter provides metrics collection
type Meter interface {
	Counter(name string) Counter
	Histogram(name string) Histogram
	Gauge(name string) Gauge
}

// Counter is a monotonically increasing metric
type Counter interface {
	Inc()
	Add(delta float64)
}

// Histogram records distributions
type Histogram interface {
	Record(value float64)
	RecordWithAttributes(value float64, attrs map[string]interface{})
}

// Gauge records current values
type Gauge interface {
	Set(value float64)
	Inc()
	Dec()
}

// Logger provides structured logging
type Logger interface {
	Debug(msg string, attrs map[string]interface{})
	Info(msg string, attrs map[string]interface{})
	Warn(msg string, attrs map[string]interface{})
	Error(msg string, attrs map[string]interface{})
}

// InMemoryTracer implements in-memory tracing
type InMemoryTracer struct {
	spans map[string]*InMemorySpan
	mu    sync.RWMutex
}

func NewInMemoryTracer() *InMemoryTracer {
	return &InMemoryTracer{
		spans: make(map[string]*InMemorySpan),
	}
}

type spanKeyType struct{}

var spanKey = spanKeyType{}

func (t *InMemoryTracer) StartSpan(ctx context.Context, name string) (context.Context, Span) {
	span := &InMemorySpan{
		name:       name,
		traceID:    generateID(),
		spanID:     generateID(),
		startTime:  time.Now(),
		attributes: make(map[string]interface{}),
		events:     make([]SpanEvent, 0),
		status:     StatusUnset,
	}

	t.mu.Lock()
	t.spans[span.spanID] = span
	t.mu.Unlock()

	return context.WithValue(ctx, spanKey, span), span
}

func (t *InMemoryTracer) GetSpan(ctx context.Context) Span {
	span, ok := ctx.Value(spanKey).(Span)
	if !ok {
		return nil
	}
	return span
}

// InMemorySpan implements Span
type InMemorySpan struct {
	name       string
	traceID    string
	spanID     string
	startTime  time.Time
	endTime    time.Time
	attributes map[string]interface{}
	events     []SpanEvent
	status     StatusCode
	statusMsg  string
	mu         sync.RWMutex
}

type SpanEvent struct {
	Name       string
	Timestamp  time.Time
	Attributes map[string]interface{}
}

func (s *InMemorySpan) SetAttribute(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attributes[key] = value
}

func (s *InMemorySpan) SetStatus(code StatusCode, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
	s.statusMsg = message
}

func (s *InMemorySpan) AddEvent(name string, attributes map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attributes,
	})
}

func (s *InMemorySpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endTime = time.Now()
}

func (s *InMemorySpan) Context() context.Context {
	return context.WithValue(context.Background(), spanKey, s)
}

func (s *InMemorySpan) TraceID() string { return s.traceID }
func (s *InMemorySpan) SpanID() string  { return s.spanID }

// InMemoryMeter implements in-memory metrics
type InMemoryMeter struct {
	counters   map[string]*InMemoryCounter
	histograms map[string]*InMemoryHistogram
	gauges     map[string]*InMemoryGauge
	mu         sync.RWMutex
}

func NewInMemoryMeter() *InMemoryMeter {
	return &InMemoryMeter{
		counters:   make(map[string]*InMemoryCounter),
		histograms: make(map[string]*InMemoryHistogram),
		gauges:     make(map[string]*InMemoryGauge),
	}
}

func (m *InMemoryMeter) Counter(name string) Counter {
	m.mu.Lock()
	defer m.mu.Unlock()

	if counter, exists := m.counters[name]; exists {
		return counter
	}

	counter := &InMemoryCounter{name: name}
	m.counters[name] = counter
	return counter
}

func (m *InMemoryMeter) Histogram(name string) Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()

	if histogram, exists := m.histograms[name]; exists {
		return histogram
	}

	histogram := &InMemoryHistogram{
		name:   name,
		values: make([]float64, 0),
	}
	m.histograms[name] = histogram
	return histogram
}

func (m *InMemoryMeter) Gauge(name string) Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()

	if gauge, exists := m.gauges[name]; exists {
		return gauge
	}

	gauge := &InMemoryGauge{name: name}
	m.gauges[name] = gauge
	return gauge
}

// InMemoryCounter implements Counter
type InMemoryCounter struct {
	name  string
	value float64
	mu    sync.RWMutex
}

func (c *InMemoryCounter) Inc() {
	c.Add(1.0)
}

func (c *InMemoryCounter) Add(delta float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += delta
}

func (c *InMemoryCounter) Value() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// InMemoryHistogram implements Histogram
type InMemoryHistogram struct {
	name   string
	values []float64
	mu     sync.RWMutex
}

func (h *InMemoryHistogram) Record(value float64) {
	h.RecordWithAttributes(value, nil)
}

func (h *InMemoryHistogram) RecordWithAttributes(value float64, attrs map[string]interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.values = append(h.values, value)
}

func (h *InMemoryHistogram) Statistics() *HistogramStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.values) == 0 {
		return &HistogramStats{}
	}

	var sum, min, max float64
	min = h.values[0]
	max = h.values[0]

	for _, v := range h.values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return &HistogramStats{
		Count: len(h.values),
		Sum:   sum,
		Min:   min,
		Max:   max,
		Mean:  sum / float64(len(h.values)),
	}
}

type HistogramStats struct {
	Count int
	Sum   float64
	Min   float64
	Max   float64
	Mean  float64
}

// InMemoryGauge implements Gauge
type InMemoryGauge struct {
	name  string
	value float64
	mu    sync.RWMutex
}

func (g *InMemoryGauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

func (g *InMemoryGauge) Inc() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value++
}

func (g *InMemoryGauge) Dec() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value--
}

func (g *InMemoryGauge) Value() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

// InMemoryLogger implements structured logging
type InMemoryLogger struct {
	logs []LogEntry
	mu   sync.RWMutex
}

type LogEntry struct {
	Level      LogLevel
	Message    string
	Attributes map[string]interface{}
	Timestamp  time.Time
}

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

func (l LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR"}[l]
}

func NewInMemoryLogger() *InMemoryLogger {
	return &InMemoryLogger{
		logs: make([]LogEntry, 0),
	}
}

func (l *InMemoryLogger) log(level LogLevel, msg string, attrs map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Level:      level,
		Message:    msg,
		Attributes: attrs,
		Timestamp:  time.Now(),
	}

	l.logs = append(l.logs, entry)

	// Print to console
	fmt.Printf("[%s] %s %s", level.String(), entry.Timestamp.Format(time.RFC3339), msg)
	if attrs != nil {
		fmt.Printf(" %v", attrs)
	}
	fmt.Println()
}

func (l *InMemoryLogger) Debug(msg string, attrs map[string]interface{}) {
	l.log(LogLevelDebug, msg, attrs)
}

func (l *InMemoryLogger) Info(msg string, attrs map[string]interface{}) {
	l.log(LogLevelInfo, msg, attrs)
}

func (l *InMemoryLogger) Warn(msg string, attrs map[string]interface{}) {
	l.log(LogLevelWarn, msg, attrs)
}

func (l *InMemoryLogger) Error(msg string, attrs map[string]interface{}) {
	l.log(LogLevelError, msg, attrs)
}

func (l *InMemoryLogger) GetLogs() []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logs := make([]LogEntry, len(l.logs))
	copy(logs, l.logs)
	return logs
}

// TelemetryCollector methods
func (tc *TelemetryCollector) GetTracer() Tracer {
	return tc.tracer
}

func (tc *TelemetryCollector) GetMeter() Meter {
	return tc.meter
}

func (tc *TelemetryCollector) GetLogger() Logger {
	return tc.logger
}

// Helper to generate IDs
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// MetricsSnapshot provides a point-in-time view of metrics
type MetricsSnapshot struct {
	Timestamp  time.Time
	Counters   map[string]float64
	Histograms map[string]*HistogramStats
	Gauges     map[string]float64
}

// GetMetricsSnapshot returns current metrics
func (tc *TelemetryCollector) GetMetricsSnapshot() *MetricsSnapshot {
	snapshot := &MetricsSnapshot{
		Timestamp:  time.Now(),
		Counters:   make(map[string]float64),
		Histograms: make(map[string]*HistogramStats),
		Gauges:     make(map[string]float64),
	}

	if meter, ok := tc.meter.(*InMemoryMeter); ok {
		meter.mu.RLock()
		for name, counter := range meter.counters {
			snapshot.Counters[name] = counter.Value()
		}
		for name, histogram := range meter.histograms {
			snapshot.Histograms[name] = histogram.Statistics()
		}
		for name, gauge := range meter.gauges {
			snapshot.Gauges[name] = gauge.Value()
		}
		meter.mu.RUnlock()
	}

	return snapshot
}
