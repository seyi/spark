package telemetry

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewTelemetryCollector(t *testing.T) {
	collector := NewTelemetryCollector()

	if collector == nil {
		t.Fatal("Collector should not be nil")
	}

	if collector.GetTracer() == nil {
		t.Error("Tracer not initialized")
	}

	if collector.GetMeter() == nil {
		t.Error("Meter not initialized")
	}

	if collector.GetLogger() == nil {
		t.Error("Logger not initialized")
	}
}

func TestInMemoryTracer(t *testing.T) {
	tracer := NewInMemoryTracer()
	ctx := context.Background()

	t.Run("Start Span", func(t *testing.T) {
		spanCtx, span := tracer.StartSpan(ctx, "test-span")

		if span == nil {
			t.Fatal("Span should not be nil")
		}

		if spanCtx == nil {
			t.Fatal("Span context should not be nil")
		}

		if span.TraceID() == "" {
			t.Error("Trace ID should not be empty")
		}

		if span.SpanID() == "" {
			t.Error("Span ID should not be empty")
		}
	})

	t.Run("Get Span from Context", func(t *testing.T) {
		spanCtx, createdSpan := tracer.StartSpan(ctx, "test-span")

		retrievedSpan := tracer.GetSpan(spanCtx)

		if retrievedSpan == nil {
			t.Fatal("Should retrieve span from context")
		}

		if retrievedSpan.SpanID() != createdSpan.SpanID() {
			t.Error("Retrieved span does not match created span")
		}
	})

	t.Run("Get Span from Empty Context", func(t *testing.T) {
		span := tracer.GetSpan(context.Background())

		if span != nil {
			t.Error("Should return nil for empty context")
		}
	})
}

func TestInMemorySpan(t *testing.T) {
	tracer := NewInMemoryTracer()
	ctx := context.Background()

	t.Run("Set Attribute", func(t *testing.T) {
		_, span := tracer.StartSpan(ctx, "test-span")

		span.SetAttribute("key1", "value1")
		span.SetAttribute("key2", 123)

		// Access through type assertion for testing
		if memSpan, ok := span.(*InMemorySpan); ok {
			memSpan.mu.RLock()
			if memSpan.attributes["key1"] != "value1" {
				t.Error("String attribute not set correctly")
			}
			if memSpan.attributes["key2"] != 123 {
				t.Error("Integer attribute not set correctly")
			}
			memSpan.mu.RUnlock()
		}
	})

	t.Run("Set Status", func(t *testing.T) {
		_, span := tracer.StartSpan(ctx, "test-span")

		span.SetStatus(StatusOK, "Success")

		if memSpan, ok := span.(*InMemorySpan); ok {
			memSpan.mu.RLock()
			if memSpan.status != StatusOK {
				t.Error("Status not set correctly")
			}
			if memSpan.statusMsg != "Success" {
				t.Error("Status message not set correctly")
			}
			memSpan.mu.RUnlock()
		}
	})

	t.Run("Add Event", func(t *testing.T) {
		_, span := tracer.StartSpan(ctx, "test-span")

		attrs := map[string]interface{}{
			"event_key": "event_value",
		}
		span.AddEvent("test-event", attrs)

		if memSpan, ok := span.(*InMemorySpan); ok {
			memSpan.mu.RLock()
			if len(memSpan.events) != 1 {
				t.Error("Event not added")
			}
			if memSpan.events[0].Name != "test-event" {
				t.Error("Event name not set correctly")
			}
			memSpan.mu.RUnlock()
		}
	})

	t.Run("End Span", func(t *testing.T) {
		_, span := tracer.StartSpan(ctx, "test-span")

		time.Sleep(10 * time.Millisecond)
		span.End()

		if memSpan, ok := span.(*InMemorySpan); ok {
			memSpan.mu.RLock()
			if memSpan.endTime.IsZero() {
				t.Error("End time not set")
			}
			if !memSpan.endTime.After(memSpan.startTime) {
				t.Error("End time should be after start time")
			}
			memSpan.mu.RUnlock()
		}
	})

	t.Run("Span Context", func(t *testing.T) {
		_, span := tracer.StartSpan(ctx, "test-span")

		spanCtx := span.Context()
		if spanCtx == nil {
			t.Error("Span context should not be nil")
		}

		retrievedSpan := tracer.GetSpan(spanCtx)
		if retrievedSpan == nil {
			t.Error("Should retrieve span from span context")
		}
	})
}

func TestInMemoryMeter(t *testing.T) {
	meter := NewInMemoryMeter()

	t.Run("Counter", func(t *testing.T) {
		counter := meter.Counter("test-counter")

		if counter == nil {
			t.Fatal("Counter should not be nil")
		}

		counter.Inc()
		counter.Add(5.0)

		if c, ok := counter.(*InMemoryCounter); ok {
			if c.Value() != 6.0 {
				t.Errorf("Expected counter value 6.0, got %f", c.Value())
			}
		}
	})

	t.Run("Counter Reuse", func(t *testing.T) {
		counter1 := meter.Counter("reuse-counter")
		counter1.Inc()

		counter2 := meter.Counter("reuse-counter")
		if c, ok := counter2.(*InMemoryCounter); ok {
			if c.Value() != 1.0 {
				t.Error("Should reuse existing counter")
			}
		}
	})

	t.Run("Histogram", func(t *testing.T) {
		histogram := meter.Histogram("test-histogram")

		if histogram == nil {
			t.Fatal("Histogram should not be nil")
		}

		histogram.Record(10.0)
		histogram.Record(20.0)
		histogram.Record(30.0)

		if h, ok := histogram.(*InMemoryHistogram); ok {
			stats := h.Statistics()

			if stats.Count != 3 {
				t.Errorf("Expected count 3, got %d", stats.Count)
			}

			if stats.Sum != 60.0 {
				t.Errorf("Expected sum 60.0, got %f", stats.Sum)
			}

			if stats.Min != 10.0 {
				t.Errorf("Expected min 10.0, got %f", stats.Min)
			}

			if stats.Max != 30.0 {
				t.Errorf("Expected max 30.0, got %f", stats.Max)
			}

			if stats.Mean != 20.0 {
				t.Errorf("Expected mean 20.0, got %f", stats.Mean)
			}
		}
	})

	t.Run("Histogram with Attributes", func(t *testing.T) {
		histogram := meter.Histogram("test-histogram-attrs")

		attrs := map[string]interface{}{
			"method": "GET",
			"status": 200,
		}

		histogram.RecordWithAttributes(100.0, attrs)

		if h, ok := histogram.(*InMemoryHistogram); ok {
			stats := h.Statistics()
			if stats.Count != 1 {
				t.Error("Should record with attributes")
			}
		}
	})

	t.Run("Gauge", func(t *testing.T) {
		gauge := meter.Gauge("test-gauge")

		if gauge == nil {
			t.Fatal("Gauge should not be nil")
		}

		gauge.Set(100.0)
		gauge.Inc()
		gauge.Dec()

		if g, ok := gauge.(*InMemoryGauge); ok {
			if g.Value() != 100.0 {
				t.Errorf("Expected gauge value 100.0, got %f", g.Value())
			}
		}
	})
}

func TestInMemoryLogger(t *testing.T) {
	logger := NewInMemoryLogger()

	t.Run("Log Levels", func(t *testing.T) {
		logger.Debug("debug message", map[string]interface{}{"key": "value"})
		logger.Info("info message", nil)
		logger.Warn("warn message", nil)
		logger.Error("error message", nil)

		logs := logger.GetLogs()

		if len(logs) != 4 {
			t.Errorf("Expected 4 log entries, got %d", len(logs))
		}

		if logs[0].Level != LogLevelDebug {
			t.Error("First log should be DEBUG level")
		}

		if logs[1].Level != LogLevelInfo {
			t.Error("Second log should be INFO level")
		}

		if logs[2].Level != LogLevelWarn {
			t.Error("Third log should be WARN level")
		}

		if logs[3].Level != LogLevelError {
			t.Error("Fourth log should be ERROR level")
		}
	})

	t.Run("Log Content", func(t *testing.T) {
		logger2 := NewInMemoryLogger()

		attrs := map[string]interface{}{
			"user_id": "123",
			"action":  "login",
		}

		logger2.Info("User logged in", attrs)

		logs := logger2.GetLogs()

		if len(logs) != 1 {
			t.Fatal("Expected 1 log entry")
		}

		if logs[0].Message != "User logged in" {
			t.Error("Log message not set correctly")
		}

		if logs[0].Attributes["user_id"] != "123" {
			t.Error("Log attributes not preserved")
		}
	})

	t.Run("Log Timestamps", func(t *testing.T) {
		logger3 := NewInMemoryLogger()

		before := time.Now()
		logger3.Info("test", nil)
		after := time.Now()

		logs := logger3.GetLogs()

		if logs[0].Timestamp.Before(before) || logs[0].Timestamp.After(after) {
			t.Error("Log timestamp not in expected range")
		}
	})
}

func TestLogLevel(t *testing.T) {
	levels := []struct {
		level    LogLevel
		expected string
	}{
		{LogLevelDebug, "DEBUG"},
		{LogLevelInfo, "INFO"},
		{LogLevelWarn, "WARN"},
		{LogLevelError, "ERROR"},
	}

	for _, tc := range levels {
		if tc.level.String() != tc.expected {
			t.Errorf("Expected level string '%s', got '%s'", tc.expected, tc.level.String())
		}
	}
}

func TestMetricsSnapshot(t *testing.T) {
	collector := NewTelemetryCollector()

	// Create some metrics
	counter := collector.GetMeter().Counter("requests")
	counter.Inc()
	counter.Inc()

	histogram := collector.GetMeter().Histogram("latency")
	histogram.Record(100.0)
	histogram.Record(200.0)

	gauge := collector.GetMeter().Gauge("connections")
	gauge.Set(42.0)

	// Get snapshot
	snapshot := collector.GetMetricsSnapshot()

	if snapshot == nil {
		t.Fatal("Snapshot should not be nil")
	}

	if snapshot.Counters["requests"] != 2.0 {
		t.Errorf("Expected counter value 2.0, got %f", snapshot.Counters["requests"])
	}

	if snapshot.Histograms["latency"].Count != 2 {
		t.Error("Histogram not captured in snapshot")
	}

	if snapshot.Gauges["connections"] != 42.0 {
		t.Errorf("Expected gauge value 42.0, got %f", snapshot.Gauges["connections"])
	}

	if snapshot.Timestamp.IsZero() {
		t.Error("Snapshot timestamp not set")
	}
}

func TestConcurrentCounter(t *testing.T) {
	meter := NewInMemoryMeter()
	counter := meter.Counter("concurrent-counter")

	const numGoroutines = 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter.Inc()
		}()
	}

	wg.Wait()

	if c, ok := counter.(*InMemoryCounter); ok {
		if c.Value() != float64(numGoroutines) {
			t.Errorf("Expected counter value %d, got %f", numGoroutines, c.Value())
		}
	}
}

func TestConcurrentHistogram(t *testing.T) {
	meter := NewInMemoryMeter()
	histogram := meter.Histogram("concurrent-histogram")

	const numRecords = 100
	var wg sync.WaitGroup

	for i := 0; i < numRecords; i++ {
		wg.Add(1)
		go func(val float64) {
			defer wg.Done()
			histogram.Record(val)
		}(float64(i))
	}

	wg.Wait()

	if h, ok := histogram.(*InMemoryHistogram); ok {
		stats := h.Statistics()
		if stats.Count != numRecords {
			t.Errorf("Expected %d records, got %d", numRecords, stats.Count)
		}
	}
}

func TestConcurrentGauge(t *testing.T) {
	meter := NewInMemoryMeter()
	gauge := meter.Gauge("concurrent-gauge")

	const numOperations = 100
	var wg sync.WaitGroup

	gauge.Set(0)

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gauge.Inc()
		}()
	}

	wg.Wait()

	if g, ok := gauge.(*InMemoryGauge); ok {
		if g.Value() != float64(numOperations) {
			t.Errorf("Expected gauge value %d, got %f", numOperations, g.Value())
		}
	}
}

func TestConcurrentLogger(t *testing.T) {
	logger := NewInMemoryLogger()

	const numLogs = 100
	var wg sync.WaitGroup

	for i := 0; i < numLogs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			logger.Info("Concurrent log", map[string]interface{}{
				"index": idx,
			})
		}(i)
	}

	wg.Wait()

	logs := logger.GetLogs()
	if len(logs) != numLogs {
		t.Errorf("Expected %d logs, got %d", numLogs, len(logs))
	}
}

func TestConcurrentTracer(t *testing.T) {
	tracer := NewInMemoryTracer()
	ctx := context.Background()

	const numSpans = 50
	var wg sync.WaitGroup

	for i := 0; i < numSpans; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, span := tracer.StartSpan(ctx, "concurrent-span")
			span.SetAttribute("index", idx)
			span.End()
		}(i)
	}

	wg.Wait()

	tracer.mu.RLock()
	if len(tracer.spans) != numSpans {
		t.Errorf("Expected %d spans, got %d", numSpans, len(tracer.spans))
	}
	tracer.mu.RUnlock()
}

func TestStatusCodes(t *testing.T) {
	codes := []StatusCode{StatusUnset, StatusOK, StatusError}

	for _, code := range codes {
		tracer := NewInMemoryTracer()
		_, span := tracer.StartSpan(context.Background(), "test")
		span.SetStatus(code, "test message")

		if memSpan, ok := span.(*InMemorySpan); ok {
			memSpan.mu.RLock()
			if memSpan.status != code {
				t.Errorf("Status code not set correctly: expected %d, got %d", code, memSpan.status)
			}
			memSpan.mu.RUnlock()
		}
	}
}

func TestHistogramStatisticsEmpty(t *testing.T) {
	histogram := &InMemoryHistogram{
		name:   "empty",
		values: make([]float64, 0),
	}

	stats := histogram.Statistics()

	if stats.Count != 0 {
		t.Error("Empty histogram should have count 0")
	}

	if stats.Sum != 0 {
		t.Error("Empty histogram should have sum 0")
	}
}

func TestHistogramStatisticsSingleValue(t *testing.T) {
	histogram := &InMemoryHistogram{
		name:   "single",
		values: []float64{42.0},
	}

	stats := histogram.Statistics()

	if stats.Count != 1 {
		t.Error("Expected count 1")
	}

	if stats.Min != 42.0 || stats.Max != 42.0 || stats.Mean != 42.0 {
		t.Error("Min, Max, and Mean should all be 42.0 for single value")
	}
}

func TestGenerateID(t *testing.T) {
	id1 := generateID()
	time.Sleep(1 * time.Nanosecond)
	id2 := generateID()

	if id1 == "" || id2 == "" {
		t.Error("Generated IDs should not be empty")
	}

	if id1 == id2 {
		t.Error("Generated IDs should be unique")
	}
}

func TestTelemetryIntegration(t *testing.T) {
	// Integration test combining tracer, meter, and logger
	collector := NewTelemetryCollector()
	ctx := context.Background()

	// Start a span
	spanCtx, span := collector.GetTracer().StartSpan(ctx, "operation")
	span.SetAttribute("operation", "test")

	// Record metrics
	counter := collector.GetMeter().Counter("operations")
	counter.Inc()

	histogram := collector.GetMeter().Histogram("operation_duration")
	histogram.Record(100.0)

	// Log
	collector.GetLogger().Info("Operation started", map[string]interface{}{
		"trace_id": span.TraceID(),
	})

	// End span
	span.SetStatus(StatusOK, "Success")
	span.End()

	// Verify
	snapshot := collector.GetMetricsSnapshot()
	if snapshot.Counters["operations"] != 1.0 {
		t.Error("Counter not recorded")
	}

	if snapshot.Histograms["operation_duration"].Count != 1 {
		t.Error("Histogram not recorded")
	}

	if logger, ok := collector.GetLogger().(*InMemoryLogger); ok {
		logs := logger.GetLogs()
		if len(logs) == 0 {
			t.Error("Log not recorded")
		}
	}

	// Verify span
	retrievedSpan := collector.GetTracer().GetSpan(spanCtx)
	if retrievedSpan == nil {
		t.Error("Span not retrievable from context")
	}
}

func TestGaugeDecrement(t *testing.T) {
	meter := NewInMemoryMeter()
	gauge := meter.Gauge("test-gauge-dec")

	gauge.Set(10.0)
	gauge.Dec()
	gauge.Dec()
	gauge.Dec()

	if g, ok := gauge.(*InMemoryGauge); ok {
		if g.Value() != 7.0 {
			t.Errorf("Expected gauge value 7.0, got %f", g.Value())
		}
	}
}

func TestSpanEventOrdering(t *testing.T) {
	tracer := NewInMemoryTracer()
	_, span := tracer.StartSpan(context.Background(), "test")

	span.AddEvent("event1", nil)
	time.Sleep(1 * time.Millisecond)
	span.AddEvent("event2", nil)
	time.Sleep(1 * time.Millisecond)
	span.AddEvent("event3", nil)

	if memSpan, ok := span.(*InMemorySpan); ok {
		memSpan.mu.RLock()
		if len(memSpan.events) != 3 {
			t.Fatalf("Expected 3 events, got %d", len(memSpan.events))
		}

		// Verify chronological order
		if !memSpan.events[1].Timestamp.After(memSpan.events[0].Timestamp) {
			t.Error("Events not in chronological order")
		}

		if !memSpan.events[2].Timestamp.After(memSpan.events[1].Timestamp) {
			t.Error("Events not in chronological order")
		}
		memSpan.mu.RUnlock()
	}
}
