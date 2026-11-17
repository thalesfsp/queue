package customapm_test

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/queue/internal/customapm"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

func TestTraceErrorEndedSpan(t *testing.T) {
	t.Setenv("ELASTIC_APM_ACTIVE", "true")
	t.Setenv("ELASTIC_APM_TRANSACTION_SAMPLE_RATE", "1")
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()
	tx := tracer.StartTransaction("ended", "test")
	ctx := apm.ContextWithTransaction(t.Context(), tx)
	span, ctx := apm.StartSpan(ctx, "ended", "test")
	span.End()
	tx.End()

	sentinel := errors.New("callback failed")
	wrapped := fmt.Errorf("processing: %w", sentinel)
	var counter expvar.Int
	require.NotPanics(t, func() {
		require.Same(t, wrapped, customapm.TraceError(ctx, wrapped, nil, &counter))
	})
	require.Equal(t, int64(1), counter.Value())
	tracer.Flush(nil)
	require.Len(t, tracer.Payloads().Errors, 1)
}

func TestTraceErrorActiveSpanOutcome(t *testing.T) {
	t.Setenv("ELASTIC_APM_ACTIVE", "true")
	t.Setenv("ELASTIC_APM_TRANSACTION_SAMPLE_RATE", "1")
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()
	tx, spans, events := tracer.WithTransaction(func(ctx context.Context) {
		span, ctx := apm.StartSpan(ctx, "active", "test")
		require.Error(t, customapm.TraceError(ctx, errors.New("callback failed"), nil, nil))
		span.End()
	})
	require.Equal(t, "failure", tx.Outcome)
	require.Len(t, spans, 1)
	require.Equal(t, "failure", spans[0].Outcome)
	require.Len(t, events, 1)
}

func TestTraceErrorConcurrentSpanEnd(t *testing.T) {
	t.Setenv("ELASTIC_APM_ACTIVE", "true")
	t.Setenv("ELASTIC_APM_TRANSACTION_SAMPLE_RATE", "1")
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()
	tx := tracer.StartTransaction("concurrent", "test")
	ctx := apm.ContextWithTransaction(t.Context(), tx)
	for range 32 {
		span, spanCtx := apm.StartSpan(ctx, "concurrent", "test")
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Go(func() { <-start; span.End() })
		workers.Go(func() { <-start; _ = customapm.TraceError(spanCtx, errors.New("failed"), nil, nil) })
		close(start)
		workers.Wait()
	}
	tx.End()
	tracer.Flush(nil)
	require.Len(t, tracer.Payloads().Errors, 32)
}

func TestTraceErrorWithoutSpan(t *testing.T) {
	sentinel := errors.New("no span")
	require.NotPanics(t, func() {
		require.Same(t, sentinel, customapm.TraceError(t.Context(), sentinel, nil, nil))
	})
}
