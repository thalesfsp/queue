# Critical findings plan: queue (base 8466bba324dff92e1d9db9689664331627502e5b)

This is the requested critical review and fix record. C-1 and C-2 are implemented in the local fix branch with hermetic unit regressions; publication awaits the parent review checkpoint. Original evidence below refers to the audited base SHA, before these fixes.

Implementation: `TraceError` delegates span failure marking to APM's synchronized `CaptureError` API. Public `Subscribe` uses an in-memory-testable AMQP registration boundary and invokes an independently owned transaction/span for every delivery, preserving caller cancellation and non-tracing values. Callback timeouts resolve positive common → positive backend → five seconds; the helper leaves the common timeout unset so backend customizations take effect.

Permanent regressions: `internal/customapm/error_test.go` and `rabbitmq/consumer_test.go`. They cover ended/concurrently ending spans, active failure outcomes, public post-return subscriptions, callback/Ack/Nack errors, inactive APM, ended caller/delivery spans, cancellation, timeout defaults and overrides. No real broker is used.

Negative controls in the external evidence directory restore the original `TraceError`, the original outer-only timeout read, and inherited delivery tracing. Each fails for its actual defect; restored fixes pass under `-race`. An early constructor-only check incorrectly assumed the common field should duplicate the backend default; parent review refuted that representation, so that control is superseded and is not accepted regression evidence.

Hosted integration follow-up: validator v10.30.6 copies a live `amqp.Channel` while probing its `Valuer` interface, racing the connection reader. Factory validation now checks a snapshot of required Queue/Client/Config reference presence; `queue.New` has already validated the embedded queue fields. Required-reference tests preserve nil rejection, and `TestValidateLiveClient` races harmless in-memory notification registration against validation without a broker. Restoring whole-object reflection reproduces `WARNING: DATA RACE`. No validation policy, race detector or hosted integration gate is disabled.

Evidence and probe logs are in `/Volumes/Dev/Dev/critical-review-20261116/queue/`:

- `probe-repo/probe/critical_probe_test.go` is the probe source.
- `logs/probe-critical.log` is the run with the APM agent's default active setting.
- `logs/probe-critical-apm-inactive.log` is the precondition control.

---

## C-1: Sampled post-return RabbitMQ callback errors can crash the process

**Severity: CRITICAL.** One handler error ends the service process. The failed message is Nacked with requeue before the
panic, so it is redelivered on restart and the crash repeats. Nothing recovers in between.

**Source:**

- `rabbitmq/rabbitmq.go:98-104`: `ctx, span := customapm.Trace(...)` is followed by `defer span.End()`. The span ends
  when `Subscribe` returns.
- `rabbitmq/rabbitmq.go:196-240`: the consumer goroutine captures that same `ctx`. It calls `customapm.TraceError(ctx, ...)`
  at line 217 for every callback error, and at lines 208 and 234 when Nack or Ack fails.
- `internal/customapm/error.go:59-63`: `span := apm.SpanFromContext(ctx); if span != nil { span.Outcome = ... }`.
- `go.elastic.co/apm@v1.15.0/span.go:270-272`: the `*SpanData` "is set to nil when the span's End method is called".
  `span.go:417` does `s.SpanData = nil`. `Outcome` is a promoted field of that nil embedded pointer, so the write
  dereferences nil.

**Trigger:** `q.Subscribe(ctx, "jobs", func(ctx context.Context, m *queue.Message) error { return errors.New("bad payload") }, rabbitmq.NewSubscribeParams())`.
Then publish one message. The APM agent defaults apply: active and sampled. No APM server or env vars are needed.

**Evidence:**

- The probe runs the exact sequence `customapm.Trace` → `span.End()` → `customapm.TraceError(ctx, err, nil, nil)`, as in
  the post-return goroutine path. It got `runtime error: invalid memory address or nil pointer dereference`.
- Negative control: the same `TraceError` call while the span is still open does not panic.
- Precondition control: with `ELASTIC_APM_ACTIVE=false` the span is dropped and is not placed in the context
  (`apm/gocontext.go:114-116`), so no panic occurs. The defect is therefore present under the agent's out-of-the-box
  defaults.
- Control flow: `Subscribe` returns right after `go func()`; only a log line and a counter run before it. A delivery
  processed after that return uses the ended span and its error branch panics. A delivery processed earlier is subject
  to the scheduling of `span.End()`; concurrent error tracing can race with the span ending.
- Why the tests missed it: the only `Subscribe` test is integration-only (`rabbitmq_test.go:16`). Its callback always
  returns nil, so the error branch never runs.

**Root cause:** a request-scoped span and its context are captured by a long-lived goroutine and used after the span ends.

**Implemented bounded fix:**

1. Each delivery owns a transaction and span ending after callback/acknowledgement error handling. Both inherited APM context values are replaced while cancellation and other caller context values remain intact.
2. Remove the unsynchronized promoted `span.Outcome` write. `apm.CaptureError` marks failure with APM's locks and tolerates ended spans; no racy `SpanData` guard is introduced.

**Regression and negative controls:**

- `TestTraceErrorEndedSpan` fails with the original nil-pointer panic; active outcomes and concurrent `Span.End` are tested as well.
- `TestSubscribeDeliveryLifetime` exercises public `Subscribe` with deliveries sent after it returns. It verifies acknowledgement/requeue counts, errors, independent ended delivery spans, cancellation and context values.
- Restoring inherited tracing fails with “ended caller span inherited” and “delivery span ended before callback”. Ack/Nack failure, inactive APM and a handler ending its own span are bypass cases.

---

## C-2: Default subscribe parameters give every callback an already-expired context

**Severity: CRITICAL.** With the documented defaults, any handler that respects its context fails on every message.
Examples are `http.NewRequestWithContext` + `Do`, `sql.DB.QueryContext` and `select` on `ctx.Done()`. Each failure is
Nacked with `requeue=true` (line 206), so the message loops on redelivery and is never processed. Together with C-1, the
first such failure crashes the process.

**Source:**

- `rabbitmq/rabbitmq.go:130-145`: when `prm == nil` the code builds `&queue.SubscribeParams{Any: NewDefaultSubscribeParams()}`.
  The outer `ContextTimeout` is 0.
- `rabbitmq/params.go:48-61`: the 5s default is set on the inner embedded `SubscribeParams` of the casted struct, not on
  the outer `prm`. `params.go:65-69`: `NewSubscribeParams()` likewise leaves the outer `ContextTimeout` at 0.
- `rabbitmq/rabbitmq.go:198`: `context.WithTimeout(ctx, prm.ContextTimeout)` reads the outer field. For a duration of 0
  or less the stdlib returns a context that is already cancelled with `DeadlineExceeded`.

**Trigger:** `q.Subscribe(ctx, "jobs", handler, rabbitmq.NewSubscribeParams())`, or `nil` params, where the handler does
`req, _ := http.NewRequestWithContext(ctx, ...); _, err := http.DefaultClient.Do(req); return err`.

**Evidence:** the probe printed `prm.ContextTimeout=0s casted.ContextTimeout=5s` and `callback ctx.Err() at creation = context deadline exceeded`
for both `NewSubscribeParams()` and the nil-defaulted path. Control: an explicit outer `ContextTimeout: 5s` gives a live
context. The integration test's callback ignores its context (`func(_ context.Context, ...)`), which is why the defect
was not caught.

**Root cause:** the timeout default is written to one field (the implementation's embedded params), and the timeout is
read from a different field (the generic outer params).

**Implemented bounded fix:**

- Resolve a positive common timeout first, then a positive backend timeout, then the five-second default. `NewSubscribeParams` keeps the common field at zero (unset); its backend supplies the five-second default. Backend edits made through this helper remain effective, while an explicitly positive common timeout takes precedence. Public types are unchanged.

**Regression and negative controls:**

- `TestNewSubscribeParamsTimeout` preserves zero-as-unset; the effective default is five seconds. The public-path regression covers helper defaults and helper-plus-backend edits.
- `TestSubscribeTimeoutWiring` exercises public `Subscribe` for nil/helper/empty params, backend edits through the helper, explicit common overrides, precedence and negative values. Restoring the original outer-only timeout read fails with `context deadline exceeded`.
- `TestSubscribeTimeout` covers resolution directly; default constructors retain all documented RabbitMQ flags. Three bypass groups (handler/ack errors with ended tracing, inactive APM/cancellation, negative and nested timeout overrides) pass without a broker.
