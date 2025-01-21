package queue

import (
	"context"
	"expvar"
	"fmt"

	"github.com/thalesfsp/queue/internal/logging"
	"github.com/thalesfsp/queue/internal/metrics"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"
)

//////
// Vars, consts, and types.
//////

// Type is the type of the entity regarding the framework. It is used to for
// example, to identify the entity in the logs, metrics, and for tracing.
const (
	DefaultMetricCounterLabel = "counter"
	Type                      = "queue"
)

// Queue definition.
type Queue struct {
	// Logger.
	Logger sypl.ISypl `json:"-" validate:"required"`

	// Name of the queue type.
	Name string `json:"name" validate:"required,lowercase,gte=1"`

	// Metrics.
	counterPublished        *expvar.Int `json:"-" validate:"required,gte=0"`
	counterPublishedFailed  *expvar.Int `json:"-" validate:"required,gte=0"`
	counterReceived         *expvar.Int `json:"-" validate:"required,gte=0"`
	counterReceivedFailed   *expvar.Int `json:"-" validate:"required,gte=0"`
	counterSubscribed       *expvar.Int `json:"-" validate:"required,gte=0"`
	counterSubscribedFailed *expvar.Int `json:"-" validate:"required,gte=0"`
}

//////
// Implements the IMeta interface.
//////

// GetLogger returns the logger.
func (s *Queue) GetLogger() sypl.ISypl {
	return s.Logger
}

// GetName returns the queue name.
func (s *Queue) GetName() string {
	return s.Name
}

// GetType returns its type.
func (s *Queue) GetType() string {
	return Type
}

// GetCounterPublished returns the counterPublished.
func (s *Queue) GetCounterPublished() *expvar.Int {
	return s.counterPublished
}

// GetCounterPublishedFailed returns the counterPublishedFailed.
func (s *Queue) GetCounterPublishedFailed() *expvar.Int {
	return s.counterPublishedFailed
}

// GetCounterReceived returns the counterReceived.
func (s *Queue) GetCounterReceived() *expvar.Int {
	return s.counterReceived
}

// GetCounterReceivedFailed returns the counterReceivedFailed.
func (s *Queue) GetCounterReceivedFailed() *expvar.Int {
	return s.counterReceivedFailed
}

// GetCounterSubscribed returns the counterSubscribed.
func (s *Queue) GetCounterSubscribed() *expvar.Int {
	return s.counterSubscribed
}

// GetCounterSubscribedFailed returns the counterSubscribedFailed.
func (s *Queue) GetCounterSubscribedFailed() *expvar.Int {
	return s.counterSubscribedFailed
}

//////
// Factory.
//////

// New returns a new Queue.
func New(ctx context.Context, name string) (*Queue, error) {
	// Queue's individual logger.
	logger := logging.Get().New(name).SetTags(Type, name)

	a := &Queue{
		Logger: logger,
		Name:   name,

		counterPublished:        metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Published, DefaultMetricCounterLabel)),
		counterPublishedFailed:  metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Published+"."+status.Failed, DefaultMetricCounterLabel)),
		counterReceived:         metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Received, DefaultMetricCounterLabel)),
		counterReceivedFailed:   metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Received+"."+status.Failed, DefaultMetricCounterLabel)),
		counterSubscribed:       metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Subscribed, DefaultMetricCounterLabel)),
		counterSubscribedFailed: metrics.NewInt(fmt.Sprintf("%s.%s.%s.%s", Type, name, status.Subscribed+"."+status.Failed, DefaultMetricCounterLabel)),
	}

	// Validate the queue.
	if err := validation.Validate(a); err != nil {
		return nil, err
	}

	a.GetLogger().PrintlnWithOptions(level.Debug, status.Created.String())

	return a, nil
}
