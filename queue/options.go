package queue

import (
	"context"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/validation"
)

//////
// Vars, consts, and types.
//////

var (
	// ErrRequiredPostHook is the error returned when the post-hook function is
	// missing.
	ErrRequiredPostHook = customerror.NewRequiredError("post-hook function", customerror.WithErrorCode("ERR_REQUIRED_POST_HOOK"))

	// ErrRequiredPreHook is the error returned when the pre-hook function is
	// missing.
	ErrRequiredPreHook = customerror.NewRequiredError("pre-hook function", customerror.WithErrorCode("ERR_REQUIRED_PRE_HOOK"))
)

// HookFunc specifies the function that will be called before and after the
// operation.
type HookFunc func(ctx context.Context, q IQueue, queueName string, m *Message) error

// OptionsFunc allows to set options.
type OptionsFunc func(o *Options) error

// Options for operations.
type Options struct {
	// QueueName name.
	QueueName string `json:"queueName"`

	// PreHookFunc is the function which runs before the operation.
	PreHookFunc HookFunc `json:"-"`

	// PostHookFunc is the function which runs after the operation.
	PostHookFunc HookFunc `json:"-"`
}

//////
// Exported built-in options.
//////

// WithPreHook set the pre-hook function.
func WithPreHook(fn HookFunc) OptionsFunc {
	return func(o *Options) error {
		if fn == nil {
			return ErrRequiredPreHook
		}

		o.PreHookFunc = fn

		return nil
	}
}

// WithPostHook set the post-hook function.
func WithPostHook(fn HookFunc) OptionsFunc {
	return func(o *Options) error {
		if fn == nil {
			return ErrRequiredPostHook
		}

		o.PostHookFunc = fn

		return nil
	}
}

// WithQueueName sets the queue name.
func WithQueueName(queueName string) OptionsFunc {
	return func(o *Options) error {
		o.QueueName = queueName

		return nil
	}
}

// NewOptions creates Options.
func NewOptions() (*Options, error) {
	o := &Options{}

	if err := validation.Validate(o); err != nil {
		return nil, err
	}

	return o, nil
}
