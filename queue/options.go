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
type HookFunc[P, S any] func(ctx context.Context, q IQueue[P, S], queueName string, m *Message) error

// OptionsFunc allows to set options.
type OptionsFunc[P, S any] func(o *Options[P, S]) error

// Options for operations.
type Options[P, S any] struct {
	// QueueName name.
	QueueName string `json:"queueName"`

	// PreHookFunc is the function which runs before the operation.
	PreHookFunc HookFunc[P, S] `json:"-"`

	// PostHookFunc is the function which runs after the operation.
	PostHookFunc HookFunc[P, S] `json:"-"`
}

//////
// Exported built-in options.
//////

// WithPreHook set the pre-hook function.
func WithPreHook[P, S any](fn HookFunc[P, S]) OptionsFunc[P, S] {
	return func(o *Options[P, S]) error {
		if fn == nil {
			return ErrRequiredPreHook
		}

		o.PreHookFunc = fn

		return nil
	}
}

// WithPostHook set the post-hook function.
func WithPostHook[P, S any](fn HookFunc[P, S]) OptionsFunc[P, S] {
	return func(o *Options[P, S]) error {
		if fn == nil {
			return ErrRequiredPostHook
		}

		o.PostHookFunc = fn

		return nil
	}
}

// WithQueueName sets the queue name.
func WithQueueName[P, S any](queueName string) OptionsFunc[P, S] {
	return func(o *Options[P, S]) error {
		o.QueueName = queueName

		return nil
	}
}

// NewOptions creates Options.
func NewOptions[P, S any]() (*Options[P, S], error) {
	o := &Options[P, S]{}

	if err := validation.Validate(o); err != nil {
		return nil, err
	}

	return o, nil
}
