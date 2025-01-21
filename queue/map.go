package queue

import (
	"context"

	"github.com/thalesfsp/concurrentloop"
)

//////
// Vars, consts, and types.
//////

// NewMap returns a new map of IQueue.
func NewMap[P, S any]() map[string]IQueue[P, S] {
	return make(map[string]IQueue[P, S])
}

//////
// Methods.
//////

// String implements the Stringer interface.
func String[P, S any](m map[string]IQueue[P, S]) string {
	// Iterate over the map and print the queue name.
	// Output: "1, 2, 3"
	var s string

	for k := range m {
		s += k + ", "
	}

	// Remove the last comma.
	if len(s) > 0 {
		s = s[:len(s)-2]
	}

	return s
}

// ToSlice converts Map to Slice of IQueue.
//
//nolint:prealloc
func ToSlice[P, S any](m map[string]IQueue[P, S]) []IQueue[P, S] {
	var s []IQueue[P, S]

	for _, q := range m {
		s = append(s, q)
	}

	return s
}

//////
// 1:N Operations.
//////

// PublishToMany publishes `msg` concurrently, against all Queues in `m`.
func PublishToMany[P, S any](
	ctx context.Context,
	m map[string]IQueue[P, S],
	queueName string,
	msg *Message,
	prm *P,
	options ...OptionsFunc[P, S],
) error {
	if _, errs := concurrentloop.Map(ctx, ToSlice(m), func(ctx context.Context, s IQueue[P, S]) (bool, error) {
		if err := Publish(ctx, s, queueName, msg, prm, options...); err != nil {
			return false, err
		}

		return true, nil
	}); len(errs) > 0 {
		return errs
	}

	return nil
}

//////
// N:1 Operations.
//////

// PublishMany publish `items` concurrently, against the specified Queue.
func PublishMany[P, S any](
	ctx context.Context,
	q IQueue[P, S],
	queueName string,
	items []*Message,
	prm *P,
	options ...OptionsFunc[P, S],
) error {
	if _, errs := concurrentloop.Map(ctx, items, func(ctx context.Context, msg *Message) (bool, error) {
		if err := Publish(ctx, q, queueName, msg, prm, options...); err != nil {
			return false, err
		}

		return true, nil
	}); len(errs) > 0 {
		return errs
	}

	return nil
}
