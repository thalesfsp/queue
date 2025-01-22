package queue

import (
	"context"

	"github.com/thalesfsp/concurrentloop"
)

//////
// Vars, consts, and types.
//////

// Map is a map of strgs
type Map map[string]IQueue

//////
// Methods.
//////

// String implements the Stringer interface.
func (m Map) String() string {
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
func (m Map) ToSlice() []IQueue {
	var s []IQueue

	for _, q := range m {
		s = append(s, q)
	}

	return s
}

//////
// 1:N Operations.
//////

// PublishToMany publishes `msg` concurrently, against all Queues in `m`.
func PublishToMany(
	ctx context.Context,
	m Map,
	queueName string,
	msg *Message,
	prm *PublishParams,
	options ...OptionsFunc,
) error {
	if _, errs := concurrentloop.Map(ctx, m.ToSlice(), func(ctx context.Context, s IQueue) (bool, error) {
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
func PublishMany(
	ctx context.Context,
	q IQueue,
	queueName string,
	items []*Message,
	prm *PublishParams,
	options ...OptionsFunc,
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
