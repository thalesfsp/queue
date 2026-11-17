package queue

import (
	"testing"
)

// This TEST exist just to ensure Mock match the IQueue interface.
func TestMock_match_interface(t *testing.T) {
	var iS IQueue = new(Mock)

	t.Log(iS)
}
