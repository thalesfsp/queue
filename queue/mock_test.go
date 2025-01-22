package queue

import (
	"testing"
)

// This TEST exist just to ensure Mock match the IQueue interface.
func TestMock_match_interface(t *testing.T) {
	var iS IQueue = &Mock{}

	t.Log(iS)
}
