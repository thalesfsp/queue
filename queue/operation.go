package queue

//////
// Vars, consts, and types.
//////

// Operation is the operation name.
type Operation string

const (
	OperationPublished  Operation = "published"
	OperationSubscribed Operation = "subscribed"
	OperationReceived   Operation = "Received"
)

//////
// Methods.
//////

// String implements the Stringer interface.
func (o Operation) String() string {
	return string(o)
}
