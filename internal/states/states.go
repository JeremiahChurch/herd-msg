// Package states defines the durable mailbox lifecycle.
package states

import "fmt"

type State string

const (
	Outbox        State = "outbox"
	Held          State = "held"
	Queued        State = "queued"
	Claimed       State = "claimed"
	Injecting     State = "injecting"
	Indeterminate State = "indeterminate"
	Dead          State = "dead"
	Sent          State = "sent"
	Injected      State = "injected"
	Dropped       State = "dropped"
	Expired       State = "expired"
)

var All = []State{Outbox, Held, Queued, Claimed, Injecting, Indeterminate, Dead, Sent, Injected, Dropped, Expired}
var edges = map[State][]State{
	Outbox:        {Sent, Dead, Dropped, Expired},
	Held:          {Queued, Dead, Dropped, Expired},
	Queued:        {Claimed, Held, Dead, Dropped, Injected, Expired},
	Claimed:       {Injecting, Queued, Dead, Dropped, Injected, Expired},
	Injecting:     {Injected, Indeterminate, Queued, Dead},
	Indeterminate: {Injected, Queued, Dropped, Expired},
	Dead:          {Outbox, Sent, Queued, Injected, Dropped, Expired},
}

func Terminal(s State) bool { return s == Sent || s == Injected || s == Dropped || s == Expired }
func Unsent(s State) bool   { return s == Held || s == Queued || s == Claimed || s == Dead }
func Valid(s State) bool {
	for _, v := range All {
		if s == v {
			return true
		}
	}
	return false
}
func Transition(from, to State) error {
	for _, s := range edges[from] {
		if s == to {
			return nil
		}
	}
	return fmt.Errorf("illegal message transition %s to %s", from, to)
}
