package states

import "testing"

func TestEveryTransition(t *testing.T) {
	allowed := map[State]string{Outbox: "sent dead dropped expired", Held: "queued dead dropped expired", Queued: "claimed held dead dropped injected expired", Claimed: "injecting queued dead dropped injected expired", Injecting: "injected indeterminate queued dead", Indeterminate: "injected queued dropped expired", Dead: "outbox sent queued injected dropped expired"}
	for _, from := range All {
		for _, to := range All {
			want := false
			for _, s := range split(allowed[from]) {
				if string(to) == s {
					want = true
				}
			}
			if got := Transition(from, to) == nil; got != want {
				t.Errorf("%s to %s allowed=%v want %v", from, to, got, want)
			}
		}
	}
	if Transition("unknown", Queued) == nil {
		t.Fatal("unknown state accepted")
	}
}
func split(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ' ' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
