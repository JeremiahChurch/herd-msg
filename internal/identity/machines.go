package identity

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
)

type Machine struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	Target  string `json:"target"`
	Enabled bool   `json:"enabled"`
}

func Machines(ctx context.Context) ([]Machine, error) {
	data, e := exec.CommandContext(ctx, "herdr", "machine", "list", "--json").Output()
	if e != nil {
		return nil, errors.New("cannot read saved herdr machines; run herdr machine list --json")
	}
	var machines []Machine
	if e = json.Unmarshal(data, &machines); e != nil {
		return nil, errors.New("invalid herdr machine list")
	}
	return machines, nil
}
func FindMachine(machines []Machine, name string) (Machine, error) {
	var found []Machine
	for _, m := range machines {
		if m.Enabled && (m.Label == name || m.ID == name || m.Target == name) {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		return Machine{}, errors.New("saved machine is missing, disabled or ambiguous; run herdr machine list --json")
	}
	return found[0], nil
}
