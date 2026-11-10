package setup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Run bounds native installer commands and retains their diagnostic output.
func Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = time.Second
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		return data, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return data, nil
}

func Find(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if path, err := exec.LookPath(name); err == nil {
		return path, nil
	}
	// Service callers must never run shell startup to discover executables.
	home, err := os.UserHomeDir()
	if err == nil {
		for _, dir := range []string{".local/bin", ".npm-global/bin"} {
			path := filepath.Join(home, dir, name)
			if info, e := os.Stat(path); e == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				return path, nil
			}
		}
	}
	return "", fmt.Errorf("%s is missing from PATH and standard user binary directories", name)
}
