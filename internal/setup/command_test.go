package setup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFindUsesUserBinaryDirectoriesWithoutShellStartup(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", filepath.Join(home, "absent"))
	path := filepath.Join(home, ".local/bin/claude")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if got, err := Find(context.Background(), "claude"); err != nil || got != path {
		t.Fatal(got, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := Find(context.Background(), "claude"); err == nil {
		t.Fatal("non-executable accepted", got)
	}
}
