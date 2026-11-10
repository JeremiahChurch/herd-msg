package main

import (
	"context"
	"os"
	"syscall"

	"github.com/JeremiahChurch/herd-msg/internal/cli"
)

var version = "dev"

func main() {
	syscall.Umask(077)
	os.Exit(cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, version))
}
