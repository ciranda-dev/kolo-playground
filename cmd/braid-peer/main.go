// Command braid-peer runs one kolo-braid member as its own process; cmd/braid-scenarios starts several of them.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ciranda-dev/kolo-playground/internal/braidpeer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(braidpeer.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
