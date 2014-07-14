package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	runner := newRunner(time.Second)

	// Create a channel to watch for quit syscalls
	quitCh := make(chan os.Signal, 2)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGQUIT)

	// Wait on a quit signal
	sig := <-quitCh
	logger.Noticef("Signal received: %s", sig)
	logger.Noticef("Attempting graceful shutdown")

	// Start a goroutine that can force quit the app if second signal received
	go func() {
		sig := <-quitCh
		logger.Noticef("Second signal received: %s", sig)
		logger.Noticef("Forcing exit")
		os.Exit(1)
	}()

	runner.Stop()
}
