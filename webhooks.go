package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snikch/go-webhooks/webhooks"
)

func main() {
	runner := webhooks.NewRunner(time.Second)

	// Create a channel to watch for quit syscalls
	quitCh := make(chan os.Signal, 2)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGQUIT)

	// Wait on a quit signal
	sig := <-quitCh
	log.Printf("Signal received: %s", sig)
	log.Printf("Attempting graceful shutdown")
	log.Printf("Sending SIGINT or SIGQUIT again will force exit with possible data loss")

	// Start a goroutine that can force quit the app if second signal received
	go func() {
		sig := <-quitCh
		log.Printf("Second signal received: %s", sig)
		log.Printf("Forcing exit")
		os.Exit(1)
	}()

	runner.Stop()
}
