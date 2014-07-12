package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

var (
	batchFinder    BatchFinder
	batchProcessor BatchProcessor
	batchSender    BatchSender
	subFinder      SubFinder

	Redis *redis.Client
)

// init sets up the default implementations of various interfaces.
func init() {
	batchFinder = newBatchFinder()
	batchProcessor = newBatchProcessor()
	batchSender = newBatchSender()
	subFinder = newSubFinder()

	var err error
	Redis, err = newRedisClient()
	if err != nil {
		panic(err)
	}
}

// Runner is responsible for processing ready batches at the given interval.
type Runner struct {
	interval time.Duration
	stopChan chan bool
	running  bool
}

// newRunner creates a runner with the supplied interval between polls.
func newRunner(interval time.Duration) Runner {
	r := Runner{
		interval: interval,
		stopChan: make(chan bool),
		running:  true,
	}
	go r.run()
	return r
}

// Stop will stop the runner after the current batches finish processing.
func (r Runner) Stop() error {
	if !r.running {
		return fmt.Errorf("Attempting to stop a stopped Runner")
	}
	r.stopChan <- true
	r.running = false
	return nil
}

// run is a blocking runner that processes ready batches at the given interval.
// Sending on the stopChan will let the current batch finish, then complete.
func (r Runner) run() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.stopChan:
			break RUN
		case <-timer.C:
			batches, err := batchFinder.ReadyBatchKeys()
			if err != nil {
				logger.Error("Failed to retrieve batches:", err.Error())
				continue
			}

			batchesLen := len(batches)

			logger.Infof("Found %d batches", batchesLen)

			completeChan := make(chan bool, batchesLen)

			for _, batch := range batches {
				go batchProcessor.ProcessBatch(batch, completeChan)
			}

			for i := 0; i < batchesLen; i++ {
				<-completeChan
			}
			timer.Reset(r.interval)
		}
	}

}
