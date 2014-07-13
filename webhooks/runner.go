package main

import (
	"fmt"
	"sync"
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
	interval       time.Duration
	currentStopper chan bool
	readyStopper   chan bool
	resendStopper  chan bool
	running        bool
}

// newRunner creates a runner with the supplied interval between polls.
func newRunner(interval time.Duration) Runner {
	r := Runner{
		interval:       interval,
		currentStopper: make(chan bool),
		readyStopper:   make(chan bool),
		resendStopper:  make(chan bool),
		running:        true,
	}
	go r.processCurrent()
	go r.processReady()
	return r
}

// Stop will stop the runner after the current batches finish processing.
func (r Runner) Stop() error {
	if !r.running {
		return fmt.Errorf("Attempting to stop a stopped Runner")
	}

	logger.Info("Stopping runner")
	wg := sync.WaitGroup{}

	go func() {
		wg.Add(1)
		r.currentStopper <- true
		logger.Info("Stopped processing current batches")
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		r.readyStopper <- true
		logger.Info("Stopped processing ready batches")
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		r.resendStopper <- true
		logger.Info("Stopped processing retry batches")
		wg.Done()
	}()

	wg.Wait()
	r.running = false
	return nil
}

// run is a blocking runner that processes ready batches at the given interval.
// Sending on the stopChan will let the current batch finish, then complete.
func (r Runner) processCurrent() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.currentStopper:
			break RUN
		case <-timer.C:
			batches, err := batchFinder.ReadyBatchKeys()
			if err != nil {
				logger.Error("Failed to retrieve batches:", err.Error())
				continue
			}

			batchesLen := len(batches)

			logger.Infof("Found %d batches", batchesLen)

			for _, batch := range batches {
				err := batchProcessor.ProcessBatch(batch)
				if err != nil {
					logger.Error(err)
				}
			}

			timer.Reset(r.interval)
		}
	}

}
func (r Runner) processReady() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.currentStopper:
			break RUN
		case <-timer.C:

			timer.Reset(r.interval)
		}
	}
}
func (r Runner) processResend() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.resendStopper:
			break RUN
		case <-timer.C:

			timer.Reset(r.interval)
		}
	}
}
