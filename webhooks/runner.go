package webhooks

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
	readyFinder    ReadyFinder
	subFinder      SubFinder

	Redis *redis.Client

	retryTx *redis.Multi
)

// init sets up the default implementations of various interfaces.
func init() {
	batchFinder = newBatchFinder()
	batchProcessor = newBatchProcessor()
	batchSender = newBatchSender()
	readyFinder = newReadyFinder()
	subFinder = newSubFinder()

	var err error
	Redis, err = newRedisClient()
	if err != nil {
		panic(err)
	}
	retryTx = Redis.Multi()
}

// runner is responsible for processing ready batches at the given interval.
type runner struct {
	interval       time.Duration
	currentStopper chan bool
	readyStopper   chan bool
	resendStopper  chan bool
	running        bool
	sendWaitGroup  *sync.WaitGroup
}

// Newrunner creates a runner with the supplied interval between polls.
func NewRunner(interval time.Duration) runner {
	r := runner{
		interval:       interval,
		currentStopper: make(chan bool),
		readyStopper:   make(chan bool),
		resendStopper:  make(chan bool),
		running:        true,
		sendWaitGroup:  &sync.WaitGroup{},
	}
	go r.processCurrent()
	go r.processReady()
	go r.processRetry()
	return r
}

// Stop will stop the runner after the current batches finish processing.
func (r runner) Stop() error {
	if !r.running {
		return fmt.Errorf("Attempting to stop a stopped runner")
	}

	logger.Info("Stopping runner")
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		r.currentStopper <- true
		logger.Info("Stopped processing current batches")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		r.readyStopper <- true
		logger.Info("Stopped processing ready batches")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		r.resendStopper <- true
		logger.Info("Stopped processing retry batches")
		wg.Done()
	}()

	wg.Wait()
	logger.Info("Waiting for any sending batches to complete")
	r.sendWaitGroup.Wait()
	logger.Info("No more sending batches")
	r.running = false
	return nil
}

// run is a blocking runner that processes ready batches at the given interval.
// Sending on the stopChan will let the current batch finish, then complete.
func (r runner) processCurrent() {
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

			if batchesLen > 0 {
				logger.Infof("Found %d new batches", batchesLen)
			}

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
func (r runner) processReady() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.readyStopper:
			break RUN
		case <-timer.C:

			batch, sub, err := readyFinder.NextReady()

			if err != nil {
				logger.Error("Error getting next ready to send", err.Error())
				timer.Reset(r.interval)
				continue
			}

			// No batch? Wait and try again.
			if batch == nil || sub == nil {
				timer.Reset(r.interval)
				continue
			}

			go func() {
				r.sendWaitGroup.Add(1)
				batchSender.Send(*batch, *sub)
				r.sendWaitGroup.Done()
			}()
			timer.Reset(0 * time.Nanosecond)
		}
	}
}
func (r runner) processRetry() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.resendStopper:
			break RUN
		case <-timer.C:

			count, err := readyFinder.ProcessReadyRetries()

			if err != nil {
				logger.Errorf("Error processing ready retries: %s", err.Error())
				timer.Reset(r.interval)
				continue
			}

			if count > 0 {
				logger.Infof("Moved %d retries to ready queue", count)
			}

			timer.Reset(r.interval)
		}
	}
}
