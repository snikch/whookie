package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

var (
	batchFinder    BatchFinder
	batchProcessor BatchProcessor
	batchSender BatchSender
	subFinder SubFinder

	Redis *redis.Client
)

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

type Runner struct {
	interval time.Duration
	stopChan chan bool
	running  bool
}

func newRunner(interval time.Duration) Runner {
	r := Runner{
		interval: interval,
		stopChan: make(chan bool),
		running:  true,
	}
	go r.run()
	return r
}

func (r Runner) Stop() error {
	if !r.running {
		return fmt.Errorf("Attempting to stop a stopped Runner")
	}
	r.stopChan <- true
	r.running = false
	return nil
}

func (r Runner) run() {
	timer := time.NewTimer(time.Nanosecond * 0)
RUN:
	for {
		select {
		case <-r.stopChan:
			break RUN
		case <-timer.C:
			batches, err := batchFinder.ReadyBatches()
			if err != nil {
				//TODO log error
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
