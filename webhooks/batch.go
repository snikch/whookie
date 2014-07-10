package main

import (
	"strconv"
	"strings"
	"time"
)

// Batch represents a group of events that occurred in a given timeperiod for a domain
type Batch struct {
	DomainId  string
	Timestamp string
}

func (b Batch) Key() string {
	return b.DomainId + ":" + b.Timestamp
}

// BatchFinder is responsible for returning batches ready to process.
type BatchFinder interface {
	ReadyBatches() ([]Batch, error)
}

func newBatchFinder() RedisBatchFinder {
	return RedisBatchFinder{expiry: time.Minute}
}

type RedisBatchFinder struct {
	expiry time.Duration
}

// ReadyBatches returns an batches over a minute old from Redis.
func (f RedisBatchFinder) ReadyBatches() ([]Batch, error) {
	batches := []Batch{}
	members, err := Redis.SMembers("webhooks:batches:current").Result()
	if err != nil {
		return nil, err
	}

	for _, member := range members {
		parts := strings.Split(member, ":")
		if len(parts) != 2 {
			logger.Errorf("Invalid batch key format: %s", member)
			continue
		}

		timestamp, err := strconv.ParseInt(parts[1], 0, 0)
		if err != nil {
			logger.Error("Could not convert timestamp to int:", err.Error())
			continue
		}

		timeFloored := time.Unix(timestamp, 0)

		// Skip if the time of this batch plus 1 min, is now or later
		if time.Now().Before(timeFloored.Add(f.expiry)) {
			continue
		}

		batch := Batch{
			DomainId:  parts[0],
			Timestamp: parts[1],
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// BatchProcessor is responsible for the processing of batches to their subs.
type BatchProcessor interface {
	ProcessBatch(Batch, chan<- bool)
}

func newBatchProcessor() RedisBatchProcessor {
	return RedisBatchProcessor{}
}

type RedisBatchProcessor struct {
}

// ProcessBatch gets any subs from redis, and sends the batch to the sub.
func (p RedisBatchProcessor) ProcessBatch(batch Batch, out chan<- bool) {

	subs, err := subFinder.Subs(batch.DomainId)
	if err != nil {
		logger.Error(err)
		out <- true
		return
	}
	subsLen := len(subs)
	if subsLen == 0 {
		logger.Info("Found no subscriptions for ", batch.Key())
	} else {
		logger.Infof("Found %d subscriptions for %s", subsLen, batch.Key())
	}

	completeChan := make(chan error, subsLen)
	for _, sub := range subs {
		go func(ch chan<- error) {
			err := batchSender.Send(batch, sub)
			if err != nil {
				logger.Error(err)
				ch <- err
				return
			}

			ch <- nil
		}(completeChan)
	}

	didHaveError := false
	for i := 0; i < subsLen; i++ {
		err := <-completeChan
		if err != nil {
			didHaveError = true
		}
	}
	if !didHaveError {
		_, err = Redis.SRem("webhooks:batches:current", batch.Key()).Result()
		if err != nil {
			logger.Error(err)
		}
	} else {
		logger.Error("Batch Process had error, leaving batch for retry")
	}
	out <- true
}

// BatchSender is responsible for sending a batch to a sub.
type BatchSender interface {
	Send(Batch, Sub) error
}

func newBatchSender() HttpBatchSender {
	return HttpBatchSender{}
}

type HttpBatchSender struct{}

// Send will submit the batch to the given sub according to the sub configuration.
func (s HttpBatchSender) Send(batch Batch, sub Sub) error {
	return nil
}
