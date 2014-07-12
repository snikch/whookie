package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{}
}

// Batch represents a group of events that occurred in a given timeperiod for a domain
type Batch struct {
	DomainId  string
	Timestamp string
	StartedAt time.Time
	Events    []Event
}

func (b Batch) BlankClone() Batch {
	return Batch{
		DomainId:  b.DomainId,
		Timestamp: b.Timestamp,
		Events:    []Event{},
	}
}

func (b Batch) Key() string {
	return b.DomainId + ":" + b.Timestamp
}

func (batch Batch) Filtered(sub Sub) (Batch, bool) {
	out := batch.BlankClone()

	// Create a hash lookup of valid events.
	eventMap := map[string]bool{}
	for _, e := range sub.Events {
		eventMap[e] = true
	}

	// Loops through the supplied batch and add valid events.
	for _, event := range batch.Events {
		_, match := eventMap[event.Type]
		if match {
			out.Events = append(out.Events, event)
		}
	}
	return out, len(out.Events) > 0
}

// BatchFinder is responsible for returning batches ready to process.
type BatchFinder interface {
	ReadyBatchKeys() ([]Batch, error)
	Batch(key string) (Batch, error)
}

func newBatchFinder() RedisBatchFinder {
	return RedisBatchFinder{expiry: time.Minute}
}

type RedisBatchFinder struct {
	expiry time.Duration
}

// ReadyBatches returns an batches over a minute old from Redis.
func (f RedisBatchFinder) ReadyBatchKeys() ([]Batch, error) {
	batches := []Batch{}
	members, err := Redis.SMembers("webhooks:batches:current").Result()
	if err != nil {
		return nil, err
	}

	for _, key := range members {
		batch, err := f.Batch(key)
		if err != nil {
			return batches, err
		}
		// Skip if the time of this batch plus 1 min, is now or later
		if time.Now().Before(batch.StartedAt.Add(f.expiry)) {
			continue
		}

		batches = append(batches, batch)
	}
	return batches, nil
}

func (f RedisBatchFinder) Batch(key string) (Batch, error) {
	batch := Batch{}
	parts := strings.Split(key, ":")
	if len(parts) != 2 {
		return batch, fmt.Errorf("Invalid batch key format: %s", key)
	}

	timestamp, err := strconv.ParseInt(parts[1], 0, 0)
	if err != nil {
		return batch, fmt.Errorf(
			"Could not convert timestamp '%s' to int: %s",
			parts[1],
			err.Error(),
		)
	}

	// Get events for this batch
	rawEvents, err := Redis.LRange(
		fmt.Sprintf("webhooks:batches:%s:%s", parts[0], parts[1]),
		0,
		-1,
	).Result()
	if err != nil {
		return batch, err
	}

	batch.Events = []Event{}
	for _, rawEvent := range rawEvents {
		event := Event{}
		err := json.Unmarshal([]byte(rawEvent), &event)
		if err != nil {
			logger.Errorf(
				"Could not unmarshal event data: %s\nData: %s",
				err.Error(),
				string(rawEvent),
			)
			continue
		}

		batch.Events = append(batch.Events, event)
	}

	if len(batch.Events) == 0 && len(rawEvents) > 0 {
		return batch, fmt.Errorf("Batch contains no events")
	}

	batch.StartedAt = time.Unix(timestamp, 0)
	batch.DomainId = parts[0]
	batch.Timestamp = parts[1]
	return batch, nil
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
		logger.Errorf("Couldn’t retrieve subscriptions: %s", err)
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
	// New batch with events filtered for this sub
	filtered, send := batch.Filtered(sub)
	if !send {
		logger.Infof("Not sending batch to %s, no valid events", sub.URL)
		return nil
	}

	logger.Infof("Sending batch with %d events to %s", len(batch.Events), sub.URL)

	body, err := json.Marshal(filtered)
	if err != nil {
		return err
	}

	// Create a new post request to the sub url, with the event payload.
	req, err := http.NewRequest("POST", sub.URL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	// Add headers
	for key, value := range sub.Headers {
		req.Header.Add(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Expected 2xx response, received %d", resp.StatusCode)
	}
	return nil
}
