package main

import (
	"encoding/json"
	"fmt"
	"math"
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
		go func(sub Sub, ch chan<- error) {
			ok, err := batchSender.Send(batch, sub)

			// Log error immediately
			if err != nil {
				logger.Error(err)
			}

			// Send to the failure manager if it failed to send.
			// Errors saving to the failure manager take priority over
			// any error from the previous operation, hence the order here.
			if !ok {
				err := batchSender.Fail(batch, sub)
				if err != nil {
					logger.Error(err)
					ch <- err
					return
				}
			}

			// Pass any error back via the channel
			if err != nil {
				ch <- err
				return
			}

			ch <- nil
		}(sub, completeChan)
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
	 // TODO(mc): Clear up batch
}

// BatchSender is responsible for sending a batch to a sub.
type BatchSender interface {
	Send(Batch, Sub) (bool, error)
	Fail(Batch, Sub) error
}

func newBatchSender() HttpBatchSender {
	return HttpBatchSender{}
}

type HttpBatchSender struct{}

// Send will submit the batch to the given sub according to the sub configuration.
// Anything which is considered an error at the client end shall return false, nil
// Anything which is an error at the server end shall return true, err
func (s HttpBatchSender) Send(batch Batch, sub Sub) (bool, error) {
	// New batch with events filtered for this sub
	filtered, send := batch.Filtered(sub)
	if !send {
		logger.Infof("Not sending batch to %s, no valid events", sub.URL)
		return true, nil
	}

	logger.Infof("Sending batch with %d events to %s", len(batch.Events), sub.URL)

	key := fmt.Sprintf("%s:%s", batch.Timestamp, sub.URL)
	rootKey := fmt.Sprintf("webhooks:batches:%s", batch.DomainId)

	// Set the state to sending
	_, err := Redis.HSet(
		fmt.Sprintf("%s:status", rootKey),
		key,
		"sending",
	).Result()
	if err != nil {
		return true, err
	}

	body, err := json.Marshal(filtered.Events)
	if err != nil {
		return true, err
	}

	// Create a new post request to the sub url, with the event payload.
	req, err := http.NewRequest("POST", sub.URL, strings.NewReader(string(body)))
	if err != nil {
		return true, err
	}

	// Add headers
	for key, value := range sub.Headers {
		req.Header.Add(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return true, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Noticef("Expected 2xx response, received %d", resp.StatusCode)
		return false, nil
	}

	// Set the state to sent
	_, err = Redis.HSet(
		fmt.Sprintf("%s:status", rootKey),
		key,
		"sent",
	).Result()
	if err != nil {
		return true, err
	}

	logger.Infof("Successfully sent %s to %s", batch.Key(), sub.URL)

	return true, nil
}

// Fail is responsible for placing the batch sub combo into failed state
// and setting retry count, time etc.
func (s HttpBatchSender) Fail(batch Batch, sub Sub) error {
	logger.Noticef("Failed to send %s to %s ", batch.Key(), sub.URL)
	tx := Redis.Multi()
	defer tx.Close()

	key := fmt.Sprintf("%s:%s", batch.Timestamp, sub.URL)
	rootKey := fmt.Sprintf("webhooks:batches:%s", batch.DomainId)

	// Set the state to failed
	_, err := tx.HSet(
		fmt.Sprintf("%s:status", rootKey),
		key,
		"resend",
	).Result()
	if err != nil {
		return err
	}

	// Update the retry count
	count, err := tx.Incr(
		fmt.Sprintf("%s:%s:retries", rootKey, sub.URL),
	).Result()
	if err != nil {
		return err
	}

	// Set the next send time
	resendTime := time.Now().Add(
		backoffDuration(int(count)),
	)
	resendTimeStr := fmt.Sprintf("%d", resendTime.Unix())
	_, err = tx.HSet(fmt.Sprintf("%s:retries", rootKey), key, resendTimeStr).Result()
	if err != nil {
		return err
	}

	_, err = tx.Exec(func() error { return nil })

	return err
}

func backoffDuration(retryCount int) time.Duration {
	seconds := math.Pow(float64(retryCount), 4) + 15
	return time.Second * time.Duration(seconds)
}
