package whookie

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{}
}

// Batch represents a group of events that occurred in a given timeperiod for a domain
type Batch struct {
	DomainId  string    `json:"domain_id"`
	Timestamp string    `json:"-"`
	StartedAt time.Time `json:"-"`
	Events    []Event   `json:"events"`
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
		if e == "all" {
			return batch, len(batch.Events) > 0
		}
		eventMap[e] = true
	}

	// Loops through the supplied batch and add valid events.
	for _, event := range batch.Events {
		_, match := eventMap[event.Type]
		if match || event.Type == "test" {
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
	ProcessBatch(Batch) error
}

func newBatchProcessor() RedisBatchProcessor {
	return RedisBatchProcessor{}
}

type RedisBatchProcessor struct {
}

// ProcessBatch gets any subs from redis, and sends the batch to the sub.
func (p RedisBatchProcessor) ProcessBatch(batch Batch) error {

	subs, err := subFinder.Subs(batch.DomainId)
	if err != nil {
		return err
	}

	subsLen := len(subs)
	if subsLen == 0 {
		logger.Info("Found no subscriptions for ", batch.Key())
	} else {
		logger.Infof("Found %d subscriptions for %s", subsLen, batch.Key())
	}

	tx := Redis.Multi()
	defer tx.Close()

	for _, sub := range subs {
		err := p.PersistBatch(tx, batch, sub)
		if err != nil {
			logger.Errorf("Error processing batch: %s", err.Error())
			err = tx.Discard()
			return err
		}
	}

	_, err = tx.Del(fmt.Sprintf("webhooks:batches:%s", batch.Key())).Result()
	if err != nil {
		logger.Errorf("Error removing batch events: %s", err.Error())
		err = tx.Discard()
		return err
	}

	_, err = tx.SRem("webhooks:batches:current", batch.Key()).Result()
	if err != nil {
		logger.Errorf("Error removing batch from current: %s", err.Error())
		err = tx.Discard()
		return err
	}

	_, err = tx.Exec(func() error { return nil })
	if err != nil {
		return err
	}
	return nil
}

func (p RedisBatchProcessor) PersistBatch(tx *redis.Multi, batch Batch, sub Sub) error {
	// New batch with events filtered for this sub
	filtered, send := batch.Filtered(sub)
	if !send {
		logger.Infof(
			"Not sending batch to %s, no of the %d events are valid",
			sub.URL,
			len(batch.Events),
		)
		return nil
	}

	payload, err := json.Marshal(filtered)
	if err != nil {
		return err
	}

	rootKey := fmt.Sprintf("webhooks:sends:%s", batch.DomainId)
	sendKey := batch.Timestamp + ":" + sub.Id

	_, err = tx.HSet(rootKey+":batch", sendKey, string(payload)).Result()
	if err != nil {
		return err
	}

	lenStr := fmt.Sprintf("%d", len(filtered.Events))
	_, err = tx.HSet(rootKey+":count", sendKey, lenStr).Result()
	if err != nil {
		return err
	}

	_, err = tx.SAdd("webhooks:sends:ready", batch.Key()+":"+sub.Id).Result()
	if err != nil {
		return err
	}
	logger.Infof("Added send with %d events", len(filtered.Events))

	return nil
}

// BatchSender is responsible for sending a batch to a sub.
type BatchSender interface {
	Send(Batch, Sub)
	Retry(Batch, Sub)
	Fail(Batch, Sub, error)
}

func newBatchSender() HttpBatchSender {
	return HttpBatchSender{}
}

type HttpBatchSender struct{}

// Send will submit the batch to the given sub according to the sub configuration.
// Anything which is considered an error at the client end shall return false, nil
// Anything which is an error at the server end shall return true, err
func (s HttpBatchSender) Send(batch Batch, sub Sub) {
	logger.Infof("Sending batch with %d events to %s", len(batch.Events), sub.URL)

	key := fmt.Sprintf("%s:%s", batch.Timestamp, sub.Id)
	rootKey := fmt.Sprintf("webhooks:batches:%s", batch.DomainId)

	// Set the state to sending
	_, err := Redis.HSet(
		fmt.Sprintf("%s:status", rootKey),
		key,
		"sending",
	).Result()
	if err != nil {
		s.Fail(batch, sub, err)
		return
	}

	body, err := json.Marshal(batch)
	if err != nil {
		s.Fail(batch, sub, err)
		return
	}

	// Create a new post request to the sub url, with the event payload.
	req, err := http.NewRequest("POST", sub.URL, strings.NewReader(string(body)))
	if err != nil {
		s.Fail(batch, sub, err)
		return
	}

	// Add headers
	for key, value := range sub.Headers {
		req.Header.Add(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		s.Fail(batch, sub, err)
		return
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Noticef("Expected 2xx response, received %d", resp.StatusCode)
		s.Retry(batch, sub)
		return
	}

	// Set the state to sent
	_, err = Redis.HSet(
		fmt.Sprintf("%s:status", rootKey),
		key,
		"sent",
	).Result()
	if err != nil {
		s.Fail(batch, sub, err)
		return
	}

	logger.Infof("Successfully sent %s to %s", batch.Key(), sub.URL)
}

func (s HttpBatchSender) Fail(batch Batch, sub Sub, err error) {
	logger.Errorf("Failed to correctly %s:%s: %s", batch.Key(), sub.URL, err.Error())
	logger.Error("Message in incorrect state")
	// TODO(revert to previous state)
}

// Fail is responsible for placing the batch sub combo into failed state
// and setting retry count, time etc.
func (s HttpBatchSender) Retry(batch Batch, sub Sub) {
	logger.Noticef("Failed to send %s to %s ", batch.Key(), sub.URL)

	key := fmt.Sprintf("%s:%s", batch.Timestamp, sub.URL)
	rootKey := fmt.Sprintf("webhooks:batches:%s", batch.DomainId)

	// Set the state to failed
	_, err := Redis.HSet(
		fmt.Sprintf("%s:status", rootKey),
		key,
		"resend",
	).Result()
	if err != nil {
		logger.Error(err.Error())
		return
	}

	// Update the retry count
	count, err := Redis.Incr(
		fmt.Sprintf("%s:%s:retries", rootKey, sub.Id),
	).Result()
	if err != nil {
		logger.Error(err.Error())
		return
	}

	duration := backoffDuration(int(count))
	logger.Noticef("Retrying in %s", duration)

	// Set the next send time
	resendTime := time.Now().Add(duration)
	z := redis.Z{float64(resendTime.Unix()), batch.DomainId + ":" + key}
	_, err = Redis.ZAdd("webhooks:sends:retry", z).Result()
	if err != nil {
		logger.Error(err.Error())
		return
	}

}

func backoffDuration(retryCount int) time.Duration {
	seconds := math.Pow(float64(retryCount), 4) + 15
	return time.Second * time.Duration(seconds)
}
