package webhooks

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type ReadyFinder interface {
	NextReady() (*Batch, *Sub, error)
	ProcessReadyRetries() (int, error)
}

func newReadyFinder() RedisReadyFinder {
	return RedisReadyFinder{}
}

type RedisReadyFinder struct{}

func (f RedisReadyFinder) NextReady() (*Batch, *Sub, error) {

	key, err := Redis.SPop("webhooks:sends:ready").Result()

	// No Batch.
	if err == redis.Nil {
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, err
	}

	return f.BatchSubFromKey(key)
}

func (f RedisReadyFinder) ProcessReadyRetries() (int, error) {

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	opts := redis.ZRangeByScore{
		Min:   "-inf",
		Max:   timestamp,
		Count: 1,
	}
	keys, err := Redis.ZRangeByScore("webhooks:sends:retry", opts).Result()
	if err != nil {
		return 0, err
	}

	moved := 0
	_, err = retryTx.Exec(func() error {

		for _, key := range keys {
			_, err = retryTx.ZRem("webhooks:sends:retry", key).Result()
			if err != nil {
				return err
			}
			_, err = retryTx.SAdd("webhooks:sends:ready", key).Result()
			if err != nil {
				return err
			}

			moved++
		}
		return nil
	})
	return moved, err
}

func (f RedisReadyFinder) BatchSubFromKey(key string) (*Batch, *Sub, error) {
	parts := strings.Split(key, ":")
	if len(parts) < 3 {
		return nil, nil, fmt.Errorf("Expected key %s to contain three parts", key)
	}

	domainId := parts[0]
	timestamp := parts[1]
	url := strings.Join(parts[2:], ":")

	sub, err := subFinder.Sub(domainId, url)
	if err != nil {
		return nil, nil, err
	}

	rootKey := "webhooks:sends:" + domainId
	sendKey := timestamp + ":" + url

	marshalled, err := Redis.HGet(rootKey+":batch", sendKey).Result()
	if err != nil {
		_, err := Redis.SAdd("webhooks:sends:ready", key).Result()
		// PANIC. This is a failure in a failure and we're out of sync
		// I believe there is a high chance of this actually occurring
		// as connection failures would be likely to encounter this
		// due to the fact that the failure above would be reproduced.
		if err != nil {
			logger.Errorf("Failed to rollback. Key %s is orphaned", key)
		}
		return nil, nil, err
	}

	// No Batch.
	if marshalled == "" {
		return nil, nil, fmt.Errorf("Couldn’t find batch for key %s", key)
	}

	batch := Batch{}
	err = json.Unmarshal([]byte(marshalled), &batch)
	if err != nil {
		return nil, nil, err
	}

	return &batch, &sub, nil
}
