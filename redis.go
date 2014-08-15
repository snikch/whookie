package webhooks

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/go-redis/redis"
)

/**
 * Creates a pooled redis client
 */
func newRedisClient() (*redis.Client, error) {

	var url string
	var password string

	// Get the environment value for REDIS_URL
	url = os.Getenv("REDIS_URL")
	if len(url) == 0 {
		url = "redis://0.0.0.0:6379"
	}

	// Check it matches a redis url format
	match, _ := regexp.MatchString("^redis://(.*:.*@)?[^@]*:[0-9]+$", url)
	if !match {
		return nil, errors.New(fmt.Sprintf("Invalid REDIS_URL format '%s'", url))
	}

	// Remove the scheme
	url = strings.Replace(url, "redis://", "", 1)
	parts := strings.Split(url, "@")

	// Smash off the credentials
	if len(parts) > 1 {
		url = parts[1]
		password = strings.Split(parts[0], ":")[1]
	}

	fmt.Println(fmt.Sprintf("Redis will connect to %s", url))
	client := redis.NewTCPClient(&redis.Options{
		Addr:     url,
		Password: password, // no password set
		DB:       0,        // use default DB
	})
	return client, nil
}
