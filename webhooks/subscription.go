package main

import "fmt"

type Sub struct {
	Events  []string
	Headers map[string]string
	URL     string
}

type SubFinder interface {
	Subs(string) ([]Sub, error)
	Sub(domainId, url string) (Sub, error)
}

func newSubFinder() RedisSubFinder {
	return RedisSubFinder{}
}

type RedisSubFinder struct {
}

func (f RedisSubFinder) Subs(domainId string) ([]Sub, error) {
	subs := []Sub{}

	subscriptionUrls, err := Redis.SMembers(
		fmt.Sprintf("webhooks:subscriptions:%s", domainId),
	).Result()

	if err != nil {
		return nil, err
	}

	for _, url := range subscriptionUrls {
		sub, err := f.Sub(domainId, url)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (f RedisSubFinder) Sub(domainId, url string) (Sub, error) {

	sub := Sub{
		URL: url,
	}

	headers, err := Redis.HGetAllMap(
		fmt.Sprintf("webhooks:subscriptions:%s:%s:headers", domainId, url),
	).Result()
	if err != nil {
		return sub, err
	}

	events, err := Redis.SMembers(
		fmt.Sprintf("webhooks:subscriptions:%s:%s:events", domainId, url),
	).Result()
	if err != nil {
		return sub, err
	}

	sub.Headers = headers
	sub.Events = events

	return sub, nil
}
