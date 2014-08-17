package webhooks

import "fmt"

type Sub struct {
	DomainId string
	Events   []string
	Headers  map[string]string
	URL      string
	Id       string
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

	subscriptionIds, err := Redis.SMembers(
		fmt.Sprintf("webhooks:subscriptions:%s", domainId),
	).Result()

	if err != nil {
		return nil, err
	}

	for _, id := range subscriptionIds {
		sub, err := f.Sub(domainId, id)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (f RedisSubFinder) Sub(domainId, id string) (Sub, error) {

	sub := Sub{
		Id:       id,
		DomainId: domainId,
	}

	url, err := Redis.Get(
		fmt.Sprintf("webhooks:subscriptions:%s:%s:url", domainId, id),
	).Result()
	if err != nil {
		return sub, err
	}

	headers, err := Redis.HGetAllMap(
		fmt.Sprintf("webhooks:subscriptions:%s:%s:headers", domainId, id),
	).Result()
	if err != nil {
		return sub, err
	}

	events, err := Redis.SMembers(
		fmt.Sprintf("webhooks:subscriptions:%s:%s:events", domainId, id),
	).Result()
	if err != nil {
		return sub, err
	}

	sub.URL = url
	sub.Headers = headers
	sub.Events = events

	return sub, nil
}
