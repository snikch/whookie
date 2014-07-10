package main

type Sub struct {
}

type SubFinder interface {
	Subs(string) ([]Sub, error)
}

func newSubFinder() RedisSubFinder {
	return RedisSubFinder{}
}

type RedisSubFinder struct {
}

func (f RedisSubFinder) Subs(domainId string) ([]Sub, error) {
	subs := []Sub{}

	return subs, nil
}
