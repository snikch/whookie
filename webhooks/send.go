package main

type Send struct {
	Batch Batch
	Sub   Sub
}

type SendFinder interface {
	NextReadySend() (*Send, error)
}

func newSendFinder() RedisSendFinder {
	return RedisSendFinder{}
}

type RedisSendFinder struct {}


func (f RedisSendFinder) NextReadySend() (*Send, error){
	return nil, nil
}
