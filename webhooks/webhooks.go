package main

import "time"

func main() {
	runner := newRunner(time.Second)
	<-time.After(6000 * time.Second)

	runner.Stop()
}
