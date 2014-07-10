package main

import "time"

func main() {
	runner := newRunner(time.Second)
	<-time.After(10 * time.Second)

	runner.Stop()
}
