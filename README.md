# Whookie

Whookie is a server that sends batched events to subscribed urls, aka Webhooks. If you want to send webhooks from your app, then Whookie can help you out.

This library is designed to be used in an existing Go application. See [Whookie Server](https://github.com/snikch/whookie-server) for a standalone binary.


## Usage

Events are pushed to Redis, and batched by the minute. A redis connection string is expected in the environment variable `REDIS_URL`.

```go
// Supply a poll delay
runner := whookie.NewRunner(time.Second)

…

runner.Stop()
```