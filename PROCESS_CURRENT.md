# Queue Ready Batches
1. SMEMBERS webhooks:batches:current
1. LOOP BATCHES:
1. SMEMBERS webhooks:subscriptions:{batch.domainID}
1. LRANGE webhooks:batches:{batch.DomainId}:{batch.Timestamp} 0, -1
1. LOOP SUBSCRIPTIONS:
1. HGETALL webhooks:subscriptions:{domainId}:{url}:headers
1. HGETALL webhooks:subscriptions:{domainId}:{url}:events
1. HSET webhooks:sends:{batch.DomainId}:payload {batch.Timestamp}:{sub.URL} {filtered}
1. HSET webhooks:sends:{batch.DomainId}:count {batch.Timestamp}:{sub.URL} {num events}
1. HSET webhooks:sends:{batch.DomainId}:status {batch.Timestamp}:{sub.URL} ready
1. SADD webhooks:sends:ready {batch.DomainId}:{batch.Timestamp}:{sub.URL}
1. END SUBSCRIPTIONS
1. DEL webhooks:batches:{batch.Key}
1. SREM webhooks:subscriptions:{batch.DomainId}
1. END BATCHES

1. CATCH ERROR
1. PIPELINE
1. SREM webhooks:sends:processing {key}
1. SADD webhooks:sends:ready {key}
1. END

# Process Ready Batch
1. TRY
1. MULTI
1. SPOP webhooks:sends:ready
1. SADD webhooks:sends:processing {poppedKey}
1. EXEC
1. HGETALL webhooks:subscriptions:{domainId}:{url}:headers
1. HGETALL webhooks:subscriptions:{domainId}:{url}:events
1. INCR webhooks:sends:{batch.DomainId}:{batch.Timestamp}:{sub.URL}:attempts {num attempts}
1. send webhook
1. REMOVE FROM LIST?!?!?
1. HSET webhooks:sends:{batch.DomainId}:status {batch.Timestamp}:{sub.URL} sent

1. CATCH RETRY
1. PIPELINE
1. SREM webhooks:sends:processing {key}
1. ZADD webhooks:sends:retry {key} {retry timestamp}
1. HSET webhooks:sends:{batch.DomainId}:status {batch.Timestamp}:{sub.URL} resending
1. END

1. CATCH ERROR
1. PIPELINE
1. SREM webhooks:sends:processing {key}
1. SADD webhooks:sends:ready {key}
1. END
