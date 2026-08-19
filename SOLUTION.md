# SOLUTION.md

## What was broken and what I changed

### Issue 1 — Webhook idempotency

The webhook provider can send the same event more than once, so the service needs to make sure the same `event_id` is processed only once.

The original implementation first checked whether the event existed and then inserted it. This looks fine at first, but there is a race condition:

1. Request A checks the event → doesn't exist.
2. Request B checks the event → doesn't exist.
3. Both requests insert the event.
4. Both requests update the account statistics.

That could make the statistics incorrect when duplicate webhooks arrived at the same time.

I fixed this by adding a `UNIQUE` constraint on `events.event_id` and letting PostgreSQL handle the deduplication with:

```sql
INSERT ... ON CONFLICT (event_id) DO NOTHING
```

The event insert, call update, and account statistics update are also handled in one PostgreSQL transaction. This means either the whole operation succeeds or none of it is applied.

I chose PostgreSQL for this because it is already the source of truth for the webhook data, so there is no need to introduce another system just for deduplication.

---

### Issue 2 — Recording processing

Recording processing originally happened in a background goroutine using the HTTP request's context.

The problem is that the request context can be cancelled as soon as the HTTP response is sent. The recording worker could then finish its work but fail when trying to update PostgreSQL because its context was already cancelled.

There was another problem: the error was effectively ignored, so there was no useful information in the logs when processing failed.

The goroutine was also not tracked. During a graceful shutdown, the HTTP server could stop while recording work was still running.

I fixed this by:

* Using `context.Background()` for the background recording operation.
* Tracking recording workers with `sync.WaitGroup`.
* Logging recording-processing failures with the event, call, and account IDs.
* Adding `Service.Shutdown()` to wait for active recording workers.
* Calling `svc.Shutdown()` after the HTTP server has stopped accepting requests.

This makes graceful shutdown much safer because active recording work gets a chance to finish.

This still does not protect against things like `SIGKILL`, an OOM kill, or a machine crash. For that level of reliability, recording jobs would need to be stored in a durable queue.

---

## Why I used PostgreSQL for deduplication

I considered three options:

| Approach                                      | Decision                                                                                                 |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Check with `EventExists()` and then insert    | **Rejected** because the check and insert can race.                                                      |
| Use Redis `SET NX`                            | **Rejected** because it introduces another consistency layer while PostgreSQL already stores the events. |
| PostgreSQL `UNIQUE(event_id)` + `ON CONFLICT` | **Chosen** because it is atomic, durable, and keeps the source of truth in one place.                    |

The PostgreSQL approach keeps the implementation simpler while also handling concurrent duplicate deliveries correctly.

---

## What I would change at 10,000 webhooks/second

At that scale, I would keep the API servers stateless and run multiple instances behind a load balancer.

For recording processing, I would move away from in-process goroutines and use a durable queue such as Kafka or SQS. Separate workers could then process recordings with retries and back-pressure without slowing down webhook ingestion.

For PostgreSQL, I would tune the connection pool and indexes based on actual load-test results. If the volume justified it, I would also consider partitioning and read replicas.

I would also add metrics around:

* Webhook throughput
* Duplicate webhook rate
* Database latency and errors
* Recording processing time
* Queue depth
* Failed recording jobs

I would measure these first and then optimize the actual bottlenecks rather than adding complexity prematurely.

---

## Tests

I added regression tests for the two issues:

* `TestDuplicateDeliveryIsIgnored` checks that repeated deliveries with the same `event_id` are processed only once.
* `TestRecordingProcessedAfterRequestReturns` checks that recording processing still completes after the HTTP request has returned.

The full test suite passes with:

```bash
go test ./...
```

I could not run the Go race detector in the local Windows environment because CGO was not available. The relevant shared-state code was reviewed for concurrent access, and the cache now protects writes with its mutex.
