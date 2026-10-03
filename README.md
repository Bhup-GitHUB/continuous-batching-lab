A toy inference server that shows how continuous batching schedulers are built. The model is fake. Timings come from a sleep-based cost model, not a GPU.

# continuous-batching-lab

This Go project lets you run an inference server, stream fake tokens with curl, and compare fixed and continuous batching under the same mixed-length workload. It uses `net/http`, the standard library, and `gonum/plot` for charts. Go 1.25 or newer is required.

## Run it

```sh
go run ./cmd/server --policy continuous --max-active 32 --kv-budget 20000 --step-base-ms 20 --step-per-active-ms 0.3 --port 8080
```

In another terminal:

```sh
curl -N http://localhost:8080/generate \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Explain continuous batching","max_tokens":4}'
curl -s http://localhost:8080/metrics
```

The stream contains one SSE event per token, followed by `done`:

```text
event: token
data: {"index":1,"text":"1:1"}

event: token
data: {"index":2,"text":"1:2"}

event: token
data: {"index":3,"text":"1:3"}

event: token
data: {"index":4,"text":"1:4"}

event: done
data: {"reason":"completed","tokens":4}
```

The text is `request_id:token_index`, starting at index 1. `X-Request-ID` identifies the request. `done.reason` can also be `stalled` or `shutdown`; a disconnected client cannot receive its cancellation event. Invalid JSON, nonpositive `max_tokens`, and requests exceeding a budget receive HTTP 400 before streaming begins.

All options have defaults; `go run ./cmd/server --help` lists them. Restart the server with `--policy fixed` to compare policies.

## Architecture

```text
                           net/http handlers
                            POST /generate
                         client-bound context
                                  |
                           bounded submit channel
                                  |
             +---------------------------------------------+
             |       ONE scheduler goroutine               |
             |                                             |
             | waiting --Prefill--> ready --admit--> active |
             |                             slots + KV      |
             |                                  |          |
             |                      Fake.DecodeStep(batch) |
             |                                  |          |
             |                   non-blocking token sends  |
             |                   pause / resume / retire   |
             +---------------------------------------------+
                        |                     ^
              per-request bounded       snapshot query channel
              output + result channels        |
                        |               GET /metrics, /trace
                   handler -> SSE

             Clock.Now / Clock.Sleep under all model costs
             real clock: timers; fake clock: advance instantly
```

Only the scheduler goroutine owns `waiting`, `ready`, `active`, slot assignment, KV reservations, totals, latency samples, and the optional trace. HTTP handlers submit requests and consume their output; they never call the model. Metrics handlers ask the loop for a copied snapshot through a channel. There are no mutexes around batch state.

One owner makes each iteration a clear state transition. Admission, decode, cancellation, and retirement cannot race each other. A handler disconnect signals its context; the loop observes it at a boundary and releases resources. The load generator separately uses a mutex to collect results from its concurrent clients.

## Walk through the loop

1. Drain the submissions queued at the iteration boundary into **waiting**. Drain a finite number so sustained producers cannot starve decoding. Submission timestamps include time spent in the channel or blocked submitting.
2. Remove cancelled requests from every collection. Release any occupied slot and reserved KV tokens, publish the result, and close the output channel.
3. Prefill waiting requests in FIFO order while their whole prompts fit the remaining per-iteration prefill budget. Move them to **ready**. A request's prompt must fit the configured budget by itself; no prompt is split across iterations.
4. Admit ready requests in FIFO order while both `max_active` and `kv_token_budget` allow it. Reserve `prompt_tokens + max_tokens` immediately. This conservative reservation guarantees that every admitted request can finish without requesting more KV capacity. FIFO can leave capacity unused behind a request whose reservation currently cannot fit.
5. Run one decode step over the runnable **active** requests. Each gets one token. Paused requests keep their slots and reservations but are excluded from the model batch.
6. Send tokens to each request's bounded channel without blocking. If a send fails, retain that token, mark that request paused, and keep decoding its peers. At subsequent boundaries, try the retained token first; resume only when output has room. Cancel a request if it stays paused for the stall timeout. This also handles a full channel on the final token without losing it.
7. Retire requests whose final token was enqueued. Free their slot and reservation, publish a result to a separate one-element channel, then close output. The handler drains buffered tokens before emitting `done`.
8. When there is no work, block on submission, snapshot queries, or shutdown. When all active requests are paused and prefill cannot make progress, wait for a short clock-driven poll interval instead of spinning. Cancellation and stall deadlines remain observable.

**Continuous** runs admission on every iteration. **Fixed** runs admission only when `active` is empty. Finished members still release resources and close their streams immediately under fixed; their freed slots stay unused until the last member of that batch retires. There is no artificial delay to gather the first batch.

The handler applies a write deadline to each SSE write. A blocked socket therefore cannot hold a handler indefinitely. Prefill and decode sleeps respond to server shutdown; individual cancellations are pruned between model calls, so one client does not interrupt its peers' step.

## Fake cost model

```text
prefill cost = prefill_base + prefill_per_token * prompt_tokens
decode cost  = step_base + step_per_active * runnable_requests
```

Prompt tokens are whitespace-separated words, with a minimum of one. Output lengths are exactly `max_tokens`, with no early EOS. By default prefill costs 2 ms plus 0.1 ms per prompt token; decode costs 20 ms plus 0.3 ms per runnable request. The dominant base cost makes batching more efficient: ideal decode throughput is about 49 tokens/s at batch size 1 and 1,081 tokens/s at batch size 32, before prefill, scheduling, and networking overhead.

`internal/clock.Clock` exposes `Now()` and context-aware `Sleep()`. The real clock uses timers. The fake clock advances immediately, and tests drive the same scheduler iteration synchronously. There are no goroutine timing races in the deterministic scheduling tests.

## The A/B/C example

All three requests are queued at time zero, with two active slots, zero prefill cost, and one time unit per decode step. A needs 2 tokens, B needs 6, C needs 2. A number is a token index; `.` is an unused slot.

```text
Step          1  2  3  4  5  6  7  8
Fixed slot 0 A1 A2  .  .  .  . C1 C2
Fixed slot 1 B1 B2 B3 B4 B5 B6  .  .

Step          1  2  3  4  5  6
Cont. slot 0 A1 A2 C1 C2  .  .
Cont. slot 1 B1 B2 B3 B4 B5 B6
```

Fixed first decodes C at step 7 and finishes everything at step 8. Continuous first decodes C at step 3 and finishes everything at step 6. Admission itself happens at the preceding boundary: time 6 versus time 2. This is enforced by `TestABC`.

## Generate load

```sh
go run ./cmd/loadgen --url http://localhost:8080 --rps 10 --duration 60s
go run ./cmd/loadgen --rps 20 --duration 10s \
  --short-tokens 8 --long-tokens 128 --long-fraction 0.3 \
  --slow-fraction 0.2 --slow-delay 100ms \
  --disconnect-fraction 0.1 --disconnect-after 2 --csv mixed.csv
```

Arrivals use a fixed interval at the target rate. A seeded random mix selects request lengths, slow readers, and early disconnects; the two client behaviors can overlap. Each request has a timeout, and the run drains outstanding requests after arrivals stop. `--max-inflight` limits concurrent clients; skipped arrivals are counted. A request selected to disconnect only does so if it reaches `--disconnect-after` before completion.

The table reports first-token, inter-token gap, and completion p50/p95/p99 in milliseconds, with sample counts. TTFT includes all clients receiving a token; completion percentiles include completed clients only. Throughput counts every received token, including partial streams, divided by elapsed time **including drain**. This is a finite-run comparison, not a steady-state GPU measurement. The CSV stores each request's status, timings, full gap list, and error. Failed requests cause a nonzero CLI exit after the CSV is written.

## Metrics

`GET /metrics` returns `waiting`, `ready`, `active`, `paused`, `kv_budget_used`, `kv_token_budget`, totals, `ttft_ms`, and `completion_ms`. Waiting counts requests already drained into the scheduler, not submissions still in its ingress channel. Active includes paused requests because they still own resources.

Totals count accepted submissions, completions, cancellations, stalls, generated tokens, delivered tokens, and decode steps. Stalls are included in cancellations. Server TTFT ends when the first token enters the output channel; completion ends when the final token enters it. These server measurements differ from client-observed latency. Nearest-rank percentiles use the most recent 10,000 samples per distribution, while totals are cumulative. Completion samples include successful requests only; TTFT samples include any request whose first token was delivered.

`--trace-limit 1000` enables a bounded slot occupancy trace. `GET /trace` returns the metrics plus completed slot intervals with request IDs, start/end milliseconds, delivered tokens, and retirement reasons. Intervals include time spent paused. Tracing defaults to off; a full trace increments `trace_dropped` rather than growing indefinitely. In-progress intervals are omitted.

## Charts

```sh
go run ./cmd/charts
go run ./cmd/charts --rates 2,5,8,12 --duration 10s --max-active 8 --out out
```

The chart runner starts a fresh server on a temporary loopback port for each policy and arrival rate, then uses the same HTTP load-generator implementation as `cmd/loadgen`. No separate server is required. Workload seeds and model costs are identical across policies; wall-clock timings will vary. Defaults use 8 slots and the server's default model costs to make batch starvation visible with modest traffic. Expect the full sweep to take a few minutes, including drain. Increase duration for more useful tail statistics.

It writes:

| File | Meaning |
| --- | --- |
| `out/ttft_vs_rps.png` | Client TTFT p50 and p99 versus target rate for both policies |
| `out/throughput_vs_rps.png` | Received tokens/s versus target rate for both policies, including drain |
| `out/timeline.png` | Stacked Gantt chart: continuous above fixed, two slots each, same eight-request mix |
| `out/summary.csv` | The rate-sweep measurements behind the two plots |
| `out/*_rate_*.csv` | Per-request measurements for each rate and policy |
| `out/*_timeline.csv`, `out/*_trace.json` | Client timings and server slot intervals behind the timeline |

The timeline uses a separate small run at 20 requests/s for 400 ms, with 8- and 32-token requests. Matching colors identify matching request IDs across policies. Gaps show unused slots. The rate sweep refuses to plot runs with failed requests or skipped arrivals, so timeouts do not silently improve a policy's apparent latency.

## Tests and source map

```sh
go test ./...
go test -race ./...
go test ./internal/scheduler -run TestABC -v
```

Tests cover A/B/C, ordered tokens including a retained final token, cancellation in every collection, disconnect-driven resource release, KV bounds, isolated slow-client pauses, stall expiry, deterministic replay, SSE completion, and load-generator stream handling. HTTP integration tests bind temporary loopback ports.

```text
cmd/server             flags, HTTP listener, graceful shutdown
cmd/loadgen            load-generator CLI
cmd/charts             local HTTP sweeps and gonum/plot rendering
internal/scheduler     state owner, iteration, HTTP adapter, tests
internal/model         fake prefill and decode costs
internal/clock         real and advancing clocks
internal/metrics       bounded latency samples and percentiles
internal/loadgen       shared HTTP workload, SSE reader, table, CSV
```

## Background and limitations

[ORCA (OSDI 2022)](https://www.usenix.org/conference/osdi22/presentation/yu) describes iteration-level scheduling and selective batching for transformer serving. Fish Audio's [July 23, 2026 engineering post](https://fish.audio/blog/tts-inference-optimization-s2-pro-free/) discusses continuous batching within a larger TTS inference stack. These are background reading for the scheduling idea. **This project is not a reconstruction of any company's scheduler.**

There are no real models, GPUs, kernels, tokenizer, prefix caching, vocoder stage, or chunked prefill. KV is an integer reservation, not allocated tensors or paged storage. Prefill runs serially with decode and can add inter-token latency; there is no overlap, priority scheduling, preemption, batching window, or production fairness scheme. The fake model cannot predict hardware throughput or any company's performance.

Admission reserves the worst-case output length, so it trades utilization for simple guarantees. The ingress and output channels are bounded, but the waiting and ready collections have no global request limit. TCP and HTTP buffering can absorb tiny token events, so a slow network reader may not immediately fill its scheduler output channel; the deterministic slow-client test consumes that channel directly. Tail estimates from short runs have few samples, and OS timer resolution, HTTP overhead, and arrival timing affect real measurements. The fake clock demonstrates scheduler behavior rather than simulating a complete network.
