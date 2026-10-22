# newst-enhanced

Enhanced Newsticker: adaptive, high-throughput feed handling with a stock-like experience under light load.

## Quick start

```elisp
(require 'newsticker)
(require 'newst-jsonrpc)
```

Then:

```text
M-x newsticker-start RET
M-x newsticker-plainview RET
```

## What it does

`newst-enhanced` keeps the bare `newsticker` feel in normal use, but replaces the
download queue with a demand-driven scheduler that measures the connection instead
of guessing from feed counts.

- **Latency model** — a time-decayed, log-spaced histogram (96 buckets, 1 ms–120 s,
  300 s half-life) provides robust p50/p95 estimates. Failures and timeouts are
  recorded as outcomes, never as latency samples.
- **AIMD concurrency** — a global controller and one controller per host increase
  their window after `cwnd` healthy successes while work is queued, and halve it
  when the measured congestion rate exceeds the configured threshold. Congestion
  means timeout, network error, or HTTP 5xx. Client errors (4xx) and parse errors
  do not reduce concurrency.
- **Fair scheduling** — jobs are ranked by
  `staleness − latency − failure penalty + user priority`; ties are FIFO. A
  two-pass dispatch gives every backlogged host its fair share first, then lets a
  busy host use otherwise idle slots.
- **Adaptive timeout** — timeout is derived from the host p95 (falling back to the
  global p95) with a safety multiplier, bounded by the configured timeout and a
  hard cap.
- **Retry with backoff** — transient failures are retried with exponential backoff
  and jitter, capped by `newst-async-net-max-retries` (default: one retry).
- **Resilient cache** — the SQLite backend backs up a corrupt `cache.db` and
  rebuilds it from the legacy prin1 files.

## Configuration

The defaults are deliberately conservative and adaptive:

```elisp
(setq newst-async-net-auto-scale-enabled t)
(setq newst-jsonrpc-auto-scale-enabled t)

(setq newst-async-net-min-concurrent 2)
(setq newst-async-net-hardcap-global 128)
(setq newst-async-net-per-host-hardcap 8)
(setq newst-async-net-host-fair-share t)

(setq newst-async-net-timeout 15)
(setq newst-async-net-timeout-hardcap 60)
(setq newst-async-net-max-retries 1)
```

Ranking weights (seconds of staleness are the unit):

```elisp
(setq newst-async-net-staleness-weight 1.0)
(setq newst-async-net-latency-weight 5.0)
(setq newst-async-net-failure-weight 30.0)
```

Optional per-feed priority; higher values are served sooner:

```elisp
(setq newst-async-net-priority-function
      (lambda (feed-name url)
        (if (member feed-name '("Important")) 100 0)))
```

Inspect the current window, queue, p50/p95 and error rates with:

```text
M-x newst-async-net-stats RET
```

Disable autoscaling to use fixed limits:

```elisp
(setq newst-async-net-auto-scale-enabled nil)
(setq newst-jsonrpc-auto-scale-enabled nil)
(setq newst-async-net-max-concurrent 3)
```

When autoscaling is enabled, a customized `newst-async-net-max-concurrent` becomes
an explicit upper bound; otherwise the global hard cap is the upper bound.

## Files

- `newst-jsonrpc.el` — streaming plainview reader
- `newst-async-net.el` — priority queue, latency statistics, AIMD, and async fetch
- `newst-sql.el` — SQLite cache and recovery layer

## Design goal

This package is intentionally adaptive by default: it preserves the original
experience under light load and keeps large feed sets usable under heavier load
without sacrificing safety.
