# newst-async-net

Enhanced Newsticker: adaptive, high-throughput feed handling with a stock-like experience under light load.

## Quick start

```elisp
(require 'newsticker)
(require 'newst-async-net)
```

Then:

```text
M-x newsticker-start RET
M-x newsticker-plainview RET
```

## What it does

`newst-async-net` keeps the bare `newsticker` feel in normal use, but replaces the
download queue with a demand-driven scheduler that measures the connection instead
of guessing from feed counts.

It is a drop-in backend: stock Newsticker commands, key bindings, buffer layout,
feed parsing, item ages, logos, and cache semantics are unchanged.  Loading the
package does not add a mode, replace plainview, or change navigation; the only
user-visible difference is smoother background refreshing.

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
- **Reading-friendly persistence** — in-memory cache updates stay immediate, but
  SQLite writes are coalesced per feed and flushed in small batches while Emacs is
  idle. New downloads also yield to pending input.
- **Resilient cache** — the SQLite backend backs up a corrupt `cache.db` and
  rebuilds it from the legacy prin1 files.

## Configuration

The defaults are deliberately conservative and adaptive:

```elisp
(setq newst-async-net-auto-scale-enabled t)

(setq newst-async-net-min-concurrent 2)
(setq newst-async-net-hardcap-global 128)
(setq newst-async-net-per-host-hardcap 8)
(setq newst-async-net-host-fair-share t)

(setq newst-async-net-timeout 15)
(setq newst-async-net-timeout-hardcap 60)
(setq newst-async-net-max-retries 1)

(setq newst-async-net-defer-on-input t)
(setq newst-async-net-input-defer-delay 0.05)
(setq newst-async-net-save-idle-delay 0.4)
(setq newst-async-net-save-batch-size 16)
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

### Reading responsiveness

Downloaded items enter the in-memory cache immediately, but their SQLite rows are
written later.  A feed is marked dirty at most once, and while Emacs is idle up to
`newst-async-net-save-batch-size` dirty feeds are persisted per flush; remaining
feeds are rescheduled for the next idle period.  All pending writes are flushed on
`kill-emacs`.  To persist immediately, run:

```text
M-x newst-async-net-flush-cache RET
```

When `newst-async-net-defer-on-input` is non-nil (the default), the scheduler does
not start new downloads while keyboard, mouse, or scroll input is pending.
In-flight downloads finish normally; dispatch is retried after
`newst-async-net-input-defer-delay` seconds.  Set the option to `nil` to keep
starting downloads during input.

Disable autoscaling to use fixed limits:

```elisp
(setq newst-async-net-auto-scale-enabled nil)
(setq newst-async-net-max-concurrent 3)
```

When autoscaling is enabled, a customized `newst-async-net-max-concurrent` becomes
an explicit upper bound; otherwise the global hard cap is the upper bound.

## Files

- `newst-async-net.el` — main entry; priority queue, latency statistics, AIMD, and async fetch
- `newst-async-net-sql.el` — SQLite cache and recovery layer

## Migration note

The package was previously referred to as `newst-enhanced`, and its components as
`newst-jsonrpc.el` and `newst-sql.el`.  The package name now matches the main file,
and the SQLite component is `newst-async-net-sql`.  The custom plainview pager has
been removed so the package stays a drop-in replacement for stock Newsticker; use
the normal `M-x newsticker-plainview` interface.

## Design goal

This package is intentionally adaptive by default: it preserves the original
experience under light load and keeps large feed sets usable under heavier load
without sacrificing safety.
