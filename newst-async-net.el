;;; newst-async-net.el --- Async feed download queue for Newsticker  -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026

;; Author: Iris Lennon
;; URL: https://github.com/ningxilai/newst-async-net
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: news, feed, newsticker

;; SPDX-License-Identifier: MIT

;;; Commentary:

;; Concurrent feed download queue for Newsticker using `url-retrieve'.
;;
;; The scheduler is demand driven.  Every request is a job in a priority
;; queue, and jobs are ranked by a small utility function:
;;
;;   score = staleness-weight * age-since-last-success
;;         - latency-weight   * expected-latency
;;         - failure-weight   * retry-count
;;         + user-priority
;;
;; Higher scores are served first.  Age is the dominant term, so old feeds
;; cannot starve; expected latency breaks ties in favour of cheap requests
;; (shortest-job-first); retries are penalised so a broken feed cannot
;; monopolise the queue.  Ties are broken by FIFO sequence number.
;;
;; Concurrency is controlled by a hierarchical AIMD (additive-increase /
;; multiplicative-decrease) controller, one instance for the whole queue and
;; one per host.  A controller increases its window by one after `cwnd'
;; successful requests while there is backlog and latency/error statistics
;; are healthy, and halves it on congestion (timeout, network failure or 5xx).
;; Client errors (4xx) and parse errors are recorded but do not reduce
;; concurrency: they are not congestion signals.
;;
;; Latency statistics are kept in a time-decayed, log-spaced histogram
;; (96 buckets from 1 ms to 120 s, 300 s half-life).  This gives robust p50/p95
;; estimates without storing individual samples and without letting ancient
;; observations influence the current window.  Timeouts are recorded as
;; failures, never as latency.  The adaptive timeout is derived from the p95
;; of the host (falling back to the global p95) with a safety multiplier.
;;
;; Hosts are protected by a per-host AIMD window, a per-host hard cap, and a
;; max-min fair share of the global window.  Dispatch is a two-pass algorithm:
;; the first pass gives every backlogged host at most its fair share; the
;; second pass lets a host use otherwise idle slots, still subject to its own
;; congestion window.  This avoids both starvation and head-of-line blocking.
;;
;; Cache updates are applied to memory immediately, but SQLite writes are
;; coalesced per feed and flushed in bounded batches when Emacs is idle.  New
;; downloads yield to pending input, so a background refresh does not compete
;; with typing, scrolling, or redisplay.  `newst-async-net-flush-cache' forces
;; a durable write, and `kill-emacs-hook' flushes any remaining dirty feeds.

;;; Code:

(require 'cl-lib)
(require 'url-parse)
(require 'newst-async-net-sql)

(eval-when-compile
  (require 'newsticker nil t))

(defvar newst-async-net-sql--immediate-save)

(declare-function newsticker--cache-get-feed "newst-backend.el")
(declare-function newsticker--cache-save-feed "newst-backend.el")
(declare-function newsticker--get-news-by-url-callback "newst-backend.el")

(defgroup newst-async-net nil
  "Async feed download queue for Newsticker."
  :group 'newsticker)

(defcustom newst-async-net-auto-scale-enabled t
  "When non-nil, adapt concurrency and timeout to measured runtime behaviour.
The adaptive controller is conservative: it grows only while there is backlog
and the recent latency/error statistics are healthy."
  :type 'boolean
  :group 'newst-async-net)

(defcustom newst-async-net-max-concurrent 3
  "Static concurrency when auto-scaling is disabled.
When auto-scaling is enabled this value is used as an upper bound only if it
has been customized by the user; otherwise `newst-async-net-hardcap-global'
is the upper bound."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-min-concurrent 2
  "Minimum global concurrency when auto-scaling is enabled."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-hardcap-global 128
  "Absolute global upper bound for active downloads; a safety cap."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-per-host-hardcap 8
  "Absolute limit for active requests to the same host."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-timeout 15
  "Default request timeout in seconds.
With auto-scaling enabled this is also the lower bound for the adaptive
timeout, so a fast feed is never given an unreasonably short deadline."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-timeout-hardcap 60
  "Upper bound for the adaptive timeout, in seconds."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-target-p95-ms 2000
  "Target p95 latency in milliseconds.
When the recent p95 exceeds this value the controller treats the queue as
congested and lowers its concurrency window."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-max-congestion-rate 0.10
  "Maximum tolerated congestion-event rate before backing off.
Congestion events are timeouts, network failures and HTTP 5xx responses.
Client errors and parse errors are deliberately excluded."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-stats-half-life 300
  "Half-life in seconds for latency and error statistics.
Older observations are exponentially down-weighted so the controller reacts
to the current network conditions rather than to historical outliers."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-decrease-cooldown 3.0
  "Minimum seconds between multiplicative-decrease events."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-increase-interval 0.25
  "Minimum seconds between additive-increase events."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-host-fair-share t
  "When non-nil, give every backlogged host a fair share of the global window.
The fair share prevents one host with many feeds from occupying every slot.
A second dispatch pass lets busy hosts use slots that other hosts cannot use."
  :type 'boolean
  :group 'newst-async-net)

(defcustom newst-async-net-defer-on-input t
  "When non-nil, do not start new downloads while input is pending.
In-flight requests continue, but dispatch is postponed briefly so typing,
scrolling and other interactive commands are handled first.  This keeps a
large background refresh from competing with reading."
  :type 'boolean
  :group 'newst-async-net)

(defcustom newst-async-net-input-defer-delay 0.05
  "Seconds to wait before retrying dispatch while input is pending."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-save-idle-delay 0.4
  "Idle seconds before dirty feed caches are written to storage.
Cache writes are coalesced and performed only when Emacs is idle, keeping
SQLite transactions out of the interactive path.  Set to 0 to flush as soon
as Emacs becomes idle."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-save-batch-size 16
  "Maximum number of dirty feeds persisted by one idle flush.
A smaller batch shortens each flush but may require more idle cycles for a
large refresh."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-max-retries 1
  "Maximum number of automatic retries for transient failures.
Only timeouts, network failures and HTTP 5xx responses are retried.  Set to
0 to disable retries."
  :type 'integer
  :group 'newst-async-net)

(defcustom newst-async-net-retry-base 2.0
  "Base delay in seconds for exponential retry backoff."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-retry-max 60.0
  "Maximum retry delay in seconds."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-never-fetched-age 86400
  "Assumed age in seconds for a feed that has never completed a download.
A large value makes never-fetched feeds sort ahead of feeds that were
successfully fetched recently."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-staleness-weight 1.0
  "Weight of feed staleness in the priority score.
The score is in arbitrary units; one second of extra staleness is worth this
many points."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-latency-weight 5.0
  "Weight of expected latency in the priority score.
Expected latency is measured in seconds, so the default makes one second of
expected download time cost five seconds of staleness.  This is a
shortest-job-first tie-breaker, not a starvation mechanism."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-failure-weight 30.0
  "Penalty per retry in the priority score, in staleness-seconds."
  :type 'number
  :group 'newst-async-net)

(defcustom newst-async-net-priority-function nil
  "Optional function returning a user priority for a feed.
It is called with FEED-NAME and URL and should return a number (higher is
served sooner).  The value is added directly to the priority score."
  :type '(choice (const :tag "None" nil) function)
  :group 'newst-async-net)

(defvar newst-async-net-debug nil
  "When non-nil, print diagnostic messages for download queue.")

(defun newst-async-net-debug (fmt &rest args)
  "Print a diagnostic message formatted with FMT and ARGS."
  (when newst-async-net-debug
    (apply #'message (concat "[async-net] " fmt) args)))

;;; Statistics -------------------------------------------------------------

(defconst newst-async-net--bucket-count 96
  "Number of logarithmic latency buckets.")

(defconst newst-async-net--min-latency-ms 1.0
  "Lower bound of the latency histogram, in milliseconds.")

(defconst newst-async-net--max-latency-ms 120000.0
  "Upper bound of the latency histogram, in milliseconds.")

(defconst newst-async-net--log-ratio
  (/ (log (/ newst-async-net--max-latency-ms
             newst-async-net--min-latency-ms))
     newst-async-net--bucket-count)
  "Logarithmic width of one latency bucket.")

(defconst newst-async-net--min-samples 5
  "Minimum successful samples before quantiles are trusted.")

(defconst newst-async-net--max-host-states 256
  "Maximum number of per-host statistic objects kept in memory.")

(cl-defstruct (newst-async-net--stats
               (:constructor newst-async-net--make-stats))
  "Time-decayed latency and outcome statistics."
  (buckets (make-vector newst-async-net--bucket-count 0.0))
  (ok 0.0)
  (err 0.0)
  (congestion 0.0)
  (timeouts 0.0)
  (last 0.0))

(cl-defstruct (newst-async-net--ctrl
               (:constructor newst-async-net--make-ctrl))
  "State of one AIMD congestion controller."
  (cwnd newst-async-net-min-concurrent)
  (successes 0)
  (last-increase 0.0)
  (last-decrease 0.0))

(cl-defstruct (newst-async-net--host-state
               (:constructor newst-async-net--make-host-state))
  "Per-host statistics and controller state."
  (stats (newst-async-net--make-stats))
  (ctrl (newst-async-net--make-ctrl))
  (active 0)
  (pending 0)
  (last-seen 0.0))

(cl-defstruct (newst-async-net--job
               (:constructor newst-async-net--make-job))
  "One queued feed download."
  feed
  url
  host
  seq
  (attempts 0)
  (user-priority 0.0))

(defun newst-async-net--now ()
  "Return the current time as a float number of seconds."
  (float-time))

(defun newst-async-net--bucket-index (ms)
  "Return the histogram bucket index for latency MS."
  (let ((ms (max newst-async-net--min-latency-ms
                 (min newst-async-net--max-latency-ms
                      (float (or ms 0.0))))))
    (min (1- newst-async-net--bucket-count)
         (floor (/ (log (/ ms newst-async-net--min-latency-ms))
                   newst-async-net--log-ratio)))))

(defun newst-async-net--bucket-value (index)
  "Return the upper edge of bucket INDEX, in milliseconds."
  (exp (+ (log newst-async-net--min-latency-ms)
          (* (1+ index) newst-async-net--log-ratio))))

(defun newst-async-net--stats-decay (stats now)
  "Exponentially decay STATS to NOW."
  (let ((dt (- now (newst-async-net--stats-last stats))))
    (when (> dt 0.0)
      (let* ((half-life (max 1.0 (float newst-async-net-stats-half-life)))
             (factor (expt 0.5 (/ dt half-life)))
             (buckets (newst-async-net--stats-buckets stats)))
        (dotimes (i newst-async-net--bucket-count)
          (aset buckets i (* (aref buckets i) factor)))
        (setf (newst-async-net--stats-ok stats)
              (* (newst-async-net--stats-ok stats) factor)
              (newst-async-net--stats-err stats)
              (* (newst-async-net--stats-err stats) factor)
              (newst-async-net--stats-congestion stats)
              (* (newst-async-net--stats-congestion stats) factor)
              (newst-async-net--stats-timeouts stats)
              (* (newst-async-net--stats-timeouts stats) factor)
              (newst-async-net--stats-last stats) now)))))

(defun newst-async-net--stats-record (stats now kind latency-ms)
  "Record outcome KIND at NOW in STATS.
LATENCY-MS is only meaningful for successful requests."
  (newst-async-net--stats-decay stats now)
  (if (eq kind 'ok)
      (progn
        (cl-incf (newst-async-net--stats-ok stats))
        (let ((index (newst-async-net--bucket-index latency-ms))
              (buckets (newst-async-net--stats-buckets stats)))
          (aset buckets index (+ (aref buckets index) 1.0))))
    (cl-incf (newst-async-net--stats-err stats))
    (when (memq kind '(timeout network http-server))
      (cl-incf (newst-async-net--stats-congestion stats)))
    (when (eq kind 'timeout)
      (cl-incf (newst-async-net--stats-timeouts stats))))
  (setf (newst-async-net--stats-last stats) now))

(defun newst-async-net--stats-quantile (stats q)
  "Return the Q quantile of successful latency in STATS, or nil.
Nil means there are not enough recent successful samples to trust the value."
  (newst-async-net--stats-decay stats (newst-async-net--now))
  (let ((weight (newst-async-net--stats-ok stats)))
    (when (>= weight newst-async-net--min-samples)
      (let ((target (* q weight))
            (cumulative 0.0)
            (index 0)
            (buckets (newst-async-net--stats-buckets stats))
            (last (1- newst-async-net--bucket-count)))
        (while (and (< index last)
                    (< (+ cumulative (aref buckets index)) target))
          (cl-incf cumulative (aref buckets index))
          (cl-incf index))
        (newst-async-net--bucket-value index)))))

(defun newst-async-net--stats-mean-ms (stats)
  "Return the decayed mean successful latency in STATS, or nil."
  (newst-async-net--stats-decay stats (newst-async-net--now))
  (let ((weight (newst-async-net--stats-ok stats)))
    (when (> weight 0.0)
      (let ((sum 0.0)
            (buckets (newst-async-net--stats-buckets stats)))
        (dotimes (i newst-async-net--bucket-count)
          (cl-incf sum (* (aref buckets i)
                          (newst-async-net--bucket-value i))))
        (/ sum weight)))))

(defun newst-async-net--stats-congestion-rate (stats)
  "Return the decayed congestion-event rate of STATS."
  (newst-async-net--stats-decay stats (newst-async-net--now))
  (let ((denominator (+ (newst-async-net--stats-ok stats)
                        (newst-async-net--stats-congestion stats))))
    (if (> denominator 0.0)
        (/ (newst-async-net--stats-congestion stats) denominator)
      0.0)))

(defun newst-async-net--stats-error-rate (stats)
  "Return the decayed overall error rate of STATS."
  (newst-async-net--stats-decay stats (newst-async-net--now))
  (let ((denominator (+ (newst-async-net--stats-ok stats)
                        (newst-async-net--stats-err stats))))
    (if (> denominator 0.0)
        (/ (newst-async-net--stats-err stats) denominator)
      0.0)))

(defun newst-async-net--stats-healthy-p (stats)
  "Return non-nil if STATS indicate a healthy connection."
  (and (< (newst-async-net--stats-congestion-rate stats)
          newst-async-net-max-congestion-rate)
       (let ((p95 (newst-async-net--stats-quantile stats 0.95)))
         (or (null p95)
             (<= p95 newst-async-net-target-p95-ms)))))

;;; Controllers ------------------------------------------------------------

(defvar newst-async-net--global-stats (newst-async-net--make-stats)
  "Global latency/error statistics.")

(defvar newst-async-net--global-ctrl (newst-async-net--make-ctrl)
  "Global AIMD controller.")

(defun newst-async-net--ctrl-increase (ctrl max-cwnd now)
  "Additively increase CTRL up to MAX-CWND at time NOW."
  (cl-incf (newst-async-net--ctrl-successes ctrl))
  (let ((cwnd (newst-async-net--ctrl-cwnd ctrl)))
    (when (and (>= (newst-async-net--ctrl-successes ctrl) (max 1 cwnd))
               (>= now (+ (newst-async-net--ctrl-last-increase ctrl)
                          newst-async-net-increase-interval)))
      (setf (newst-async-net--ctrl-cwnd ctrl) (min max-cwnd (1+ cwnd))
            (newst-async-net--ctrl-successes ctrl) 0
            (newst-async-net--ctrl-last-increase ctrl) now))))

(defun newst-async-net--ctrl-decrease (ctrl min-cwnd now)
  "Multiplicatively decrease CTRL down to MIN-CWND at time NOW."
  (setf (newst-async-net--ctrl-cwnd ctrl)
        (max min-cwnd (floor (newst-async-net--ctrl-cwnd ctrl) 2))
        (newst-async-net--ctrl-successes ctrl) 0
        (newst-async-net--ctrl-last-decrease ctrl) now))

(defun newst-async-net--maybe-decrease (ctrl min-cwnd now)
  "Decrease CTRL at NOW unless the cooldown has not elapsed."
  (when (>= now (+ (newst-async-net--ctrl-last-decrease ctrl)
                   newst-async-net-decrease-cooldown))
    (newst-async-net--ctrl-decrease ctrl min-cwnd now)))

;;; Host state -------------------------------------------------------------

(defvar newst-async-net--host-states (make-hash-table :test 'equal)
  "Map host name to `newst-async-net--host-state'.")

(defvar newst-async-net--last-success (make-hash-table :test 'equal)
  "Map (FEED . URL) to the time of the last successful download.")

(defun newst-async-net--host-from-url (url)
  "Return a host string for URL, or nil if it cannot be parsed."
  (condition-case nil
      (let* ((parsed (url-generic-parse-url url))
             (host (url-host parsed)))
        (if (stringp host) host nil))
    (error nil)))

(defun newst-async-net--evict-host-state ()
  "Remove one idle, least-recently-seen host state, if possible."
  (let (victim
        (victim-time nil))
    (maphash (lambda (key state)
               (when (and (zerop (newst-async-net--host-state-active state))
                          (zerop (newst-async-net--host-state-pending state))
                          (or (null victim-time)
                              (< (newst-async-net--host-state-last-seen state)
                                 victim-time)))
                 (setq victim key
                       victim-time (newst-async-net--host-state-last-seen state))))
             newst-async-net--host-states)
    (when victim
      (remhash victim newst-async-net--host-states))))

(defun newst-async-net--host-state (host &optional create)
  "Return the state for HOST.
When CREATE is non-nil, create the state if necessary."
  (let ((key (or host "")))
    (or (gethash key newst-async-net--host-states)
        (when create
          (when (>= (hash-table-count newst-async-net--host-states)
                    newst-async-net--max-host-states)
            (newst-async-net--evict-host-state))
          (let ((state (newst-async-net--make-host-state)))
            (puthash key state newst-async-net--host-states)
            state)))))

(defun newst-async-net--host-active (host)
  "Return the number of active requests for HOST."
  (let ((state (newst-async-net--host-state host)))
    (if state (newst-async-net--host-state-active state) 0)))

(defun newst-async-net--host-pending (host)
  "Return the number of queued requests for HOST."
  (let ((state (newst-async-net--host-state host)))
    (if state (newst-async-net--host-state-pending state) 0)))

(defun newst-async-net--host-release (host)
  "Release one active request slot for HOST."
  (let ((state (newst-async-net--host-state host)))
    (when state
      (setf (newst-async-net--host-state-active state)
            (max 0 (1- (newst-async-net--host-state-active state)))
            (newst-async-net--host-state-last-seen state)
            (newst-async-net--now)))))

;;; Queue and ranking ------------------------------------------------------

(defvar newst-async-net--active 0
  "Number of currently active feed downloads.")

(defvar newst-async-net--pending (make-hash-table :test 'equal)
  "Map (FEED . URL) to queued `newst-async-net--job' objects.")

(defvar newst-async-net--active-jobs (make-hash-table :test 'equal)
  "Map (FEED . URL) to active `newst-async-net--job' objects.")

(defvar newst-async-net--seq 0
  "Monotonic sequence number used for FIFO tie-breaking.")

(defvar newst-async-net--dispatch-timer nil
  "Timer used to debounce dispatch requests.")

;;; Cache persistence ------------------------------------------------------

(defvar newst-async-net--dirty-feeds (make-hash-table :test 'equal)
  "Feed names whose in-memory cache needs to be written to storage.")

(defvar newst-async-net--save-timer nil
  "Idle timer used to flush `newst-async-net--dirty-feeds'.")

(defun newst-async-net--cancel-save-timer ()
  "Cancel the pending idle save timer."
  (when (timerp newst-async-net--save-timer)
    (cancel-timer newst-async-net--save-timer))
  (setq newst-async-net--save-timer nil))

(defun newst-async-net--schedule-save ()
  "Schedule a dirty-cache flush for the next idle period."
  (unless (timerp newst-async-net--save-timer)
    (setq newst-async-net--save-timer
          (run-with-idle-timer
           (max 0.0 (float newst-async-net-save-idle-delay))
           nil #'newst-async-net--flush-dirty-feeds))))

(defun newst-async-net--queue-feed-save (feed-name)
  "Mark FEED-NAME as dirty and arrange an idle cache write."
  (puthash feed-name t newst-async-net--dirty-feeds)
  (newst-async-net--schedule-save))

(defun newst-async-net--flush-dirty-feeds (&optional all)
  "Persist dirty feed caches, up to one idle batch.
When ALL is non-nil, persist every dirty feed.  Return the number of feeds
written.  Errors are logged and the affected feed is dropped from the dirty
set; a later fetch will mark it dirty again."
  (newst-async-net--cancel-save-timer)
  (let ((remaining (if all
                       most-positive-fixnum
                     (max 1 (or newst-async-net-save-batch-size 1))))
        (names nil)
        (saved 0))
    (when (fboundp 'newsticker--cache-get-feed)
      (maphash (lambda (name _)
                 (when (> remaining 0)
                   (push name names)
                   (cl-decf remaining)))
               newst-async-net--dirty-feeds)
      (dolist (name names)
        (remhash name newst-async-net--dirty-feeds)
        (condition-case err
            (let ((feed (newsticker--cache-get-feed name)))
              (when feed
                (let ((newst-async-net-sql--immediate-save t))
                  (newsticker--cache-save-feed feed))
                (cl-incf saved)))
          (error
           (newst-async-net-debug "cache save failed for %s: %S"
                                  name err)))))
    (when (> (hash-table-count newst-async-net--dirty-feeds) 0)
      (newst-async-net--schedule-save))
    saved))

(defun newst-async-net--flush-all-dirty-feeds ()
  "Flush all dirty caches without signalling, for `kill-emacs-hook'."
  (condition-case err
      (newst-async-net--flush-dirty-feeds t)
    (error
     (newst-async-net-debug "final cache flush failed: %S" err))))

(add-hook 'kill-emacs-hook #'newst-async-net--flush-all-dirty-feeds)

(defun newst-async-net-flush-cache ()
  "Persist all pending feed cache writes immediately.
The scheduler normally coalesces writes and performs them when Emacs is idle;
use this command when an immediate durable cache is required."
  (interactive)
  (let ((saved (newst-async-net--flush-dirty-feeds t)))
    (when (called-interactively-p 'interactive)
      (message "newst-async-net: saved %d feed%s"
               saved (if (= saved 1) "" "s")))
    saved))

(defun newst-async-net--job-key (job)
  "Return the hash key for JOB."
  (cons (newst-async-net--job-feed job)
        (newst-async-net--job-url job)))

(defun newst-async-net--customized-p (symbol)
  "Return non-nil if SYMBOL's value differs from its defcustom default."
  (let ((standard (get symbol 'standard-value)))
    (and standard
         (condition-case nil
             (not (equal (eval (car standard) t)
                         (symbol-value symbol)))
           (error nil)))))

(defun newst-async-net--global-cap ()
  "Return the maximum global concurrency permitted by configuration."
  (if (newst-async-net--customized-p 'newst-async-net-max-concurrent)
      (max 1 (min newst-async-net-max-concurrent
                  newst-async-net-hardcap-global))
    (max 1 newst-async-net-hardcap-global)))

(defun newst-async-net--global-limit ()
  "Return the current global concurrency limit."
  (if (not newst-async-net-auto-scale-enabled)
      (max 1 (min newst-async-net-max-concurrent
                  newst-async-net-hardcap-global))
    (max 1 (min (newst-async-net--global-cap)
                (max newst-async-net-min-concurrent
                     (newst-async-net--ctrl-cwnd
                      newst-async-net--global-ctrl))))))

(defun newst-async-net--host-limit (host limit backlogged &optional no-fair)
  "Return the concurrency limit for HOST.
LIMIT is the global limit and BACKLOGGED is the number of hosts with queued
work.  When NO-FAIR is non-nil, ignore the fair-share restriction."
  (let* ((state (newst-async-net--host-state host t))
         (cap (if newst-async-net-auto-scale-enabled
                  (min newst-async-net-per-host-hardcap
                       (max 1 (newst-async-net--ctrl-cwnd
                               (newst-async-net--host-state-ctrl state))))
                newst-async-net-per-host-hardcap))
         (fair (if (or no-fair
                       (not newst-async-net-host-fair-share))
                   newst-async-net-per-host-hardcap
                 (max 1 (ceiling (/ (float limit)
                                    (max 1 backlogged)))))))
    (max 1 (min cap fair newst-async-net-per-host-hardcap))))

(defun newst-async-net--user-priority (feed-name url)
  "Return the user priority for FEED-NAME and URL."
  (if (functionp newst-async-net-priority-function)
      (condition-case nil
          (let ((value (funcall newst-async-net-priority-function
                                feed-name url)))
            (if (numberp value) (float value) 0.0))
        (error 0.0))
    0.0))

(defun newst-async-net--job-staleness (job now)
  "Return the staleness of JOB in seconds at NOW."
  (let ((last (gethash (newst-async-net--job-key job)
                       newst-async-net--last-success)))
    (if last
        (max 0.0 (- now last))
      (float newst-async-net-never-fetched-age))))

(defun newst-async-net--expected-latency-ms (host)
  "Return the expected latency in milliseconds for HOST."
  (let ((state (newst-async-net--host-state host)))
    (or (and state
             (newst-async-net--stats-mean-ms
              (newst-async-net--host-state-stats state)))
        (newst-async-net--stats-mean-ms newst-async-net--global-stats)
        0.0)))

(defun newst-async-net--job-score (job now expected-ms)
  "Return the priority score of JOB at NOW.
EXPECTED-MS is the expected latency of the job's host."
  (+ (* (float newst-async-net-staleness-weight)
        (newst-async-net--job-staleness job now))
     (- (* (float newst-async-net-latency-weight)
           (/ (float expected-ms) 1000.0)))
     (- (* (float newst-async-net-failure-weight)
           (newst-async-net--job-attempts job)))
     (newst-async-net--job-user-priority job)))

(defun newst-async-net--ranked-ready-jobs (now)
  "Return queued jobs sorted by descending priority at NOW."
  (let ((means (make-hash-table :test 'equal))
        (scored nil))
    (maphash
     (lambda (key job)
       (unless (gethash key newst-async-net--active-jobs)
         (let* ((host (newst-async-net--job-host job))
                (mean (or (gethash host means)
                          (puthash host
                                   (newst-async-net--expected-latency-ms host)
                                   means))))
           (push (cons (newst-async-net--job-score job now mean) job)
                 scored))))
     newst-async-net--pending)
    (setq scored
          (sort scored
                (lambda (a b)
                  (let ((sa (car a))
                        (sb (car b))
                        (ja (cdr a))
                        (jb (cdr b)))
                    (cond
                     ((> sa sb) t)
                     ((< sa sb) nil)
                     ((< (newst-async-net--job-seq ja)
                         (newst-async-net--job-seq jb))
                      t)
                     ((> (newst-async-net--job-seq ja)
                         (newst-async-net--job-seq jb))
                      nil)
                     (t (string< (or (newst-async-net--job-feed ja) "")
                                 (or (newst-async-net--job-feed jb) ""))))))))
    (mapcar #'cdr scored)))

(defun newst-async-net--distinct-hosts (jobs)
  "Return the number of distinct hosts among JOBS."
  (let ((table (make-hash-table :test 'equal)))
    (dolist (job jobs)
      (puthash (newst-async-net--job-host job) t table))
    (hash-table-count table)))

;;; Dispatch ---------------------------------------------------------------

(defun newst-async-net--schedule-dispatch (&optional delay)
  "Arrange for a dispatch on the next event-loop turn.
DELAY defaults to 0.01 seconds."
  (unless (timerp newst-async-net--dispatch-timer)
    (setq newst-async-net--dispatch-timer
          (run-at-time (or delay 0.01) nil #'newst-async-net--dispatch))))

(defun newst-async-net--enqueue (feed-name url &optional attempts)
  "Queue a download of URL for FEED-NAME.
ATTEMPTS is the number of retries already made."
  (let ((key (cons feed-name url)))
    (unless (or (gethash key newst-async-net--pending)
                (gethash key newst-async-net--active-jobs))
      (let* ((host (or (newst-async-net--host-from-url url) ""))
             (state (newst-async-net--host-state host t))
             (job (newst-async-net--make-job
                   :feed feed-name
                   :url url
                   :host host
                   :seq (cl-incf newst-async-net--seq)
                   :attempts (or attempts 0)
                   :user-priority (newst-async-net--user-priority
                                   feed-name url))))
        (puthash key job newst-async-net--pending)
        (cl-incf (newst-async-net--host-state-pending state))
        (newst-async-net-debug "queue %s (queued=%d)"
                               feed-name
                               (hash-table-count newst-async-net--pending))
        (newst-async-net--schedule-dispatch)))))

(defun newst-async-net--start (job)
  "Move JOB from the queue to the active set and start its download."
  (let* ((host (newst-async-net--job-host job))
         (key (newst-async-net--job-key job))
         (state (newst-async-net--host-state host t)))
    (when (gethash key newst-async-net--pending)
      (remhash key newst-async-net--pending)
      (setf (newst-async-net--host-state-pending state)
            (max 0 (1- (newst-async-net--host-state-pending state))))
      (puthash key job newst-async-net--active-jobs)
      (cl-incf newst-async-net--active)
      (cl-incf (newst-async-net--host-state-active state))
      (setf (newst-async-net--host-state-last-seen state)
            (newst-async-net--now))
      (newst-async-net-debug "start %s" (newst-async-net--job-url job))
      (newst-async-net--fetch-url job))))

(defun newst-async-net--dispatch ()
  "Start as many queued jobs as the current limits allow."
  (when (timerp newst-async-net--dispatch-timer)
    (cancel-timer newst-async-net--dispatch-timer))
  (setq newst-async-net--dispatch-timer nil)
  (if (and newst-async-net-defer-on-input
           (or (> newst-async-net--active 0)
               (> (hash-table-count newst-async-net--pending) 0))
           (input-pending-p))
      (progn
        (newst-async-net-debug "dispatch deferred: input pending")
        (newst-async-net--schedule-dispatch
         newst-async-net-input-defer-delay))
    (let* ((now (newst-async-net--now))
           (limit (newst-async-net--global-limit))
           (ready (newst-async-net--ranked-ready-jobs now))
           (hosts (newst-async-net--distinct-hosts ready)))
      ;; First pass: guarantee each backlogged host its fair share.
      ;; Second pass: fill slots that the fair-share pass could not use.
      (dolist (pass '(fair fill))
        (dolist (job ready)
          (when (and (< newst-async-net--active limit)
                     (< (newst-async-net--host-active
                         (newst-async-net--job-host job))
                        (newst-async-net--host-limit
                         (newst-async-net--job-host job)
                         limit hosts (eq pass 'fill))))
            (newst-async-net--start job))))
      (newst-async-net-debug "dispatch: active=%d/%d queued=%d"
                             newst-async-net--active
                             limit
                             (hash-table-count newst-async-net--pending)))))

;;; Outcomes ---------------------------------------------------------------

(defun newst-async-net--classify-url-error (error-flag)
  "Classify a `url-retrieve' ERROR-FLAG into an outcome symbol."
  (let ((code (and (consp error-flag)
                   (eq (car error-flag) 'error)
                   (eq (cadr error-flag) 'http)
                   (nth 2 error-flag))))
    (cond
     ((and (integerp code) (>= code 500)) 'http-server)
     ((and (integerp code) (>= code 400)) 'http-client)
     (t 'network))))

(defun newst-async-net--record-outcome (host kind latency-ms)
  "Record outcome KIND for HOST with LATENCY-MS and update controllers."
  (let* ((now (newst-async-net--now))
         (state (newst-async-net--host-state host t))
         (stats (newst-async-net--host-state-stats state))
         (global newst-async-net--global-stats))
    (newst-async-net--stats-record stats now kind latency-ms)
    (newst-async-net--stats-record global now kind latency-ms)
    (setf (newst-async-net--host-state-last-seen state) now)
    (if (eq kind 'ok)
        (newst-async-net--controller-success state now)
      (newst-async-net--controller-failure state now kind))))

(defun newst-async-net--controller-success (state now)
  "Update controllers after a success on the host described by STATE.
NOW is the current time."
  (when newst-async-net-auto-scale-enabled
    (let ((host-stats (newst-async-net--host-state-stats state))
          (global-stats newst-async-net--global-stats))
      (if (newst-async-net--stats-healthy-p host-stats)
          (when (> (newst-async-net--host-state-pending state) 0)
            (newst-async-net--ctrl-increase
             (newst-async-net--host-state-ctrl state)
             newst-async-net-per-host-hardcap
             now))
        (newst-async-net--maybe-decrease
         (newst-async-net--host-state-ctrl state) 1 now))
      (if (newst-async-net--stats-healthy-p global-stats)
          (when (> (hash-table-count newst-async-net--pending) 0)
            (newst-async-net--ctrl-increase
             newst-async-net--global-ctrl
             (newst-async-net--global-cap)
             now))
        (newst-async-net--maybe-decrease
         newst-async-net--global-ctrl
         newst-async-net-min-concurrent
         now)))))

(defun newst-async-net--controller-failure (state now kind)
  "Update controllers after failure KIND on the host described by STATE.
NOW is the current time.  Client and parse errors are not congestion."
  (when (and newst-async-net-auto-scale-enabled
             (memq kind '(timeout network http-server)))
    (let ((host-stats (newst-async-net--host-state-stats state))
          (global-stats newst-async-net--global-stats))
      (unless (newst-async-net--stats-healthy-p host-stats)
        (newst-async-net--maybe-decrease
         (newst-async-net--host-state-ctrl state) 1 now))
      (unless (newst-async-net--stats-healthy-p global-stats)
        (newst-async-net--maybe-decrease
         newst-async-net--global-ctrl
         newst-async-net-min-concurrent
         now)))))

(defun newst-async-net--retry-delay (attempts)
  "Return a jittered exponential backoff delay for ATTEMPTS."
  (let* ((base (* (float newst-async-net-retry-base)
                  (expt 2.0 (max 0 (1- attempts)))))
         (cap (min (float newst-async-net-retry-max) base)))
    (* cap (+ 0.5 (/ (random 100) 100.0)))))

(defun newst-async-net--maybe-retry (job kind)
  "Schedule a retry for JOB after failure KIND, when appropriate."
  (when (and (memq kind '(timeout network http-server))
             (< (newst-async-net--job-attempts job)
                newst-async-net-max-retries))
    (let ((feed-name (newst-async-net--job-feed job))
          (url (newst-async-net--job-url job))
          (attempts (1+ (newst-async-net--job-attempts job)))
          (failed-at (newst-async-net--now)))
      (newst-async-net-debug "retry %s in %.1fs (attempt %d)"
                             url
                             (newst-async-net--retry-delay attempts)
                             attempts)
      (run-at-time
       (newst-async-net--retry-delay attempts) nil
       (lambda ()
         (let ((last (gethash (cons feed-name url)
                              newst-async-net--last-success)))
           (unless (and last (> last failed-at))
             (newst-async-net--enqueue feed-name url attempts))))))))

(defun newst-async-net--finish (job kind latency-ms &optional status)
  "Finish JOB with outcome KIND and LATENCY-MS.
STATUS is passed to Newsticker's stock error reporting when this is the
final failure of JOB."
  (let* ((host (newst-async-net--job-host job))
         (key (newst-async-net--job-key job))
         (retry (unless (eq kind 'ok)
                  (newst-async-net--maybe-retry job kind))))
    (remhash key newst-async-net--active-jobs)
    (setq newst-async-net--active (max 0 (1- newst-async-net--active)))
    (newst-async-net--host-release host)
    (newst-async-net--record-outcome host kind latency-ms)
    (if (eq kind 'ok)
        (puthash key (newst-async-net--now) newst-async-net--last-success)
      (when (and (null retry)
                 (not (eq kind 'parse)))
        (newst-async-net--report-failure
         job (or status (list :error (list 'error kind))))))
    (newst-async-net-debug "finish %s: %s"
                           (newst-async-net--job-url job) kind)
    (newst-async-net--dispatch)))

;;; Fetching ---------------------------------------------------------------

(defun newst-async-net--timeout-for (host)
  "Return the adaptive timeout in seconds for HOST."
  (if (not newst-async-net-auto-scale-enabled)
      newst-async-net-timeout
    (let* ((state (newst-async-net--host-state host t))
           (p95 (or (newst-async-net--stats-quantile
                     (newst-async-net--host-state-stats state) 0.95)
                    (newst-async-net--stats-quantile
                     newst-async-net--global-stats 0.95))))
      (if (null p95)
          newst-async-net-timeout
        (min newst-async-net-timeout-hardcap
             (max newst-async-net-timeout
                  (ceiling (+ 1.0 (* 2.5 (/ p95 1000.0))))))))))

(defun newst-async-net--parse-and-process (job)
  "Parse and cache the current HTTP response for JOB.
This delegates to Newsticker's own URL callback so feed formats, dates,
retained elements, logos, coding systems, and cache semantics stay identical."
  (newsticker--get-news-by-url-callback nil (newst-async-net--job-feed job))
  'ok)

(defun newst-async-net--report-failure (job status)
  "Let Newsticker record its stock download-error entry for JOB.
STATUS is the `url-retrieve' status list, or a synthesized timeout status."
  (condition-case err
      (with-temp-buffer
        (newsticker--get-news-by-url-callback
         status (newst-async-net--job-feed job)))
    (error
     (newst-async-net-debug "could not record download failure for %s: %S"
                            (newst-async-net--job-feed job) err))))

(defun newst-async-net--kill-url-buffer (buffer)
  "Kill BUFFER and its process, ignoring errors."
  (when (buffer-live-p buffer)
    (let ((proc (get-buffer-process buffer)))
      (when (and proc (process-live-p proc))
        (delete-process proc)))
    (condition-case nil
        (kill-buffer buffer)
      (error nil))))

(defun newst-async-net--fetch-url (job)
  "Download JOB asynchronously and parse the XML."
  (let* ((url (newst-async-net--job-url job))
         (host (newst-async-net--job-host job))
         (timeout (max 1 (newst-async-net--timeout-for host)))
         (start (newst-async-net--now))
         (finished nil)
         (url-buffer nil)
         (timer nil))
    (cl-labels
        ((finish (kind latency-ms &optional status)
           (unless finished
             (setq finished t)
             (when (timerp timer)
               (cancel-timer timer))
             ;; If called from the url-retrieve callback, that callback's
             ;; unwind-protect owns the buffer.  Killing the current buffer
             ;; here would upset `with-current-buffer'.
             (when (and (buffer-live-p url-buffer)
                        (not (eq (current-buffer) url-buffer)))
               (newst-async-net--kill-url-buffer url-buffer))
             (newst-async-net--finish job kind latency-ms status)))
         (handle (buf status)
           (when (and (not finished)
                      (buffer-live-p buf))
             (unwind-protect
                 (with-current-buffer buf
                 (let ((error-flag (plist-get status :error)))
                   (if error-flag
                       (progn
                         (newst-async-net-debug
                          "download error %s: %S" url error-flag)
                         (finish
                          (newst-async-net--classify-url-error error-flag)
                          nil status))
                     (condition-case parse-err
                         (let ((kind (newst-async-net--parse-and-process job)))
                           (finish
                            kind
                            (and (eq kind 'ok)
                                 (round
                                  (* 1000.0
                                     (- (newst-async-net--now) start))))))
                       (error
                        (newst-async-net-debug
                         "parse error %s: %S" url parse-err)
                        (finish 'parse nil))))))
             (condition-case nil
                 (kill-buffer buf)
               (error nil))))))
      (setq timer
            (run-at-time
             timeout nil
             (lambda ()
               (newst-async-net-debug "timeout %s after %ss" url timeout)
               (finish 'timeout nil (list :error '(error timeout))))))
      (condition-case err
          (setq url-buffer
                (let ((coding-system-for-read 'no-conversion))
                  (url-retrieve
                   url
                   (lambda (status)
                     (handle (current-buffer) status))
                   nil t)))
        (error
         (newst-async-net-debug "url-retrieve failed %s: %S" url err)
         (finish 'network nil (list :error err)))))))

(defun newst-async-net-advice-get-news-by-url (_orig-fn feed-name url)
  ":around advice for `newsticker--get-news-by-url'."
  (newst-async-net--enqueue feed-name url)
  (force-mode-line-update)
  nil)

(advice-add 'newsticker--get-news-by-url :around
            #'newst-async-net-advice-get-news-by-url)

(defun newst-async-net-stats ()
  "Return a plist describing the current scheduler state."
  (interactive)
  (let* ((global newst-async-net--global-stats)
         (result
          (list :active newst-async-net--active
                :queued (hash-table-count newst-async-net--pending)
                :cwnd (newst-async-net--ctrl-cwnd
                       newst-async-net--global-ctrl)
                :limit (newst-async-net--global-limit)
                :p50-ms (newst-async-net--stats-quantile global 0.50)
                :p95-ms (newst-async-net--stats-quantile global 0.95)
                :samples (newst-async-net--stats-ok global)
                :timeouts (newst-async-net--stats-timeouts global)
                :congestion-rate
                (newst-async-net--stats-congestion-rate global)
                :error-rate (newst-async-net--stats-error-rate global))))
    (when (called-interactively-p 'interactive)
      (message "%S" result))
    result))

(provide 'newst-async-net)
;;; newst-async-net.el ends here
