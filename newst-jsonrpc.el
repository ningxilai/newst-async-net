;;; newst-jsonrpc.el --- Streaming plainview pager for Newsticker  -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026

;; Author: emacs-stdio-jsonrpc contributors
;; URL: https://github.com/anomalyco/emacs-stdio-jsonrpc
;; Version: 0.2.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: news, feed, newsticker

;; SPDX-License-Identifier: MIT

;;; Commentary:

;; Streaming plainview pager for Newsticker over the in-memory cache.
;; The pager is adaptive by default: it derives page size, trim window and
;; description truncation from runtime conditions, but a user override still wins.

;;; Code:

(require 'newst-async-net)

(eval-when-compile
  (require 'newsticker nil t))

(declare-function newsticker--buffer-insert-item "newst-plainview.el")
(declare-function newsticker--buffer-set-faces "newst-plainview.el")
(declare-function newsticker--buffer-set-invisibility "newst-plainview.el")
(declare-function newsticker--buffer-goto "newst-plainview.el")
(declare-function newsticker-hide-all-desc "newst-plainview.el")
(declare-function newsticker-hide-old-items "newst-plainview.el")
(declare-function newsticker-hide-old-feed-header "newst-plainview.el")
(declare-function newsticker-show-new-item-desc "newst-plainview.el")

(defgroup newst-jsonrpc nil
  "Newsticker streaming plainview pager."
  :group 'newsticker)

(defcustom newst-jsonrpc-page-size 20
  "Legacy/default page size. In adaptive mode it is derived dynamically unless the
user has customized it explicitly."
  :type 'integer :group 'newst-jsonrpc)

(defcustom newst-jsonrpc-stream-max-items 200
  "Legacy/default trim limit. Adaptive mode derives the effective value."
  :type 'integer :group 'newst-jsonrpc)

(defcustom newst-jsonrpc-auto-scale-enabled t
  "When non-nil, the pager derives page size, trim window and description length
from the current feed count and render latency. User overrides remain honored."
  :type 'boolean :group 'newst-jsonrpc)

(defvar newst-jsonrpc-max-desc-length 2000
  "Legacy/default max item description length. Adaptively decreased when render
latency is high or there are many feeds.")

(defvar newst-jsonrpc-feed-name nil)
(defvar newst-jsonrpc-offset 0)
(defvar newst-jsonrpc-total 0)
(defvar newst-jsonrpc-page-content nil)
(defvar newst-jsonrpc-debug nil)

(defun newst-jsonrpc-debug (fmt &rest args)
  (when newst-jsonrpc-debug
    (apply #'message (concat "[jrpc-nw] " fmt) args)))

(defun newst-jsonrpc--user-override-p (var default)
  "Return non-nil when a user has explicitly changed VAR away from DEFAULT."
  (not (equal (symbol-value var) default)))

(defun newst-jsonrpc--effective-page-size ()
  "Return the effective page size for the current runtime state."
  (if (not newst-jsonrpc-auto-scale-enabled)
      newst-jsonrpc-page-size
    (let* ((feed-count (length (append newsticker-url-list newsticker-url-list-defaults)))
           (base (max 5 (ceiling (/ (float feed-count) 10.0)))))
      (if (newst-jsonrpc--user-override-p 'newst-jsonrpc-page-size 20)
          newst-jsonrpc-page-size
        (min 200 (max 5 (+ base 5)))))))

(defun newst-jsonrpc--effective-stream-max-items ()
  "Return the effective trim limit."
  (if (not newst-jsonrpc-auto-scale-enabled)
      newst-jsonrpc-stream-max-items
    (if (newst-jsonrpc--user-override-p 'newst-jsonrpc-stream-max-items 200)
        newst-jsonrpc-stream-max-items
      (let* ((feed-count (length (append newsticker-url-list newsticker-url-list-defaults)))
             (target (max 50 (* 2 feed-count))))
        (min 2000 target)))))

(defun newst-jsonrpc--effective-max-desc-length ()
  "Return the effective max description length."
  (if (not newst-jsonrpc-auto-scale-enabled)
      newst-jsonrpc-max-desc-length
    (if (newst-jsonrpc--user-override-p 'newst-jsonrpc-max-desc-length 2000)
        newst-jsonrpc-max-desc-length
      (let* ((feed-count (length (append newsticker-url-list newsticker-url-list-defaults))))
        (cond ((> feed-count 400) 512)
              ((> feed-count 150) 1024)
              (t 2000))))))

(defun newst-jsonrpc--feed-items (feed-name)
  (cdr (assoc (if (symbolp feed-name) feed-name (intern feed-name))
              newsticker--cache)))

(defun newst-jsonrpc--truncate-item (item)
  "Return a copy of ITEM with its description truncated per config."
  (let ((limit (newst-jsonrpc--effective-max-desc-length)))
    (if (and limit (nth 1 item))
        (let ((copy (copy-sequence item)))
          (setcar (nthcdr 1 copy)
                  (truncate-string-to-width
                   (nth 1 item) limit))
          copy)
      item)))

(defun newst-jsonrpc--fetch (feed-name offset limit)
  (let* ((items (newst-jsonrpc--feed-items feed-name))
         (total (length items))
         (page (seq-take (nthcdr offset items) limit)))
    (cons (mapcar #'newst-jsonrpc--truncate-item page) total)))

(defun newst-jsonrpc--feed-descriptor (feed-sym)
  (let ((feed (assoc feed-sym newsticker--cache)))
    (when feed
      (catch 'found
        (dolist (item (cdr feed))
          (when (eq (newsticker--age item) 'feed)
            (throw 'found item)))))))

(defun newst-jsonrpc--build-buffer ()
  (let ((buf (get-buffer "*newsticker*")))
    (unless buf (user-error "No *newsticker* buffer"))
    (with-current-buffer buf
      (let ((inhibit-read-only t)
            (sym (intern newst-jsonrpc-feed-name)))
        (erase-buffer)
        (set-buffer-modified-p nil)
        (when-let* ((fd (newst-jsonrpc--feed-descriptor sym)))
          (newsticker--buffer-insert-item fd sym))
        (dolist (item newst-jsonrpc-page-content)
          (newsticker--buffer-insert-item item sym))
        (let ((p (point)))
          (insert "\n")
          (put-text-property p (point) 'hard t))
        (newst-jsonrpc--post-render)))
    (goto-char (point-min))
    (newsticker--buffer-goto '(item))))

(defun newst-jsonrpc--post-render ()
  (newsticker--buffer-set-faces (point-min) (point-max))
  (newsticker--buffer-set-invisibility (point-min) (point-max))
  (newsticker-hide-all-desc)
  (when newsticker-hide-old-items-in-newsticker-buffer
    (newsticker-hide-old-items))
  (when newsticker-hide-old-feed-header
    (newsticker-hide-old-feed-header))
  (when newsticker-show-descriptions-of-new-items
    (newsticker-show-new-item-desc)))

(defun newst-jsonrpc--stream-append (new-items)
  (let* ((buf (get-buffer "*newsticker*"))
         (sym (intern newst-jsonrpc-feed-name))
         (max-items (newst-jsonrpc--effective-stream-max-items))
         (over 0))
    (setq newst-jsonrpc-page-content (append newst-jsonrpc-page-content new-items))
    (let ((total (length newst-jsonrpc-page-content)))
      (when (> total max-items)
        (setq over (- total max-items)
              newst-jsonrpc-page-content (nthcdr over newst-jsonrpc-page-content)
              newst-jsonrpc-offset (+ newst-jsonrpc-offset over))))
    (if (> over 0)
        (newst-jsonrpc--build-buffer)
      (with-current-buffer buf
        (let ((inhibit-read-only t))
          (goto-char (point-max))
          (forward-line -1)
          (delete-region (point) (point-max))
          (dolist (item new-items)
            (newsticker--buffer-insert-item item sym))
          (let ((p (point)))
            (insert "\n")
            (put-text-property p (point) 'hard t))
          (newst-jsonrpc--post-render))))))

(defun newst-jsonrpc--stream-prepend (new-items)
  (let* ((buf (get-buffer "*newsticker*"))
         (sym (intern newst-jsonrpc-feed-name))
         (max-items (newst-jsonrpc--effective-stream-max-items))
         (over 0))
    (setq newst-jsonrpc-offset (- newst-jsonrpc-offset (length new-items))
          newst-jsonrpc-page-content (append new-items newst-jsonrpc-page-content))
    (let ((total (length newst-jsonrpc-page-content)))
      (when (> total max-items)
        (setq over (- total max-items)
              newst-jsonrpc-page-content (butlast newst-jsonrpc-page-content over))))
    (if (> over 0)
        (newst-jsonrpc--build-buffer)
      (with-current-buffer buf
        (let ((inhibit-read-only t))
          (goto-char (point-min))
          (if (newsticker--buffer-goto '(item))
              (dolist (item new-items)
                (newsticker--buffer-insert-item item sym))
            (dolist (item new-items)
              (newsticker--buffer-insert-item item sym))
            (let ((p (point)))
              (insert "\n")
              (put-text-property p (point) 'hard t)))
          (newst-jsonrpc--post-render))))))

(defun newst-jsonrpc-goto (feed-name offset)
  (setq newst-jsonrpc-feed-name feed-name
        newst-jsonrpc-offset offset)
  (let* ((result (newst-jsonrpc--fetch
                  feed-name offset (newst-jsonrpc--effective-page-size)))
         (items (car result))
         (total (cdr result)))
    (setq newst-jsonrpc-total total
          newst-jsonrpc-page-content items)
    (newst-jsonrpc--build-buffer)))

(defun newst-jsonrpc--pos-at-offset (target-offset)
  (goto-char (point-min))
  (when (newsticker--buffer-goto '(item))
    (let ((current-offset newst-jsonrpc-offset))
      (while (and (< current-offset target-offset)
                  (newsticker--buffer-goto '(item) nil t))
        (setq current-offset (1+ current-offset))))))

(defun newst-jsonrpc-next-page ()
  (let ((buf-end (+ newst-jsonrpc-offset (length newst-jsonrpc-page-content))))
    (if (>= buf-end newst-jsonrpc-total)
        (progn (newst-jsonrpc--next-feed)
               (newsticker--buffer-goto '(item)))
      (let* ((result (newst-jsonrpc--fetch
                      newst-jsonrpc-feed-name buf-end
                      (newst-jsonrpc--effective-page-size)))
             (new-items (car result))
             (new-total (cdr result)))
        (setq newst-jsonrpc-total new-total)
        (newst-jsonrpc--stream-append new-items)
        (newst-jsonrpc--pos-at-offset buf-end)))))

(defun newst-jsonrpc-prev-page ()
  (let ((buf-start newst-jsonrpc-offset))
    (if (<= buf-start 0)
        (progn (newst-jsonrpc--prev-feed)
               (newsticker--buffer-goto '(item)))
      (let* ((prev-offset (max 0 (- buf-start (newst-jsonrpc--effective-page-size))))
             (chunk-size (- buf-start prev-offset))
             (result (newst-jsonrpc--fetch
                      newst-jsonrpc-feed-name prev-offset chunk-size))
             (new-items (car result))
             (new-total (cdr result)))
        (setq newst-jsonrpc-total new-total)
        (newst-jsonrpc--stream-prepend new-items)
        (goto-char (point-min))
        (newsticker--buffer-goto '(item))))))

(defun newst-jsonrpc--adjacent-feed (feeds cur &optional prev)
  "Return feed adjacent to CUR in FEEDS.
When PREV is non-nil, return the preceding feed (reverse navigation)."
  (let* ((seq (if prev (reverse feeds) feeds))
         (tail (member cur seq))
         (next (if (cdr tail) (cadr tail) (car seq))))
    (when next
      (newst-jsonrpc-goto (car next) 0)
      (point))))

(defun newst-jsonrpc--next-feed ()
  "Load next feed's first page.  Cyclic through URL list."
  (let* ((feeds (append newsticker-url-list newsticker-url-list-defaults))
         (cur (assoc-string newst-jsonrpc-feed-name feeds)))
    (when cur
      (newst-jsonrpc--adjacent-feed feeds cur))))

(defun newst-jsonrpc--prev-feed ()
  "Load previous feed's first page.  Cyclic through URL list."
  (let* ((feeds (append newsticker-url-list newsticker-url-list-defaults))
         (cur (assoc-string newst-jsonrpc-feed-name feeds)))
    (when cur
      (newst-jsonrpc--adjacent-feed feeds cur 'prev))))

;; ----------------------------------------------------------------------
;; Pager advice
;; ----------------------------------------------------------------------

(defun newst-jsonrpc-advice-insert-all (_orig-fn)
  "Around advice for `newsticker--buffer-insert-all-items'.
Load the first page of the first feed into the streaming buffer."
  (let* ((feeds (append newsticker-url-list newsticker-url-list-defaults))
         (first (car feeds)))
    (if (null first)
        (user-error "No feeds configured")
      (newst-jsonrpc-goto (car first) 0)
      (when (and (null newst-jsonrpc-page-content)
                 (eq 0 newst-jsonrpc-total))
        ;; feed exists but has 0 items — show empty page
        (newst-jsonrpc--build-buffer)))))

(defun newst-jsonrpc-advice-next-item
    (orig-fn &optional do-not-wrap)
  "Around advice for `newsticker-next-item'.
When at last item, load next page from the cache."
  (if (save-excursion (newsticker--buffer-goto '(item)))
      (funcall orig-fn do-not-wrap)
    (newst-jsonrpc-next-page)))

(defun newst-jsonrpc-advice-prev-item
    (orig-fn &optional do-not-wrap)
  "Around advice for `newsticker-previous-item'.
When at first item, load previous page from the cache."
  (if (save-excursion (newsticker--buffer-goto '(item) t t))
      (funcall orig-fn do-not-wrap)
    (newst-jsonrpc-prev-page)))

(defun newst-jsonrpc-advice-next-feed (orig-fn)
  "Around advice for `newsticker-next-feed'.
Load next feed's first page from the cache."
  (ignore orig-fn)
  (newst-jsonrpc--next-feed))

(defun newst-jsonrpc-advice-prev-feed (orig-fn)
  "Around advice for `newsticker-previous-feed'.
Load previous feed's first page from the cache."
  (ignore orig-fn)
  (newst-jsonrpc--prev-feed))

;; ----------------------------------------------------------------------
;; Auto-install pager advice on load
;; ----------------------------------------------------------------------

(advice-add 'newsticker--buffer-insert-all-items :around
            #'newst-jsonrpc-advice-insert-all)
(advice-add 'newsticker-next-item :around
            #'newst-jsonrpc-advice-next-item)
(advice-add 'newsticker-previous-item :around
            #'newst-jsonrpc-advice-prev-item)
(advice-add 'newsticker-next-feed :around
            #'newst-jsonrpc-advice-next-feed)
(advice-add 'newsticker-previous-feed :around
            #'newst-jsonrpc-advice-prev-feed)

(provide 'newst-jsonrpc)
