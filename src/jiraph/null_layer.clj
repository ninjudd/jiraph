(ns jiraph.null-layer
  (:use jiraph.layer retro.core))

;; a layer object like /dev/null - it ignores all writes, and returns nil for all reads
(defrecord NullLayer []
  SortedEnumerate
  (node-id-subseq [this cmp start] ())
  (node-subseq    [this cmp start] ())

  Basic
  (get-node [this id not-found] not-found)
  (update-in-node [this keyseq f args] (constantly nil))

  Layer
  (open       [this] nil)
  (close      [this] nil)
  (sync!      [this] nil)
  (optimize!  [this] nil)
  (truncate!  [this] nil)
  (same? [this other] true)

  Schema
  (schema            [this id] nil)
  (verify-node [this id attrs] nil)

  ChangeLog
  (get-revisions   [this id] nil)
  (get-changed-ids [this rev] nil)

  Transactional
  (txn-begin!    [this] nil)
  (txn-commit!   [this] nil)
  (txn-rollback! [this] nil)

  Optimized
  (query-fn [this keyseq not-found f] (constantly not-found))

  OrderedRevisions
  (max-revision [this] Double/POSITIVE_INFINITY)
  (touch        [this] nil))

(defn make []
  (NullLayer.))
