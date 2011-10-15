(ns jiraph.stm-layer
  (:refer-clojure :exclude [meta])
  (:use [jiraph.layer :only [Enumerate Counted Optimized Basic Layer LayerMeta
                             ChangeLog skip-applied-revs max-revision get-revisions]]
        [retro.core   :only [WrappedTransactional TransactionHooks Revisioned
                             get-queue at-revision current-revision]]
        [useful.fn    :only [given]]))

(def empty-store (sorted-map-by >))

(defn current-rev [layer]
  (or (:revision layer) ;; revision explicitly set
      (first (keys (-> layer :store deref))))) ;;  get most recent revision

(defn now [layer]
  (-> layer :store deref (get (current-rev layer))))

(def ^{:private true} nodes (comp :nodes now))
(def ^{:private true} meta (comp :meta now))

(defrecord STMLayer [store revision]
  Enumerate
  (node-id-seq [this]
    (-> this nodes keys))
  (node-seq [this]
    (-> this nodes seq))

  Counted
  (node-count [this]
    (-> this nodes count))

  LayerMeta
  (get-layer-meta [this k]
    (-> this meta (get k)))
  (assoc-layer-meta! [this k v]
    (alter (:store this) ;; TODO make this work when no revision?
           assoc-in [(current-rev this) :meta k] v))

  Basic
  (get-node [this k not-found]
    (let [n (-> this nodes (get k not-found))]
      (if-not (identical? n not-found)
        n
        (let [touched-revisions (get-revisions (at-revision this nil) k)
              curr (current-revision this)
              most-recent (or (first (if curr
                                       (drop-while #(> % curr) touched-revisions)
                                       touched-revisions))
                              0)]
          (-> this :store deref (get-in [most-recent :nodes k] not-found))))))
  (assoc-node! [this k v]
    (alter (:store this)
           assoc-in [(current-rev this) :nodes k] v))
  (dissoc-node! [this k]
    (alter (:store this)
           update-in [(current-rev this) :nodes] dissoc k))

  Revisioned
  (at-revision [this rev]
    (assoc this :revision rev))
  (current-revision [this]
    (current-rev this))

  TransactionHooks
  (before-mutate [this]
    (-> this
        (skip-applied-revs)
        (given (comp seq get-queue)
               update-in [:store] #(doto %
                                     (alter (fn [store]
                                              (let [max-rev (max-revision this)]
                                                (assoc store (inc max-rev)
                                                       (get store max-rev)))))))))

  WrappedTransactional
  (txn-wrap [this f]
    #(dosync (f)))

  Layer
  (open [this] nil)
  (close [this] nil)
  (sync! [this] nil)
  (optimize! [this] nil)
  (truncate! [this]
    (ref-set (:store this) empty-store))
  (node-exists? [this id]
    (-> this nodes (get id) boolean)))

(defn make []
  (STMLayer. (ref {0 {}}) nil))
