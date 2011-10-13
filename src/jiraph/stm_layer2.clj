(ns jiraph.stm-layer2
  (:refer-clojure :exclude [meta])
  (:use [jiraph.layer :only [Enumerate Counted Optimized Basic Layer LayerMeta
                             ChangeLog skip-applied-revs max-revision]]
        [retro.core   :only [WrappedTransactional TransactionHooks Revisioned]]
        [useful.fn    :only [given]]))

(def empty-store {:revisions (sorted-map-by >)})

(defn current-rev [layer]
  (or (:revision layer)
      (max-revision layer)))

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
           assoc-in [:scratch :meta k] v))

  Basic
  (get-node [this k not-found]
    (-> this nodes (get k not-found)))
  (assoc-node! [this k v]
    (alter (:store this)
           assoc-in [:scratch :nodes k] v))
  (dissoc-node! [this k]
    (alter (:store this)
           update-in [:scratch :nodes] dissoc k))

  Revisioned
  (at-revision [this rev]
    (STMLayer. (:store this) rev))
  (current-revision [this]
    (current-rev this))

  TransactionHooks
  (before-mutate [this]
    (let [pending (skip-applied-revs this)]
      (-> this
          (skip-applied-revs)
          (given (comp seq :queue)
                 update-in [:store] alter (fn [store]
                                            (let [max-rev (max-revision this)]
                                              (assoc store (inc max-rev)
                                                     (get store max-rev))))))))

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
  (STMLayer. (ref {}) nil))