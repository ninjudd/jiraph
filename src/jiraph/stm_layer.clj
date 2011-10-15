(ns jiraph.stm-layer
  (:refer-clojure :exclude [meta])
  (:use [jiraph.layer :only [Enumerate Counted Optimized Basic Layer LayerMeta
                             ChangeLog skip-applied-revs max-revision get-revisions close]]
        [retro.core   :only [WrappedTransactional TransactionHooks Revisioned
                             get-queue at-revision current-revision]]
        [useful.fn    :only [given]])
  (:import (java.io FileNotFoundException)))

(comment
  The STM layer stores a single ref, pointing to a series of whole-layer
  snapshots over time, one per committed revision. Each snapshot contains
  a :nodes section and a :meta section - the meta is managed entirely by
  jiraph.graph. Note that while metadata is revisioned, it does not keep a
  changelog like nodes do, so you can't meaningfully read metadata from a
  revision in the future (which you can do for nodes).

  The layer also tracks a current revision, to allow any snapshot to be
  viewed as if it were the full graph.

  So a sample STM layer might look like the following.

  {:revision 2
   :store (ref {0 {}
                1 {:meta {"some-key" "some-value"}
                   :nodes {"profile-4" {:name "Rita" :edges {"profile-9" {:rel :child}}}}}
                2 {:meta {"some-key" "other-value"}
                   :nodes {"profile-4" {:name "Rita" :edges {"profile-9" {:rel :child}}
                                        :age 39}
                           "profile-9" {:name "William"}}}})})

(def empty-store (sorted-map-by >))

(defn current-rev [layer]
  (or (:revision layer) ;; revision explicitly set
      (first (keys (-> layer :store deref))))) ;;  get most recent revision

(defn now [layer]
  (-> layer :store deref (get (current-rev layer))))

(def ^{:private true} nodes (comp :nodes now))
(def ^{:private true} meta (comp :meta now))

(defrecord STMLayer [store revision filename]
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
  (open [this]
    (when filename
      (dosync
       (try
         (doto this
           (-> :store (ref-set (read-string (slurp filename)))))
         (catch FileNotFoundException e this)))))
  (close [this]
    (when filename
      (spit filename @store)))
  (sync! [this]
    (close this))
  (optimize! [this] nil)
  (truncate! [this]
    (ref-set (:store this) empty-store))
  (node-exists? [this id]
    (-> this nodes (get id) boolean)))

(defn make
  ([] (make nil))
  ([filename] (STMLayer. (ref {0 {}}) nil filename)))
