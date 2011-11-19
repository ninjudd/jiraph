(ns jiraph.stm-layer
  (:refer-clojure :exclude [meta])
  (:use [jiraph.layer     :only [Enumerate Basic Layer Optimized Meta meta-key?
                                 ChangeLog skip-applied-revs max-revision get-revisions close]]
        [jiraph.graph     :only [*skip-writes*]]
        [retro.core       :only [WrappedTransactional Revisioned
                                 get-queue at-revision current-revision empty-queue]]
        [useful.fn        :only [given fix]]
        [useful.utils     :only [returning or-min]]
        [useful.map       :only [keyed]]
        [useful.datatypes :only [assoc-record]])
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

(def empty-store (sorted-map-by > 0 {}))

(defn storage-area [k]
  (if (vector? k), :meta, :nodes))

(defn storage-name [k]
  (fix k vector? first))

(defn current-rev [layer]
  (let [store (-> layer :store deref)
        rev (:revision layer)]
    (if (and rev (contains? store rev))
      rev
      (first (keys store)))))

(defn now [layer]
  (-> layer :store deref (get (current-rev layer))))

(def ^{:private true} nodes (comp :nodes now))
(def ^{:private true} meta (comp :meta now))

(defrecord STMLayer [store revision filename]
  Object
  (toString [this]
    (pr-str this))

  Enumerate
  (node-id-seq [this]
    (-> this nodes keys))
  (node-seq [this]
    (-> this nodes seq))

  ;; the STM layer can't optimize any of these things; these are simply
  ;; reference/testing implementations
  Optimized
  (query-fn [this keyseq f]
    (when (= 'specialized-count f)
      (fn [update-counter]
        (do (swap! update-counter inc)
            (count (get-in (nodes this) keyseq))))))
  (update-fn [this [id key :as keyseq] f]
    (when (= :edges key)
      (fn [& args]
        (let [old (get-in (nodes this) keyseq)
              new (apply f old args)]
          (alter store
                 assoc-in (list* (current-rev this) :nodes keyseq)
                 new)
          (keyed [old new])))))

  Meta
  (meta-key [this k]
    [k])
  (meta-key? [this k]
    (vector? k))

  Basic
  (get-node [this k not-found]
    (if (meta-key? this k)
      (-> this meta ? (get (? (first (? k))) not-found) ?)
      (let [n (-> this nodes (get k not-found))]
        (if-not (identical? n not-found)
          n
          (let [touched-revisions (? (get-revisions (at-revision (? this) nil) (? k)))
                most-recent (or (first (if revision
                                         (drop-while #(> % revision) touched-revisions)
                                         touched-revisions))
                                0)]
            (-> @store (get-in [most-recent :nodes k] not-found)))))))
  (assoc-node! [this k v]
    (alter store
           assoc-in [(current-rev this) (storage-area k) (storage-name k)] v))
  (dissoc-node! [this k]
    (alter store
           update-in [(current-rev this) (storage-area k)] dissoc (storage-name k)))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision)

  WrappedTransactional
  (txn-wrap [_ f]
    (fn [^STMLayer layer]
      (dosync
       (let [rev (.revision layer)
             max-rev (max-revision layer)]
         (cond (nil? rev) (f layer)
               (< rev max-rev) (binding [*skip-writes* true] (f (empty-queue layer)))
               :else
               (let [store (.store layer)
                     prev (? (get @store (? max-rev)))]
                 (?
                  (alter store fix
                         #(not (contains? % rev))
                         #(assoc % rev prev)))
                 (returning (f (at-revision layer rev))
                   (alter store fix
                          #(identical? prev (get % rev))
                          #(dissoc % rev)))))))))

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
    (dosync ;; since this should only be called outside a retro transaction
     (ref-set (:store this) empty-store))))

(defn make
  ([] (make nil))
  ([filename] (STMLayer. (ref empty-store) nil filename)))
