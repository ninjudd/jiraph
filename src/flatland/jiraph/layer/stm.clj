(ns flatland.jiraph.layer.stm
  (:refer-clojure :exclude [meta])
  (:use [flatland.jiraph.layer :as layer
         :only [Enumerate EnumerateIds Basic Layer Optimized ChangeLog]]
        [flatland.jiraph.graph :as graph :only [with-action get-node]]
        [flatland.jiraph.utils :only [meta-id meta-id? base-id]]
        [flatland.retro.core :only [WrappedTransactional Revisioned OrderedRevisions
                                    max-revision at-revision current-revision]]
        [flatland.useful.fn :only [given fix]]
        [flatland.useful.seq :only [assert-length]]
        [flatland.useful.utils :only [returning or-min]]
        [flatland.useful.datatypes :only [assoc-record]])
  (:import (java.io FileNotFoundException)))

(comment
  The STM layer stores a single ref, pointing to a series of whole-layer
  snapshots over time, one per committed revision. Each snapshot contains
  a :nodes section and a :meta section - the meta is managed entirely by
  flatland.jiraph.graph. Note that while metadata is revisioned, it does not keep a
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
  (let [store @(:store layer)]
    (ffirst (if-let [rev (:revision layer)]
              (subseq store >= rev)
              store))))

(defn now [layer]
  (-> layer :store deref (get (current-rev layer))))

(def ^{:private true} nodes (comp :nodes now))
(def ^{:private true} meta  (comp :meta now))

(defrecord STMLayer [store revision filename]
  Object
  (toString [this]
    (pr-str this))

  Enumerate
  (node-seq [this opts]
    (layer/deny-sorted-seq opts)
    (-> this nodes seq))
  
  EnumerateIds
  (node-id-seq [this opts]
    (layer/deny-sorted-seq opts)
    (-> this nodes keys))

  Basic
  (get-node [this k not-found]
    (if (meta-id? k)
      (-> this meta (get (base-id k) not-found))
      (let [n (-> this nodes (get k not-found))]
        (if-not (identical? n not-found)
          n
          (let [touched-revisions (layer/get-revisions (at-revision this nil) k)
                most-recent (or (first (if revision
                                         (drop-while #(> % revision) touched-revisions)
                                         touched-revisions))
                                0)]
            (-> @store (get-in [most-recent :nodes k] not-found)))))))
  (update-in-node [this keyseq f args]
    (let [ioval (graph/simple-ioval this keyseq f args)]
      (if (empty? keyseq)
        (condp = f
          assoc (let [[id attrs] (assert-length 2 args)]
                  (recur [id] (constantly attrs) nil))
          dissoc (let [[id] (assert-length 1 args)]
                   (ioval (fn [layer]
                            (alter store
                                   update-in [(current-rev layer) (storage-area id)]
                                   dissoc (storage-name id))))))
        (let [[id & keys] keyseq]
          (ioval (fn [layer]
                   (apply alter store
                          update-in (list* (current-rev layer) (storage-area id)
                                           (storage-name id) keys)
                          f args)))))))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision)

  OrderedRevisions
  (max-revision [this]
    (-> @store ))
  (touch [this]
    nil) ;; hopefully that works?

  WrappedTransactional
  (txn-wrap [_ f]
    (fn []
      (dosync (f))))

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
    (layer/close this))
  (truncate! [this]
    (dosync
     (ref-set store empty-store)))
  (optimize! [this] nil))

(defn make
  ([] (make nil))
  ([filename] (STMLayer. (ref empty-store) nil filename)))
