(ns jiraph.wrapped-layer
  (:use [useful.map :only [merge-in]]
        [useful.experimental.delegate :only [parse-deftype-specs emit-deftype-specs]]))

(def default-specs
  (parse-deftype-specs
   '(Object
     (toString [this] (pr-str this))

     Enumerate
     (node-id-seq [this] (node-id-seq layer))
     (node-seq    [this] (node-seq layer))

     Basic
     (get-node     [this id not-found] (get-node layer id not-found))
     (assoc-node!  [this id attrs]     (assoc-node! layer id attrs))
     (dissoc-node! [this id]           (dissoc-node! layer id))

     Optimized
     (query-fn  [this keyseq not-found f] (query-fn layer keyseq not-found f))
     (update-fn [this keyseq f]           (update-fn layer keyseq f))

     Layer
     (open      [this] (open  layer))
     (close     [this] (close layer))
     (sync!     [this] (sync! layer))
     (optimize! [this] (optimize! layer))
     (truncate! [this] (truncate! layer))

     Schema
     (schema      [this id]       (schema layer id))
     (verify-node [this id attrs] (verify-node layer id attrs))

     ChangeLog
     (get-revisions   [this id]  (get-revisions layer id))
     (get-changed-ids [this rev] (get-changed-ids layer rev))

     WrappedTransactional
     (txn-wrap [this f]
       (let [wrapped (txn-wrap layer ; let layer wrap transaction, but call f with this
                               (fn [_]
                                 (f this)))]
         (fn [layer]
           (wrapped (:layer layer)))))

     Revisioned
     (at-revision      [this rev] (assoc-record this :layer (at-revision layer rev)))
     (current-revision [this]     (current-revision layer))

     OrderedRevisions
     (max-revision [this] (max-revision layer))

     Preferences
     (manage-changelog? [this] (manage-changelog? layer))
     (manage-incoming?  [this] (manage-incoming?  layer))
     (single-edge?      [this] (single-edge?      layer)))))

(defmacro defwrapped [name fields & specs]
  `(defrecord ~name [~@fields]
     ~@(emit-deftype-specs
         (merge-in default-specs
                   (parse-deftype-specs specs)))))
