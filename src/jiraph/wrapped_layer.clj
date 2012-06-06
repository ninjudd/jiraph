(ns jiraph.wrapped-layer
  (:use jiraph.layer retro.core
        [useful.map :only [merge-in]]
        [useful.datatypes :only [assoc-record]]
        [useful.experimental.delegate :only [parse-deftype-specs emit-deftype-specs]]))

(defn default-specs [layer-sym]
  (let [layer-key (keyword layer-sym)]
    (parse-deftype-specs
     `(Object
       (toString [this#] (pr-str this#))

       Enumerate
       (node-id-seq [this#] (node-id-seq ~layer-sym))
       (node-seq    [this#] (node-seq ~layer-sym))

       Basic
       (get-node     [this# id# not-found#] (get-node ~layer-sym id# not-found#))
       (assoc-node!  [this# id# attrs#]     (assoc-node! ~layer-sym id# attrs#))
       (dissoc-node! [this# id#]            (dissoc-node! ~layer-sym id#))

       Optimized
       (query-fn  [this# keyseq# not-found# f#] (query-fn ~layer-sym keyseq# not-found# f#))
       (update-fn [this# keyseq# f#]            (update-fn ~layer-sym keyseq# f#))

       Layer
       (open      [this#] (open  ~layer-sym))
       (close     [this#] (close ~layer-sym))
       (sync!     [this#] (sync! ~layer-sym))
       (optimize! [this#] (optimize! ~layer-sym))
       (truncate! [this#] (truncate! ~layer-sym))

       Schema
       (schema      [this# id#]        (schema ~layer-sym id#))
       (verify-node [this# id# attrs#] (verify-node ~layer-sym id# attrs#))

       ChangeLog
       (get-revisions   [this# id#]  (get-revisions ~layer-sym id#))
       (get-changed-ids [this# rev#] (get-changed-ids ~layer-sym rev#))

       WrappedTransactional
       (txn-wrap [this# f#]
                 (let [wrapped# (txn-wrap ~layer-sym ; let layer wrap transaction, but call f with this#
                                          (fn [_#]
                                            (f# this#)))]
                   (fn [layer#]
                     (wrapped# (~layer-key layer#)))))

       Revisioned
       (at-revision      [this# rev#] (assoc-record this# ~layer-key (at-revision ~layer-sym rev#)))
       (current-revision [this#]      (current-revision ~layer-sym))

       OrderedRevisions
       (max-revision [this#] (max-revision ~layer-sym))

       Preferences
       (manage-changelog? [this#] (manage-changelog? ~layer-sym))
       (manage-incoming?  [this#] (manage-incoming?  ~layer-sym))
       (single-edge?      [this#] (single-edge?      ~layer-sym))))))

(defmacro defwrapped [name [wrapped-layer-fieldname :as fields] & specs]
  `(defrecord ~name [~@fields]
     ~@(emit-deftype-specs
         (merge-in (default-specs wrapped-layer-fieldname)
                   (parse-deftype-specs specs)))))
