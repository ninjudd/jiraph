(ns flatland.jiraph.wrapped-layer
  (:use flatland.jiraph.layer flatland.retro.core
        [flatland.useful.utils :only [update-peek]]
        [flatland.useful.map :only [merge-in update]]
        [flatland.useful.datatypes :only [assoc-record]]
        [flatland.useful.experimental.delegate :only [parse-deftype-specs emit-deftype-specs]]))

(defn update-wrap-read [ioval f & args]
  (fn [read]
    (let [actions (ioval read)]
      (update-peek actions update :wrap-read
                   (fn [wrapper]
                     (fn [read]
                       (let [read (wrapper read)]
                         (apply f read args))))))))

(defn forward-reads [read master slave]
  (fn [layer keyseq & [not-found]]
    (read (if (same? master layer)
            slave, layer)
          keyseq, not-found)))

(defn conditional-read-wrapper [pred? wrapper]
  (fn [read & args]
    (fn [layer keyseq & [not-found]]
      ((if (apply pred? layer args)
         (wrapper read)
         read)
       layer keyseq not-found))))

; (read-fixer (sublayer-pred sublayer-type get-sublayer sublayer) wrapper)

(defn match-sublayer [sublayer-type get-sublayer]
  (fn [layer sublayer]
    (and (instance? sublayer-type layer)
         (same? (get-sublayer layer) sublayer))))

(defprotocol Wrapped
  "For layers which provide additional functionality by wrapping other layers."
  (unwrap [layer]
    "Provide access to the underlying layer."))

(defprotocol NodeFilter
  "Implement this to get automatic filtering of nodes and ids in node-[id-]seq."
  (keep-node? [layer id]))

(defn default-specs [layer-sym]
  (let [layer-key (keyword layer-sym)]
    (parse-deftype-specs
     `(Object
       (toString [this#] (pr-str this#))

       NodeFilter
       (keep-node? [this# id#] true)

       Wrapped
       (unwrap [this#] ~layer-sym)

       Parent
       (children [this#] (children ~layer-sym))
       (child    [this# kind#] (child ~layer-sym kind#))

       Enumerate
       (node-seq [this# opts#]
                 (filter #(keep-node? this# (first %))
                         (node-seq ~layer-sym opts#)))

       EnumerateIds
       (node-id-seq [this# opts#]
                    (filter #(keep-node? this# %)
                            (node-id-seq ~layer-sym opts#)))

       Basic
       (get-node [this# id# not-found#] (get-node ~layer-sym id# not-found#))
       (update-in-node [this# keyseq# f# args#]
                       (-> (update-in-node ~layer-sym keyseq# f# args#)
                           (update-wrap-read forward-reads this# ~layer-sym)))

       Optimized
       (query-fn  [this# keyseq# not-found# f#] (query-fn ~layer-sym keyseq# not-found# f#))

       Layer
       (open       [this#] (open  ~layer-sym))
       (close      [this#] (close ~layer-sym))
       (sync!      [this#] (sync! ~layer-sym))
       (optimize!  [this#] (optimize! ~layer-sym))
       (truncate!  [this#] (truncate! ~layer-sym))
       (same? [this# other#]
         (flatland.jiraph.graph/same? ~layer-sym (~layer-key other#)))

       Schema
       (schema      [this# id#]        (schema ~layer-sym id#))
       (verify-node [this# id# attrs#] (verify-node ~layer-sym id# attrs#))

       ChangeLog
       (get-revisions   [this# id#]  (get-revisions ~layer-sym id#))
       (get-changed-ids [this# rev#] (get-changed-ids ~layer-sym rev#))

       Transactional
       (txn-begin!    [this#] (txn-begin! ~layer-sym))
       (txn-commit!   [this#] (txn-commit! ~layer-sym))
       (txn-rollback! [this#] (txn-rollback! ~layer-sym))

       Revisioned
       (at-revision      [this# rev#] (assoc-record this# ~layer-key (at-revision ~layer-sym rev#)))
       (current-revision [this#]      (current-revision ~layer-sym))

       OrderedRevisions
       (max-revision [this#] (max-revision ~layer-sym))
       (touch        [this#] (touch ~layer-sym))))))

(defmacro defwrapped [name [wrapped-layer-fieldname :as fields] & specs]
  `(defrecord ~name [~@fields]
     ~@(emit-deftype-specs
         (merge-in (default-specs wrapped-layer-fieldname)
                   (parse-deftype-specs specs)))))
