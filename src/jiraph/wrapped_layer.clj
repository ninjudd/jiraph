(ns jiraph.wrapped-layer
  (:use jiraph.layer retro.core
        [useful.map :only [merge-in]]
        [useful.datatypes :only [assoc-record]]
        [useful.experimental.delegate :only [parse-deftype-specs emit-deftype-specs]]))

(defn wrap-forwarded-reads [ioval master slave]
  (fn [read]
    (update-in (ioval read) [0 :wrap-read]
               (fn [wrapper]
                 (fn [read]
                   (let [read (wrapper read)]
                     (fn [layer keyseq]
                       (read (if (same? master layer)
                               slave, layer)
                             keyseq))))))))

(defprotocol Wrapped
  "For layers which provide additional functionality by wrapping other layers."
  (unwrap [layer]
    "Provide access to the underlying layer."))

(defprotocol Parent
  (children [layer]
    "Return the names of all the children that this layer has, ie those for
    which (child l name) would return non-nil.")
  (child [layer name]
    "Find a layer which is related in some way to this one. For example, pass :incoming to get the
    layer (if any) on which incoming edges from this layer are stored."))

(defprotocol NodeFilter
  "Implement this to get automatic filtering of nodes and ids in node-[id-]seq."
  (keep-node? [layer id]))

(extend-type Object
  Parent
  (children [this]
    nil)
  (child [this name]
    nil))

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
       (node-id-seq [this#] (filter #(keep-node? this# %)
                                    (node-id-seq ~layer-sym)))
       (node-seq    [this#] (filter #(keep-node? this# (first %))
                                    (node-seq ~layer-sym)))

       Incoming
       (get-incoming [this# id#] (get-incoming ~layer-sym id#))

       SortedEnumerate
       (node-id-subseq [this# cmp# start#] (filter #(keep-node? this# %)
                                                   (node-id-subseq ~layer-sym cmp# start#)))
       (node-subseq    [this# cmp# start#] (filter #(keep-node? this# (first %))
                                                   (node-subseq ~layer-sym cmp# start#)))

       Basic
       (get-node [this# id# not-found#] (get-node ~layer-sym id# not-found#))
       (update-in-node [this# keyseq# f# args#]
                       (-> (update-in-node ~layer-sym keyseq# f# args#)
                           (wrap-forwarded-reads this# ~layer-sym)))

       Optimized
       (query-fn  [this# keyseq# not-found# f#] (query-fn ~layer-sym keyseq# not-found# f#))

       Layer
       (open       [this#] (open  ~layer-sym))
       (close      [this#] (close ~layer-sym))
       (sync!      [this#] (sync! ~layer-sym))
       (optimize!  [this#] (optimize! ~layer-sym))
       (truncate!  [this#] (truncate! ~layer-sym))
       (same? [this# other#]
         (jiraph.graph/same? ~layer-sym (~layer-key other#)))

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
