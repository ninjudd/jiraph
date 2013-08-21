(ns flatland.jiraph.wrapped-layer
  (:use flatland.jiraph.layer flatland.retro.core
        [flatland.useful.utils :only [update-peek]]
        [flatland.useful.map :only [merge-in update]]
        [flatland.useful.datatypes :only [assoc-record]]
        [flatland.useful.experimental.delegate :only [parse-deftype-specs emit-deftype-specs]])
  (:use flatland.useful.debug))

(defn update-wrap-read
  "Given an ioval and a read-wrapper, return a new ioval which has had the read wrapper applied to
   its wrap-read entry. Accepts extra &args like other update functions."
  [ioval wrapper & args]
  (fn [read]
    (let [actions (ioval read)]
      (update-peek actions update :wrap-read
                   #(fn [read]
                      (apply wrapper (% read) args))))))

(defn forward-reads
  "A read-wrapper (thus suitable for passing to update-wrap-read) which \"hijacks\" any reads to
   the source and passes them to the destination instead (leaving unrelated reads alone)."
  [read source destination]
  (fn [layer keyseq & [not-found]]
    (read (if (same? source layer)
            destination, layer)
          keyseq, not-found)))

(defn fix-read
  "A read-wrapper (thus suitable for passing to update-wrap-read) that only wraps reads when pred
   returns truthy for the layer being read."
  [read pred wrapper]
  (fn [layer keyseq & [not-found]]
    ((if (pred layer)
       (wrapper read)
       read)
     layer keyseq not-found)))

(defn sublayer-matcher
  "Returns a layer predicate that...
  TODO: discuss with Alan
  - shouldn't this check :layer and :merge-layer?"
  [layer-class get-sublayer sublayer]
  (fn [layer]
    (and (= layer-class (class layer))
         (same? sublayer (get-sublayer layer)))))

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
