(ns jiraph.graph
  (:use [useful.map :only [into-map update-each filter-keys-by-val remove-vals map-to]]
        [useful.utils :only [memoize-deref adjoin into-set with-adjustments map-entry]]
        [useful.fn :only [any to-fix]]
        [useful.macro :only [with-altered-var]]
        [clojure.string :only [split join]]
        [ego.core :only [type-key]]
        [jiraph.wrapper :only [*read-wrappers* *write-wrappers*]]
        [useful.experimental :only [wrap-multiple]])
  (:require [jiraph.layer :as layer]
            [retro.core :as retro]))

(def ^{:dynamic true} *read-only* #{})

(defn layer-meta
  "Fetch a metadata key from a layer."
  [layer key]
  (key (meta layer)))

(defn edges
  "Gets edges from a node. Returns all edges, including deleted ones."
  [node]
  (:edges node))

(defn edges-valid? [layer edges]
  (or (not (layer/single-edge? layer))
      (> 2 (count edges))))

(defn types-valid? [layer id node]
  (let [types (layer-meta layer :types)]
    (or (not types)
        (let [node-type (type-key id)]
          (and (contains? types node-type)
               (every? (partial contains? (types node-type))
                       (map type-key (keys (edges node)))))))))

(defn filter-edge-ids [pred node]
  (filter-keys-by-val pred (edges node)))

(defn filter-edges [pred node]
  (select-keys (edges node) (filter-edge-ids pred node)))

(defn read-only? [layer]
  (*read-only* layer))

(defmacro with-readonly [layers & body]
  `(binding [*read-only* (into *read-only* layers)]
     ~@body))

(defn- refuse-readonly [layers]
  (doseq [layer layers]
    (if (read-only? layer)
      (throw (IllegalStateException.
              (format "Can't write to %s in read-only mode" layer)))
      (retro/modify! layer))))

(defmacro with-transaction
  "Execute forms within a transaction on the specified layers."
  [layers & forms]
  `((reduce ;; TODO is this right? Or do we need transactions for read-only mode?
     retro/wrap-transaction
     (fn [] ~@forms)
     (remove read-only? ~layers))))

(def abort-transaction retro/abort-transaction)

(defn sync!
  "Flush changes for the specified layers to the storage medium."
  [& layers]
  (doseq [layer layers]
    (layer/sync! layer)))

(defn optimize!
  "Optimize the underlying storage for the specified layers."
  [& layers]
  (doseq [layer layers]
    (layer/optimize! layer)))

(defn truncate!
  "Remove all nodes from the specified layers."
  [& layers]
  (refuse-readonly layers)
  (doseq [layer layers]
    (layer/truncate! layer)))

(defn node-id-seq
  "Return a lazy sequence of all node ids in this layer."
  [layer]
  (layer/node-id-seq layer))

(defn node-count
  "Return the total number of nodes in this layer."
  [layer]
  (layer/node-count layer))

(defn get-property
  "Fetch a layer-wide property."
  [layer key]
  (layer/get-layer-meta layer key))

(defn set-property!
  "Store a layer-wide property."
  [layer key val]
  (refuse-readonly [layer])
  (layer/assoc-layer-meta! layer key val))

(defn update-property!
  "Update the given layer property by calling function f with the old value and any supplied args."
  [layer key f & args]
  (let [val (get-property layer key)]
    (set-property! layer key (apply f val args))))

(defn get-node
  "Fetch a node's data from this layer."
  ([layer id & [not-found]]
     (layer/get-node layer id not-found)))

(let [sentinel (Object.)]
  (defn find-node
    "Get a node's data along with its id."
    ([layer id]
       (let [node (get-node layer id sentinel)]
         (when-not (identical? node sentinel)
           (map-entry id node))))))

(defn query-in-node
  "Fetch data from inside a node and immediately call a function on it."
  ([layer keyseq f & args]
     (if-let [query-fn (layer/query-fn layer keyseq f)]
       (apply query-fn args)
       (let [[id & keys] keyseq
             node (get-node layer id)]
         (apply f (get-in node keys) args)))))

(defn get-in-node
  "Fetch data from inside a node."
  ([layer [id & keys] & [not-found]]
     (query-in-node [id] get-in keys not-found)))

(defn get-edges
  "Fetch the edges for a node on this layer."
  ([layer id]
     (get-in-node layer [id :edges] nil)))

(defn get-in-edge
  "Fetch data from inside an edge."
  ([layer [id to-id & keys] & [not-found]]
     (get-in-node layer (list* id :edges to-id keys) not-found)))

(defn get-edge
  "Fetch an edge from node with id to to-id."
  ([layer id to-id & [not-found]]
     (get-in-edge layer [id to-id] not-found)))

(defn node-exists?
  "Check if a node exists on this layer."
  ([layer id]
     (layer/node-exists? layer id)))

(def ^{:dynamic true :private true} *compacting* false)

(letfn [(changed-edges [old-edges new-edges]
          (reduce (fn [edges [edge-id edge]]
                    (let [was-present (not (:deleted edge))
                          is-present  (when-let [e (get edges edge-id)] (not (:deleted e)))]
                      (if (= was-present is-present)
                        (dissoc edges edge-id)
                        (assoc-in edges [edge-id :deleted] was-present))))
                  new-edges old-edges))

        (update-incoming! [layer [id & keys] old new]
          (when (layer/manage-incoming? layer)
            (doseq [[edge-id {:keys [deleted]}]
                    (apply changed-edges
                           (map (comp :edges (to-fix keys (partial assoc-in {} keys)))
                                [old new]))]
              ((if deleted layer/drop-incoming! layer/add-incoming!)
               layer edge-id id))))

        (update-changelog! [layer node-id]
          (when (layer/manage-changelog? layer)
            (when-let [rev-id (retro/current-revision layer)]
              (set-property! layer "revision-id" rev-id)
              (when node-id
                (layer/update-meta! layer node-id "affected-by" conj [rev-id])
                (update-property! layer (str "changed-ids-" rev-id) conj node-id)))))]

  (defn update-in-node! [layer keys f & args]
    (refuse-readonly [layer])
    (with-transaction [layer]
      (let [updater (partial layer/update-fn layer)
            keys (seq keys)]
        (if-let [update! (updater keys f)]
          ;; maximally-optimized; the layer can do this exact thing well
          (let [{:keys [old new]} (apply update! args)]
            (update-incoming! layer keys old new)
            (update-changelog! layer (first keys)))
          (if-let [update! (and (seq keys) ;; don't look for assoc-in of empty keys
                                (updater (butlast keys) assoc))]
            ;; they can replace this sub-node efficiently, at least
            (let [old (get-in-node layer keys)
                  new (apply f old args)]
              (update! (last keys) new)
              (update-incoming! layer keys old new)
              (update-changelog! layer (first keys)))

            ;; need some special logic for unoptimized top-level assoc/dissoc
            (if-let [update! (and (not keys) ({assoc  layer/assoc-node!
                                               dissoc layer/dissoc-node!} f))]
              (let [[id new] args
                    old (when (layer/manage-incoming? layer)
                          (get-node layer id))]
                (apply update! layer args)
                (update-incoming! layer [id] old new)
                (update-changelog! layer id))
              (let [[id & keys] keys
                    old (get-node layer id)
                    new (if keys
                          (apply update-in old keys f args)
                          (apply f old args))]
                (layer/assoc-node! layer id new)
                (update-incoming! layer [id] old new)
                (update-changelog! layer id)))))))))

(defn update-in-node
  "Functional version of update-in-node! for use in a transaction."
  [layer keys f & args]
  (retro/enqueue layer #(apply update-in-node! % keys f args)))

(do (defn update-node!
      "Update a node by calling function f with the old value and any supplied args."
      [layer id f & args]
      (apply update-in-node! layer [id] f args))
    (defn update-node
      "Functional version of update-node! for use in a transaction."
      [layer id f & args]
      (retro/enqueue layer #(apply update-node! % id f args))))

(do (defn dissoc-node!
      "Remove a node from a layer (incoming links remain)."
      [layer id]
      (update-in-node! layer [] dissoc id))
    (defn dissoc-node
      "Functional version of update-node! for use in a transaction."
      [layer id]
      (retro/enqueue layer #(dissoc-node! % id))))

(do (defn assoc-node!
      "Create or set a node with the given id and value."
      [layer id value]
      (update-in-node! layer [] assoc id value))
    (defn assoc-node
      "Functional version of assoc-node! for use in a transaction."
      [layer id value]
      (retro/enqueue layer #(assoc-node! % id value))))

(do (defn assoc-in-node!
      "Set attributes inside of a node."
      [layer keys value]
      (update-in-node! layer (butlast keys) assoc (last keys) value))
    (defn assoc-in-node
      "Functional version of assoc-in-node! for use in a transaction."
      [layer keys value]
      (retro/enqueue layer #(assoc-in-node! % keys value))))

(defn fields
  "Return a map of fields to their metadata for the given layer."
  ([layer]
     (layer/fields layer))
  ([layer subfields]
     (layer/fields layer subfields)))

(defn node-valid?
  "Check if the given node is valid for the specified layer."
  [layer id attrs]
  (and (or (nil? id) (types-valid? layer id attrs))
       (edges-valid? layer attrs)
       (layer/node-valid? layer attrs)))

(defn verify-node
  "Assert that the given node is valid for the specified layer."
  [layer id attrs]
  (when id
    (assert (types-valid? layer id attrs)))
  (assert (edges-valid? layer attrs))
  (assert (layer/node-valid? layer attrs)))

(defn get-all-revisions
  "Return a seq of all revisions that have ever modified this node on this layer, even if the data has been
   subsequently compacted."
  [layer id]
  (filter pos? (layer/get-revisions layer id)))

(defn get-revisions
  "Return a seq of all revisions with data for this node."
  [layer id]
  (reverse
   (take-while pos? (reverse (layer/get-revisions layer id)))))

(defn get-incoming
  "Return the ids of all nodes that have incoming edges on this layer to this node (excludes edges marked :deleted)."
  [layer id]
  (layer/get-incoming layer id))

(defn wrap-bindings
  "Wrap the given function with the current graph context."
  [f]
  (bound-fn ([& args] (apply f args))))

(comment these guys need to be updated and split up once we've figured out our
         plan for wrapping, and also the split between graph and core.
  (wrap-multiple #'*read-wrappers*
                 node-id-seq node-count get-property current-revision
                 get-node node-exists? get-incoming)
  (wrap-multiple #'*write-wrappers*
                 update-node! optimize! truncate! set-property! update-property!
                 sync! add-node! append-node! assoc-node! compact-node! delete-node!))
