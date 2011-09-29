(ns jiraph.graph
  (:use [useful.map :only [into-map update filter-keys-by-val remove-vals map-to]]
        [useful.utils :only [memoize-deref adjoin]]
        [useful.fn :only [any]]
        [useful.macro :only [with-altered-var]]
        [clojure.string :only [split join]]
        [ego.core :only [type-key]]
        [jiraph.wrapper :only [*read-wrappers* *write-wrappers*]]
        [useful.experimental :only [wrap-multiple]])
  (:require [jiraph.layer :as layer]
            [retro.core :as retro]))

(def ^{:dynamic true} *read-only* false)

(defn layer-meta
  "Fetch a metadata key from a layer."
  [layer key]
  (key (meta layer)))

(letfn [(meta-accessor [key]
          (fn [layer]
            (layer-meta layer key)))]
  (def single-edge? (meta-accessor :single-edge))
  (def append-only? (meta-accessor :append-only)))

(defn edge-key [layer]
  (if (single-edge? layer)
    :edge, :edges))

(defn edges
  "Gets edges from a node. Returns all edges, including deleted ones."
  [node]
  (if-let [edge (:edge node)]
    (when-let [id (:id edge)]
      {id edge})
    (:edges node)))

(defn edges-valid? [layer node]
  (not ((if (single-edge? layer)
          :edges, :edge) ;; NB opposite of usual edge-key
        node)))

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

(defmacro at-revision
  "Execute the given forms with the graph at revision rev. Can be used in to mark changes with a given
   revision, or rewind the state of the graph to a given revision."
  [rev & forms]
  `(retro/at-revision ~rev ~@forms))

(defn read-only? []
  *read-only*)

(defmacro with-readonly [& body]
  `(binding [*read-only* true]
     ~@body))

(defn- refuse-readonly []
  (when (read-only?)
    (throw (IllegalStateException. "Can't write in read-only mode"))))

(defmacro with-transaction
  "Execute forms within a transaction on the specified layers."
  [layers & forms]
  `(if (read-only?)
     (do ~@forms)
     ((reduce
       retro/wrap-transaction
       (fn [] ~@forms)
       ~layers))))

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
  (refuse-readonly)
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
  (refuse-readonly)
  (layer/assoc-layer-meta! layer key val))

(defn update-property!
  "Update the given layer property by calling function f with the old value and any supplied args."
  [layer key f & args]
  (refuse-readonly)
  (let [val (get-property layer key)]
    (set-property! layer key (apply f val args))))

(defn get-node
  "Fetch a node's data from this layer."
  ([layer id & [not-found]]
     (layer/get-node layer id not-found)))

(defn get-edges
  "Fetch the edges for a node on this layer."
  [layer id]
  (let [single? (single-edge? layer)
        key (if single? :edge :edges)
        data (layer/get-in-node layer [key] nil)]
    (if single?
      (when-let [id (:id data)]
        {id data})
      data)))

(defn get-in-node
  "Fetch data from inside a node."
  ([layer keyseq & [not-found]]
     (layer/get-in-node layer keyseq not-found)))

(defn get-in-edge
  "Fetch data from inside an edge."
  ([layer [id to-id & keys] & [not-found]]
     (get-in-node layer (list* id (edge-key layer) to-id keys) not-found)))

(defn get-edge
  "Fetch an edge from node with id to to-id."
  ([layer id to-id & [not-found]]
     (get-in-edge layer [id to-id] not-found)))

(defn node-exists?
  "Check if a node exists on this layer."
  [layer id]
  (layer/node-exists? layer id))

(def ^{:dynamic true :private true} *compacting* false)

(defn update-node!
  "Update a node by calling function f with the old value and any supplied args."
  [layer-name id f & args]
  {:pre [(or (not (append-only? (layer layer-name))) *compacting*)]}
  (refuse-readonly)
  (with-transaction layer-name
    (let [layer (layer layer-name)
          old   (layer/get-node layer id)
          new   (layer/set-node! layer id (apply f old args))]
      (assert (types-valid? layer-name id new))
      (assert (edges-valid? layer-name new))
      (let [new-edges (set (filter-edge-ids (complement :deleted) new))
            old-edges (set (keys (edges old)))]
        (doseq [to-id (remove old-edges new-edges)]
          (layer/add-incoming! layer to-id id))
        (doseq [to-id (remove new-edges old-edges)]
          (layer/drop-incoming! layer to-id id)))
      [(dissoc old :id) new])))

(defn append-node!
  "Append attrs to a node or create it if it doesn't exist. Note: some layers may not implement this."
  [layer-name id & attrs]
  {:pre [(if (append-only? layer-name) retro/*revision* true)]}
  (refuse-readonly)
  (let [node (into-map attrs)
        layer (layer layer-name)]
    (assert (types-valid? layer-name id node))
    (assert (edges-valid? layer-name node))
    (if (instance? jiraph.layer.Append layer)
      (with-transaction layer-name
        (let [node (layer/append-node! layer id node)]
          (doseq [[to-id edge] (edges node)]
            (if (:deleted edge)
              (layer/drop-incoming! layer to-id id)
              (layer/add-incoming!  layer to-id id)))
          node))
      (do (apply update-node! layer-name id adjoin attrs)
          (layer/make-node node)))))

(defn append-edge!
  [layer-name id to-id & attrs]
  (append-node! layer-name id
                (if (single-edge? layer-name)
                  {:edge (into-map :id to-id attrs)}
                  {:edges {to-id (into-map attrs)}})))

(defn delete-node!
  "Remove a node from a layer (incoming links remain)."
  [layer-name id]
  (refuse-readonly)
  (with-transaction layer-name
    (let [layer (layer layer-name)
          node  (layer/delete-node! layer id)]
      (doseq [[to-id edge] (edges node)]
        (if-not (:deleted edge)
          (layer/drop-incoming! layer to-id id))))))

(defn assoc-node!
  "Associate attrs with a node."
  [layer-name id & attrs]
  (refuse-readonly)
  (let [layer (layer layer-name)]
    (if (instance? jiraph.layer.Assoc layer)
      (layer/assoc-node! layer id (into-map attrs))
      (apply update-node! layer-name id merge attrs))))

(defn compact-node!
  "Compact a node by removing deleted edges. This will also collapse appended revisions."
  [layer-name id]
  (refuse-readonly)
  (binding [*compacting* true]
    (apply update-node! layer-name id update
           (if (single-edge? layer-name)
             [:edge #(when-not (:deleted %) %)]
             [:edges remove-vals :deleted]))))

(defn fields
  "Return a map of fields to their metadata for the given layer."
  ([layer-name]
     (layer/fields (layer layer-name)))
  ([layer-name subfields]
     (layer/fields (layer layer-name) subfields)))

(defn node-valid?
  "Check if the given node is valid for the specified layer."
  [layer-name id & attrs]
  (let [attrs (into-map attrs)]
    (and (or (nil? id) (types-valid? layer-name id attrs))
         (edges-valid? layer-name attrs)
         (layer/node-valid? (layer layer-name) attrs))))

(defn verify-node
  "Assert that the given node is valid for the specified layer."
  [layer-name id & attrs]
  (let [attrs (into-map attrs)]
    (when id
      (assert (types-valid? layer-name id attrs)))
    (assert (edges-valid? layer-name attrs))
    (assert (layer/node-valid? (layer layer-name) attrs))))

(defn layers
  "Return the names of all layers in the current graph."
  ([] (keys *graph*))
  ([type]
     (for [[name layer] *graph*
           :let [meta (meta layer)]
           :when (and (contains? (:types meta) type)
                      (not (:hidden meta)))]
       name)))

(defn schema
  "Return a map of fields for a given type to the metadata for each layer. If a subfield is
  provided, then the schema returned is for the nested type within that subfield."
  ([type]
     (apply merge-with conj
            (for [layer        (layers type)
                  [field meta] (fields layer)]
              {field {layer meta}})))
  ([type subfield]
     (apply merge-with conj
            (for [layer        (keys (get (schema type) subfield))
                  [field meta] (fields layer [subfield])]
              {field {layer meta}}))))

(alter-var-root #'schema #(with-meta (memoize-deref [#'jiraph.graph/*graph*] %) (meta %)))

(defn layer-exists?
  "Does the named layer exist in the current graph?"
  [layer-name]
  (contains? *graph* layer-name))

(defn get-all-revisions
  "Return a seq of all revisions that have ever modified this node on this layer, even if the data has been
   subsequently compacted."
  [layer-name id]
  (filter pos? (layer/get-revisions (layer layer-name) id)))

(defn get-revisions
  "Return a seq of all revisions with data for this node."
  [layer-name id]
  (reverse
   (take-while pos? (reverse (layer/get-revisions (layer layer-name) id)))))

(defn get-incoming
  "Return the ids of all nodes that have incoming edges on this layer to this node (excludes edges marked :deleted)."
  [layer-name id]
  (layer/get-incoming (layer layer-name) id))

(defn wrap-caching
  "Wrap the given function with a new function that memoizes read methods. Nested wrap-caching calls
   are collapsed so only the outer cache is used."
  [f]
  (let [vars [#'jiraph.graph/*graph* #'retro/*revision*]]
    (fn []
      (if *use-outer-cache*
        (f)
        (binding [*use-outer-cache* true
                  get-node          (memoize-deref vars get-node)
                  get-incoming      (memoize-deref vars get-incoming)
                  get-revisions     (memoize-deref vars get-revisions)
                  get-all-revisions (memoize-deref vars get-all-revisions)]
          (f))))))

(defmacro with-caching
  "Enable caching for the given forms. See wrap-caching."
  [& forms]
  `((wrap-caching (fn [] ~@forms))))

(defn wrap-bindings
  "Wrap the given function with the current graph context."
  [f]
  (bound-fn ([& args] (apply f args))))

(wrap-multiple #'*read-wrappers*
               node-ids node-count get-property current-revision
               get-node node-exists? get-incoming)
(wrap-multiple #'*write-wrappers*
               update-node! optimize! truncate! set-property! update-property!
               sync! add-node! append-node! assoc-node! compact-node! delete-node!)
