(ns jiraph.graph
  (:use [useful :only [into-map conj-vec update filter-keys-by-val remove-vals any memoize-deref adjoin]]
        [clojure.string :only [split]])
  (:require [jiraph.layer :as layer]
            [retro.core :as retro]
            [masai.tokyo :as tokyo]
            [jiraph.masai-layer :as masai-layer]))

(def ^{:dynamic true} *graph* nil)
(def ^{:dynamic true} *verbose* nil)
(def ^{:dynamic true} *use-outer-cache* nil)

(defn- split-id [s] (split s #"-"))

(defn layer
  "Return the layer for a given name from *graph*."
  [layer-name]
  (if *graph*
    (or (get *graph* layer-name)
        (throw (java.io.IOException. (format "cannot find layer %s in open graph" layer-name))))
    (throw (java.io.IOException. (format "attempt to use a layer without an open graph")))))

(defn layer-meta
  "Fetch a metadata key from a layer."
  [layer-name key]
  (key (meta (layer layer-name))))

(defn edges
  "Gets edges from a node. Returns all edges, including deleted ones."
  [node]
  (if-let [edge (:edge node)]
    {(:id edge) edge}
    (:edges node)))

(defn edges-valid? [layer-name node]
  (not (if (layer-meta layer-name :single-edge)
         (:edges node)
         (:edge node))))

(defn- type-valid? [types id]
  (or (empty? types)
      (some (partial = (first (split-id id)))
            types)))

(defn- schema-valid? [layer-name id node]
  (and (type-valid? (layer-meta layer-name :types) id)
       (every? true?
               (map (partial type-valid? (layer-meta layer-name :edge-types))
                    (keys (edges node))))))

(defn filter-edge-ids [pred node]
  (filter-keys-by-val pred (edges node)))

(defn filter-edges [pred node]
  (select-keys (edges node) (filter-edge-ids pred node)))

(defmacro with-each-layer
  "Execute forms with layer bound to each layer specified or all layers if layers is empty."
  [layers & forms]
  `(doseq [[~'layer-name ~'layer] (cond (keyword? ~layers) [~layers (layer ~layers)]
                                        (empty?   ~layers) *graph*
                                        :else              (select-keys *graph* ~layers))]
     (when *verbose*
       (println (format "%-20s %s"~'layer-name (apply str (map pr-str '~forms)))))
     ~@forms))

(defn open! []
  (with-each-layer []
    (layer/open layer)))

(defn close! []
  (with-each-layer []
    (layer/close layer)))

(defn set-graph! [graph]
  (alter-var-root #'*graph* (fn [_] graph)))

(defmacro with-graph [graph & forms]
  `(binding [*graph* ~graph]
     (try (open!)
          ~@forms
          (finally (close!)))))

(defmacro with-graph! [graph & forms]
  `(let [graph# *graph*]
     (set-graph! ~graph)
     (try (open!)
          ~@forms
          (finally (close!)
                   (set-graph! graph#)))))

(defmacro at-revision
  "Execute the given forms with the graph at revision rev. Can be used in to mark changes with a given
   revision, or rewind the state of the graph to a given revision."
  [rev & forms]
  `(retro/at-revision ~rev ~@forms))

(defmacro with-transaction
  "Execute forms within a transaction on the named layer/layers."
  [layers & forms]
  `((reduce
     retro/wrap-transaction
     (fn [] ~@forms)
     (cond (keyword? ~layers) [(layer ~layers)]
           (empty?   ~layers) (vals *graph*)
           :else              (map layer ~layers)))))

(def abort-transaction retro/abort-transaction)

(defn sync!
  "Flush changes for the specified layers to the storage medium, or all layers if none are specified."
  [& layers]
  (with-each-layer layers
    (layer/sync! layer)))

(defn optimize!
  "Optimize the underlying storage for the specified layers, or all layers if none are specified."
  [& layers]
  (with-each-layer layers
    (layer/optimize! layer)))

(defn truncate!
  "Remove all nodes from the specified layers, or all layers if none are specified."
  [& layers]
  (with-each-layer layers
    (layer/truncate! layer)))

(defn node-ids
  "Return a lazy sequence of all node ids in this layer."
  [layer-name]
  (layer/node-ids (layer layer-name)))

(defn node-count
  "Return the total number of nodes in this layer."
  [layer-name]
  (layer/node-count (layer layer-name)))

(defn get-property
  "Fetch a layer-wide property."
  [layer-name key]
  (layer/get-property (layer layer-name) key))

(defn set-property!
  "Store a layer-wide property."
  [layer-name key val]
  (layer/set-property! (layer layer-name) key val))

(defn update-property!
  "Update the given layer property by calling function f with the old value and any supplied args."
  [layer-name key f & args]
  (let [val (get-property layer-name key)]
    (set-property! layer-name key (apply f val args))))

(defn current-revision
  "The maximum revision on all specified layers, or all layers if none are specified."
  [& layers]
  (apply max 0 (for [layer (if (empty? layers) (keys *graph*) layers)]
                 (or (get-property layer :rev) 0))))

(defn get-node
  "Fetch a node's data from this layer."
  [layer-name id]
  (when-let [node (layer/get-node (layer layer-name) id)]
    (assoc node :id id)))

(defn get-in-node
  "Fetch data from inside a node."
  [layer-name keys]
  (get-in (get-node layer-name (first keys)) (rest keys)))

(defn get-edge
  "Fetch an edge from node with id to to-id."
  [layer-name id to-id]
  ((edges (get-node layer-name id)) to-id))

(defn node-exists?
  "Check if a node exists on this layer."
  [layer-name id]
  (layer/node-exists? (layer layer-name) id))

(defn layers-with-type
  "Get a list of layers whose :types metadata contains type."
  [type]
  (for [[name layer] *graph* :when (some #{type} (layer-meta name :types))]
    name))

(defn add-node!
  "Add a node with the given id and attrs if it doesn't already exist."
  [layer-name id & attrs]
  {:pre [(if (layer-meta layer-name :append-only) retro/*revision* true)]}
  (let [attrs (into-map attrs)]
    (assert (schema-valid? layer-name id attrs))
    (assert (edges-valid? layer-name attrs))
    (with-transaction layer-name
      (let [layer (layer layer-name)
            node  (layer/add-node! layer id attrs)]
        (when-not node
          (throw (java.io.IOException. (format "cannot add node %s because it already exists" id))))
        (doseq [[to-id edge] (edges node)]
          (when-not (:deleted edge)
            (layer/add-incoming! layer to-id id)))
        node))))

(def ^{:dynamic true :private true} *compacting* false)

(defn update-node!
  "Update a node by calling function f with the old value and any supplied args."
  [layer-name id f & args]
  {:pre [(or (not (layer-meta layer-name :append-only)) *compacting*)]}
  (with-transaction layer-name
    (let [layer (layer layer-name)
          old   (layer/get-node layer id)
          new   (layer/set-node! layer id (apply f old args))]
      (assert (schema-valid? layer-name id new))
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
  {:pre [(if (layer-meta layer-name :append-only) retro/*revision* true)]}
  (let [node (into-map attrs)
        layer (layer layer-name)]
    (assert (schema-valid? layer-name id node))
    (assert (edges-valid? layer-name node))
    (if (instance? jiraph.layer.Append layer)
      (with-transaction layer-name
        (let [node (layer/append-node! layer id node)]
          (doseq [[to-id edge] (:edges node)]
            (if (:deleted edge)
              (layer/drop-incoming! layer to-id id)
              (layer/add-incoming!  layer to-id id)))
          node))
      (do (apply update-node! layer-name id adjoin attrs)
          (layer/make-node node)))))

(defn delete-node!
  "Remove a node from a layer (incoming links remain)."
  [layer-name id]
  (with-transaction layer-name
    (let [layer (layer layer-name)
          node  (layer/delete-node! layer id)]
      (doseq [[to-id edge] (edges node)]
        (if-not (:deleted edge)
          (layer/drop-incoming! layer to-id id))))))

(defn assoc-node!
  "Associate attrs with a node."
  [layer-name id & attrs]
  (let [layer (layer layer-name)]
    (if (instance? jiraph.layer.Assoc layer)
      (layer/assoc-node! layer id (into-map attrs))
      (apply update-node! layer-name id merge attrs))))

(defn compact-node!
  "Compact a node by removing deleted edges. This will also collapse appended revisions."
  [layer-name id]
  (binding [*compacting* true]
    (if (layer-meta layer-name :single-edge)
      (update-node! layer-name id update :edge #(when-not (:deleted %) %))
      (update-node! layer-name id update :edges (partial remove-vals :deleted)))))

(defn schema
  "Get the schema for layers that contain type."
  [type]
  (into {}
        (map (fn [layer-name]
               [layer-name (into {} (layer/schema (layer layer-name)))])
             (layers-with-type type))))

(defn layers
  "Return the names of all layers in the current graph."
  []
  (keys *graph*))

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

(defn fields-to-layers
  "Return a mapping from field to layers for all the layers provided. Fields can appear in more than one layer."
  [graph layers]
  (reduce (fn [m layer]
            (reduce #(update %1 %2 conj-vec layer)
                    m (layer/fields (graph layer))))
          {} layers))

(defn make-layer [path]
  (masai-layer/make (tokyo/make {:path path :create true})))

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
  (useful/wrap-bindings [#'get-node #'get-incoming #'get-revisions #'get-all-revisions
                         #'*graph* #'*verbose* #'*use-outer-cache* #'retro/*revision*] f))
