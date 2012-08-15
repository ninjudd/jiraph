(ns jiraph.graph
  (:use [jiraph.utils :only [meta-id meta-id?]]
        [useful.map :only [filter-keys-by-val assoc-in* update-in*]]
        [useful.utils :only [memoize-deref adjoin into-set map-entry verify]]
        [useful.fn :only [fix]]
        [clojure.string :only [split join]]
        [ego.core :only [type-key]]
        (ordered [set :only [ordered-set]]
                 [map :only [ordered-map]]))
  (:require [jiraph.layer :as layer]
            [jiraph.wrapped-layer :as wrapped]
            [retro.core :as retro]))

(def ^{:dynamic true} *skip-writes* false)
(def ^{:dynamic true, :doc "All layers that are currently in read-only mode. You should probably not modify this directly; prefer using with-readonly instead."}
  *read-only* #{})

(def ^{:private true :dynamic true} *compacting* false)
(def ^{:private true :dynamic true} *use-outer-cache* nil)
(def ^{:private true}               write-count (atom 0))

(defn edges
  "Gets edges from a node. Returns all edges, including deleted ones."
  [node]
  (:edges node))

(defn filter-edge-ids [pred node]
  (filter-keys-by-val pred (edges node)))

(defn filter-edges [pred node]
  (select-keys (edges node) (filter-edge-ids pred node)))

(defn read-only? [layer]
  (*read-only* layer))

(defmacro with-readonly [layers & body]
  `(binding [*read-only* (into *read-only* ~layers)]
     ~@body))

(defn- refuse-readonly [layers]
  (doseq [layer layers]
    (if (read-only? layer)
      (throw (IllegalStateException.
              (format "Can't write to %s in read-only mode" layer)))
      (retro/modify! layer))))

(defmacro with-transaction
  "Execute forms within a transaction on the specified layers."
  [layer & forms]
  `(let [layer# ~layer]
     (retro/with-transaction layer#
       (do ~@forms
           layer#))))

(def touch retro/touch)

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

(defn node-seq
  "Return a lazy sequence of all [id node] pairs in the layer."
  [layer]
  (layer/node-seq layer))

(defn node-id-subseq
  "Return a lazy subsequence of node ids in this layer."
  [layer cmp start]
  (layer/node-id-subseq layer cmp start))

(defn node-subseq
  "Return a lazy subsequence of [node id] pairs in the layer."
  [layer cmp start]
  (layer/node-subseq layer cmp start))

(let [sentinel (Object.)]
  (defn ^{:dynamic true} get-node
    "Fetch a node's data from this layer."
    [layer id & [not-found]]
    (layer/get-node layer id not-found))

  (defn find-node
    "Get a node's data along with its id."
    [layer id]
    (let [node (get-node layer id sentinel)]
      (when-not (identical? node sentinel)
        (map-entry id node)))))

(defn query-in-node*
  "Fetch data from inside a node, replacing it with not-found if it is missing,
   and immediately call a function on it."
  [layer keyseq not-found f & args]
  (if-let [query-fn (layer/query-fn layer keyseq not-found f)]
    (apply query-fn args)
    (if-let [query-fn (layer/query-fn layer keyseq not-found identity)]
      (apply f (query-fn) args)
      (let [[id & keys] keyseq
            node (layer/get-node layer id nil)]
        (apply f (get-in node keys) args)))))

(defn query-in-node
  "Fetch data from inside a node and immediately call a function on it."
  [layer keyseq f & args]
  (apply query-in-node* layer keyseq nil f args))

(defn get-in-node
  "Fetch data from inside a node."
  [layer keyseq & [not-found]]
  (query-in-node* layer keyseq not-found identity))

(defn get-edges
  "Fetch the edges for a node on this layer."
  [layer id]
  (get-in-node layer [id :edges] nil))

(defn get-in-edge
  "Fetch data from inside an edge."
  [layer [id to-id & keys] & [not-found]]
  (get-in-node layer (list* id :edges to-id keys) not-found))

(defn get-edge
  "Fetch an edge from node with id to to-id."
  [layer id to-id & [not-found]]
  (get-in-edge layer [id to-id] not-found))

(declare update-in-node!)

(defn- changed-edges [old-edges new-edges]
  (reduce (fn [edges [edge-id edge]]
            (let [was-present (not (:deleted edge))
                  is-present  (when-let [e (get edges edge-id)] (not (:deleted e)))]
              (if (= was-present is-present)
                (dissoc edges edge-id)
                (assoc-in* edges [edge-id :deleted] was-present))))
          new-edges old-edges))

(defn- update-edges! [layer [id & keys] old new]
  (when (layer/manage-incoming? layer)
    (doseq [[edge-id {:keys [deleted]}]
            (apply changed-edges
                   (map (comp :edges (partial assoc-in* {} keys))
                        [old new]))]
      ((if deleted layer/drop-incoming! layer/add-incoming!)
       layer edge-id id))))

(defn- update-changelog! [layer node-id]
  (when (layer/manage-changelog? layer)
    (when-let [rev-id (retro/current-revision layer)]
      (update-in-node! layer [(meta-id :revision-id)] (constantly rev-id))
      (when node-id
        (update-in-node! layer [(meta-id node-id) "affected-by"] conj rev-id)
        (update-in-node! layer [(meta-id :changed-ids) rev-id] conj node-id)))))

(defn update-in-node!
  "Update the subnode at keyseq by calling function f with the old value and any supplied args."
  [layer keyseq f & args]
  (refuse-readonly [layer])
  (with-transaction layer
    (when-not *skip-writes*
      (let [update-meta! (if (meta-id? (first keyseq))
                           (constantly nil)
                           (fn [keyseq old new]
                             (update-edges! layer keyseq old new)
                             (update-changelog! layer (first keyseq))))]
        (let [updater (partial layer/update-fn layer)
              keyseq  (seq keyseq)]
          (if-let [update! (updater keyseq f)]
            ;; maximally-optimized; the layer can do this exact thing well
            (let [{:keys [old new]} (apply update! args)]
              (update-meta! keyseq old new))
            (if-let [update! (and keyseq ;; don't look for assoc-in of empty keys
                                  (updater (butlast keyseq) assoc))]
              ;; they can replace this sub-node efficiently, at least
              (let [old (get-in-node layer keyseq)
                    new (apply f old args)]
                (update! (last keyseq) new)
                (update-meta! keyseq old new))

              ;; need some special logic for unoptimized top-level assoc/dissoc
              (if-let [update! (and (not keyseq) ({assoc  layer/assoc-node!
                                                   dissoc layer/dissoc-node!} f))]
                (let [[id new] args
                      old (when (layer/manage-incoming? layer)
                            (get-node layer id))]
                  (apply update! layer args)
                  (update-meta! [id] old new))
                (let [[id & keyseq] keyseq
                      old (get-node layer id)
                      new (if keyseq
                            (apply update-in* old keyseq f args)
                            (apply f old args))]
                  (layer/assoc-node! layer id new)
                  (update-meta! [id] old new))))))
        (swap! write-count inc)))))

(defn update-in-node
  "Functional version of update-in-node! for use in a transaction."
  [layer keyseq f & args]
  {layer [#(apply update-in-node! % keyseq f args)]})

(do (defn update-node!
      "Update a node by calling function f with the old value and any supplied args."
      [layer id f & args]
      (apply update-in-node! layer [id] f args))
    (defn update-node
      "Functional version of update-node! for use in a transaction."
      [layer id f & args]
      {layer [#(apply update-node! % id f args)]}))

(do (defn dissoc-node!
      "Remove a node from a layer (incoming links remain)."
      [layer id]
      (update-in-node! layer [] dissoc id))
    (defn dissoc-node
      "Functional version of update-node! for use in a transaction."
      [layer id]
      {layer [#(dissoc-node! % id)]}))

(do (defn assoc-node!
      "Create or set a node with the given id and value."
      [layer id value]
      (update-in-node! layer [] assoc id value))
    (defn assoc-node
      "Functional version of assoc-node! for use in a transaction."
      [layer id value]
      {layer [#(assoc-node! % id value)]}))

(defn unwrap-layer
  "Return the underlying layer object from a wrapped layer. Throws an exception
   if the layer is not a wrapping layer."
  [layer]
  (wrapped/unwrap layer))

(defn unwrap-all
  "Return the \"base\" layer from a stack of wrapped layers, unwrapping as many
   times as necessary (possibly zero) to get to it."
  [layer]
  (->> layer
       (iterate wrapped/unwrap)
       (drop-while #(satisfies? wrapped/Wrapped %))
       (first)))

(do (defn assoc-in-node!
      "Set attributes inside of a node."
      [layer keyseq value]
      (update-in-node! layer (butlast keyseq) assoc (last keyseq) value))
    (defn assoc-in-node
      "Functional version of assoc-in-node! for use in a transaction."
      [layer keyseq value]
      {layer #(assoc-in-node! % keyseq value)}))

(defn schema
  [layer id-or-type]
  (let [id (fix id-or-type
                keyword? #(str (name %) "-1"))]
    (layer/schema layer id)))

(defn fields
  "Return a map of fields to their metadata for the given layer."
  ([layer id-or-type]
     (keys (:fields (schema layer id-or-type)))))

(defn edges-valid? [layer edges]
  (or (not (layer/single-edge? layer))
      (> 2 (count edges))))

(defn node-valid?
  "Check if the given node is valid for the specified layer."
  [layer id attrs]
  (and (edges-valid? layer attrs)
       (layer/node-valid? layer attrs)))

(defn verify-node
  "Assert that the given node is valid for the specified layer."
  [layer id attrs]
  (assert (edges-valid? layer attrs))
  (layer/verify-node layer id attrs)
  nil)

(defn ^{:dynamic true} get-revisions
  "Return a seq of all revisions with data for this node."
  [layer id]
  (layer/get-revisions layer id))

(defn node-history
  "Return a map from revision number to node data, for each revision that
   affected this node, sorted with oldest-first."
  [layer id]
  (layer/node-history layer id))

(extend-type Object
  layer/ChangeLog
  (get-revisions [layer id]
    (get-in-node layer [(meta-id id) "affected-by"]))
  (get-changed-ids [layer rev]
    (get-in-node layer [(meta-id :changed-ids) rev]))

  retro/OrderedRevisions
  (max-revision [layer]
    (-> layer
        (retro/at-revision nil)
        (get-in-node [(meta-id :revision-id)])
        (or 0)))

  layer/Incoming
  ;; default behavior: use node meta with special prefix to track incoming edges
  (get-incoming [layer id]
    (get-in-node layer [(meta-id id) :incoming]))
  (add-incoming! [layer id from-id]
    (update-in-node! layer [(meta-id id) :incoming] adjoin (ordered-map from-id true)))
  (drop-incoming! [layer id from-id]
    (update-in-node! layer [(meta-id id) :incoming] adjoin (ordered-map from-id false))))

(defn ^{:dynamic true} get-incoming
  "Return the ids of all nodes that have incoming edges on this layer to this node (excludes edges marked :deleted)."
  [layer id]
  (let [incoming (layer/get-incoming layer id)]
    (if (set? incoming)
      incoming
      (into (ordered-set)
            (for [[k v] incoming
                  :when v]
              k)))))

(defn ^{:dynamic true} get-incoming-map
  "Return a map of incoming edges, where the value for each key indicates whether an edge is
   incoming from that node."
  [layer id]
  (let [incoming (layer/get-incoming layer id)]
    (if (map? incoming)
      incoming
      (into (ordered-map)
            (for [node incoming]
                 [node true])))))

(defn wrap-bindings
  "Wrap the given function with the current graph context."
  [f]
  (bound-fn ([& args] (apply f args))))

(defn wrap-caching
  "Wrap the given function with a new function that memoizes read methods. Nested wrap-caching calls
   are collapsed so only the outer cache is used."
  [f]
  (fn []
    (if *use-outer-cache*
      (f)
      (binding [*use-outer-cache* true
                get-node          (memoize-deref [write-count] get-node)
                get-incoming      (memoize-deref [write-count] get-incoming)
                get-revisions     (memoize-deref [write-count] get-revisions)]
        (f)))))

(defmacro with-caching
  "Enable caching for the given forms. See wrap-caching."
  ([form]
     `((wrap-caching (fn [] ~form))))
  ([cache form]
     `((fix (fn [] ~form) (boolean ~cache) wrap-caching))))
