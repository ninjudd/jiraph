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

(def ^{:private true :dynamic true} *compacting* false)
(def ^{:private true :dynamic true} *use-outer-cache* nil)

(defn edges
  "Gets edges from a node. Returns all edges, including deleted ones."
  [node]
  (:edges node))

(defn filter-edge-ids [pred node]
  (filter-keys-by-val pred (edges node)))

(defn filter-edges [pred node]
  (select-keys (edges node) (filter-edge-ids pred node)))

(defn- refuse-readonly [layers]
  (doseq [layer layers]
    (retro/modify! layer)))

(defmacro with-action
  "Construct a simple retro IOValue that touches exactly one layer.

   The IOValue's :value will be the one you specify, and its :actions will
   evaluate body with layer-name bound to (an appropriately-revisioned view of)
   layer-value."
  [[layer-name layer-value] value & body]
  `(retro/with-actions ~value
     {~layer-value [(fn [~layer-name]
                      ~@body)]}))

(defn advance-reader
  "Given a vector of actualized jiraph IOValues and a read function, return a new read function by
  calling the :wrap-read function of each action."
  [read actions]
  (reduce (fn [read wrapper]
            (wrapper read))
          read, (map :wrap-read actions)))

(defn compose
  "Compose a number of jiraph IOValues (functions of read) into a single action."
  [& fs]
  (fn [read]
    (first (reduce (fn [[actions read] f]
                     (let [more-actions (f read)]
                       [(into actions more-actions)
                        (advance-reader read more-actions)]))
                   [[] read]
                   (remove nil? fs)))))

(defn same?
  "Determine whether two objects are layers using the same storage backend. Useful because = will
   compare incidentals such as current revision, but you may wish to ignore those."
  [x y]
  (and (= (class x) (class y))
       (layer/same? x y)))

(defn path-parts
  "Given two paths, return a triple of [shared, read, write]. If neither path is a prefix of the
  other, then nil is returned; otherwise, shared will be the shorter of the two, and whatever is
  remaining from the longer path will be returned under either read or write (according to whichever
  of the input arguments was longer)."
  [read-path write-path]
  (loop [shared [], read-path read-path, write-path write-path]
    (cond (empty? read-path) [shared write-path []]
          (empty? write-path) [shared [] read-path]
          (not= (first read-path) (first write-path)) nil
          :else (recur (conj shared (first read-path))
                       (rest read-path), (rest write-path)))))

(defn read-wrapper
  "Create a simple wrap-read function representing a single update to the specified layer, at the
  specified keyseq, to become (apply f current-value args)."
  [layer write-keyseq f args]
  (fn [read]
    (fn [layer' read-keyseq]
      (if-let [[read-path update-path get-path]
               (and (same? layer layer')
                    (path-parts read-keyseq write-keyseq))]
        (let [update (partial apply update-in*)]
          (-> (read layer' read-path)
              (update update-path f args)
              (get-in get-path)))
        (read layer' read-keyseq)))))

(defn simple-ioval
  "Given the arguments to update-in-node, return a function for creating a basic jiraph iovalue.
   The returned function expects to be passed a write function for use in the real retro iovalue.

   That's a little wordy and hard to keep track of; below is a sort of pseudo type-signature. It's
   still a bit hard to follow, but is at least clear about what functions are called when and what
   args they want.

   (Layer -> keyseq -> f -> args -> (write -> (read -> ioval)))"
  [layer keyseq f args]
  (fn [write]
    (fn [read]
      [{:write write :wrap-read (read-wrapper layer keyseq f args)
        :layer layer :keyseq keyseq :f f :args args}])))

(letfn [(layers-op [layers f]
          (dorun (map f layers)))]

  (defn sync!
    "Flush changes for the specified layers to the storage medium."
    [& layers]
    (layers-op layers layer/sync!))

  (defn optimize!
    "Optimize the underlying storage for the specified layers."
    [& layers]
    (layers-op layers layer/optimize!))

  (defn truncate!
    "Remove all nodes from the specified layers."
    [& layers]
    (refuse-readonly layers)
    (layers-op layers layer/truncate!)))

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

(defn ->retro-ioval
  "Convert a jiraph IOValue to a retro IOValue."
  [ioval]
  (let [actions (ioval get-in-node)]
    (retro/with-actions nil
      (reduce (fn [retro-ioval {:keys [layer write]}]
                (update-in retro-ioval [layer] (fnil conj []) write))
              {}, actions))))

(def touch retro/touch)
(defmacro txn [actions]
  `(retro/txn (->retro-ioval ~actions)))
(defmacro unsafe-txn [actions]
  `(retro/unsafe-txn (->retro-ioval ~actions)))
(defmacro dotxn [layers & body]
  `(retro/dotxn ~layers ~@body))

(defn update-in-node
  "Return an IOValue that will update the subnode at keyseq by calling function
  f with the old value and any supplied args."
  [layer keyseq f & args]
  (layer/update-in-node layer keyseq f args))

(defn update-in-node!
  "Mutable version of update-in-node: applies changes immediately, at current
  revision."
  [layer keyseq f & args]
  (unsafe-txn
   (apply update-in-node layer keyseq f args)))

(do (defn update-node
      "Update a node by calling function f with the old value and any supplied args."
      [layer id f & args]
      (apply update-in-node layer [id] f args))
    (defn update-node!
      "Mutable version of update-node: applies changes immediately, at current revision."
      [layer id f & args]
      (unsafe-txn (apply update-node layer id f args))))

(do (defn dissoc-node
      "Remove a node from a layer."
      [layer id]
      (update-in-node layer [] dissoc id))
    (defn dissoc-node!
      "Mutable version of dissoc-node: changes are applied immediately, at the current revision."
      [layer id]
      (unsafe-txn (dissoc-node layer id))))

(do (defn assoc-node
      "Create or set a node with the given id and value."
      [layer id value]
      (update-in-node layer [] assoc id value))
    (defn assoc-node!
      "Mutable version of assoc-node: changes are applied immediately, at the current revision."
      [layer id value]
      (unsafe-txn (assoc-node layer id value))))

(do (defn assoc-in-node
      "Set attributes inside of a node."
      [layer keyseq value]
      (update-in-node layer (butlast keyseq) assoc (last keyseq) value))
    (defn assoc-in-node!
      "Mutable version of assoc-in-node: changes are applied immediately, at the current revision."
      [layer keyseq value]
      (unsafe-txn (assoc-in-node layer keyseq value))))

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

(defn schema
  [layer id-or-type]
  (let [id (fix id-or-type
                keyword? #(str (name %) "-1"))]
    (layer/schema layer id)))

(defn fields
  "Return a map of fields to their metadata for the given layer."
  ([layer id-or-type]
     (keys (:fields (schema layer id-or-type)))))

(defn node-valid?
  "Check if the given node is valid for the specified layer."
  [layer id attrs]
  (layer/node-valid? layer attrs))

(defn verify-node
  "Assert that the given node is valid for the specified layer."
  [layer id attrs]
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
        (or 0))) ;; these guys want a default/permanent sentinel

  layer/Layer
  ;; default implementation is to not do anything, hoping you do it
  ;; automatically at reasonable times, or don't need it done at all
  (open      [layer] nil)
  (close     [layer] nil)
  (sync!     [layer] nil)
  (optimize! [layer] nil)

  ;; we can simulate this for you, pretty inefficiently
  (truncate! [layer]
    (unsafe-txn
     (apply compose
            (for [id (node-id-seq layer)]
              (update-in-node layer [] dissoc id))))))

(defn- get-incoming-edges
  "Get incoming edges from a node, including whatever data is on the edges."
  [layer id]
  (when-let [incoming-layer (wrapped/child layer :incoming)]
    (get-in-node incoming-layer [id :edges])))

(defn ^{:dynamic true} get-incoming-map
  "Return a map of incoming edges, where the value for each key indicates whether an edge is
   incoming from that node."
  [layer id]
  (into (ordered-map)
        (for [[node-id {:keys [deleted]}] (get-incoming-edges layer id)]
          [node-id (not deleted)])))

(defn ^{:dynamic true} get-incoming
  "Return the ids of all nodes that have incoming edges on this layer to this node (excludes edges marked :deleted)."
  [layer id]
  (into (ordered-set)
        (for [[node-id {:keys [deleted]}] (get-incoming-edges layer id)
              :when (not deleted)]
          node-id)))

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
                get-node          (memoize get-node)
                get-incoming      (memoize get-incoming)
                get-revisions     (memoize get-revisions)]
        (f)))))

(defmacro with-caching
  "Enable caching for the given forms. See wrap-caching."
  ([form]
     `((wrap-caching (fn [] ~form))))
  ([cache form]
     `((fix (fn [] ~form) (boolean ~cache) wrap-caching))))
