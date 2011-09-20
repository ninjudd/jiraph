(ns jiraph.layer
  (:use [retro.core :only [*revision*]]
        [useful.utils :only [adjoin]]))

(def ^:dynamic *fallback-warnings* false)

(defn fallback-warning [layer impl]
  (.printf *err* "Layer %s falling back on %s\n" layer impl))

(defmacro ^{:private true} fallback [impl]
  `(do
     (when *fallback-warnings*
       (fallback-warning ~'layer '~impl))
     ~impl))

(let [layer-meta-key (fn [id] (str "_" id))
      node-meta-key (fn [node key] (str "_" node "_" key))
      incoming-key (fn [id] (node-meta-key id "incoming"))]

  (extend-type Object
    Schema
    (fields
      ([layer] nil)
      ([layer subfields] nil))
    ;; TODO add a whats-wrong-with-this-node function
    (node-valid? [layer attrs] true)

    LayerMeta
    ;; Implemented in terms of Compound
    (get-layer-meta [layer key]
      ((fallback get-node) layer (layer-meta-key key)))
    (assoc-layer-meta! [layer key value]
      ((fallback assoc-node!) layer (layer-meta-key key) value))

    NodeMeta
    ;; Implemented in terms of LayerMeta
    (get-meta [layer node key]
      ((fallback get-layer-meta) layer (node-meta-key node key)))
    (assoc-meta! [layer node key val]
      ((fallback assoc-layer-meta!) layer (node-meta-key node key) val))

    UpdateMeta
    (update-meta! [layer node key f args]
      ((fallback assoc-meta!) layer node key (apply f (get-meta layer node key) args)))

    Counted
    ;; sorry, you have to implement this yourself

    Incoming
    (get-incoming [layer id]
      ((fallback get-meta) layer id (incoming-key id)))
    (add-incoming! [layer id from-id]
      ((fallback update-meta!) layer id (incoming-key id) conj from-id))
    (drop-incoming! [layer id from-id]
      ((fallback update-meta!) layer id (incoming-key id) disj from-id))

    Compound
    (get-node [layer id]
      ((fallback get-in-node) [layer id []]))
    (assoc-node! [layer id attrs]
      ((fallback assoc-in-node!) layer id [] attrs))
    (dissoc-node! [layer id]
      ((fallback dissoc-in-node!) layer id []))))

(defprotocol Counted
  (node-count       [layer]            "Return the total number of nodes in this layer."))

(defprotocol Schema
  (fields
    [layer]
    [layer subfields]
    "A list of canonical fields stored in this layer. Can be empty.")
  (node-valid?  [layer attrs]      "Check if the given node is valid according to the layer schema."))

(defprotocol LayerMeta
  (get-layer-meta         [layer key]        "Fetch a layer-wide property.")
  (assoc-layer-meta!      [layer key val]    "Store a layer-wide property."))

(defprotocol NodeMeta
  (get-meta               [layer node key])
  (assoc-meta!            [layer node key val]))
(defprotocol UpdateMeta
  (update-meta!           [layer node key f args]))

(defprotocol Revisioned
  (get-revision
    [layer] [layer id]
    "Return the revision this layer is at, or the revision of the specified node."))

(defprotocol Incoming
  (get-incoming     [layer id]         "Return the ids of all nodes that have an incoming edge to this one.")
  (add-incoming!    [layer id from-id] "Add an incoming edge record on id for from-id.")
  (drop-incoming!   [layer id from-id] "Remove the incoming edge record on id for from-id."))

(defprotocol Compound
  (get-node         [layer id]         "Fetch a node.")
  (assoc-node!      [layer id attrs]   "Add a node to the database.")
  (dissoc-node!     [layer id]         "Remove a node from the database."))

(defprotocol Layer
  "Jiraph layer protocol"
  (open             [layer]            "Open the layer file.")
  (close            [layer]            "Close the layer file.")
  (sync!            [layer]            "Flush all layer changes to the storage medium.")
  (optimize!        [layer]            "Optimize underlying layer storage.")
  (truncate!        [layer]            "Removes all node data from the layer.")
  (get-revisions    [layer id]         "Return all revision ids for a given node.")
  (node-exists?     [layer id]         "Check if a node exists on this layer."))

(defprotocol Seqable
  (seq-in-node [layer keyseq] "A seq of all the properties under the given keyseq."))

(defprotocol Sorted
  "Methods for sorted layers."
  (seq-in-node
    [layer keyseq test key]
    [layer keyseq stest skey etest ekey])
  (rseq-in-node
    [layer keyseq test key]
    [layer keyseq stest skey etest ekey])
  (dissoc-subseq-in-node
    [layer keyseq test key]
    [layer keyseq test key etest ekey]))

(defprotocol Nested
  "Layer that has indexed access into some subset of 'deeper' attributes, such as edges."
  (get-in-node [layer keyseq])
  (assoc-in-node! [layer keyseq value])
  (dissoc-in-node! [layer keyseq]))

(defprotocol Update
  "Layer that can do (some) updates of sub-nodes more efficiently than fetching a whole node, changing it, and then writing it."
  (update-in-node!
    [layer keyseq f argseq]
    "Arguments are as to clojure.core/update-in, but without varargs."))
