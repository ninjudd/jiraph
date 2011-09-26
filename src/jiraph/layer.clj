(ns jiraph.layer
  (:use [retro.core :only [*revision*]]
        [useful.utils :only [adjoin]])
  (:import (java.util Map$Entry)))

(def ^:dynamic *fallback-warnings* false)

(defn fallback-warning [layer impl]
  (.printf *err* "Layer %s falling back on %s\n" (class layer) impl))

(defmacro ^{:private true} fallback [impl]
  `(do
     (when *fallback-warnings*
       (fallback-warning ~'layer '~impl))
     ~impl))

(defprotocol Enumerate
  (node-id-seq [layer] "A seq of all node ids in this layer")
  (node-seq    [layer] "A seq of all nodes in this layer"))

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

(defprotocol Incoming
  (get-incoming     [layer id]         "Return the ids of all nodes that have an incoming edge to this one.")
  (add-incoming!    [layer id from-id] "Add an incoming edge record on id for from-id.")
  (drop-incoming!   [layer id from-id] "Remove the incoming edge record on id for from-id."))

(defprotocol Compound
  (get-node         [layer id not-found] "Fetch a node.")
  (assoc-node!      [layer id attrs]     "Add a node to the database.")
  (dissoc-node!     [layer id]           "Remove a node from the database.")
  (update-node!     [layer id f args]    "Update a node in the database."))

(defprotocol Revisioned
  (get-revision
    [layer] [layer id]
    "Return the revision this layer is at, or the revision of the specified node."))

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
  (subseq-in-node
    [layer keyseq test key]
    [layer keyseq stest skey etest ekey])
  (rsubseq-in-node
    [layer keyseq test key]
    [layer keyseq stest skey etest ekey])
  (dissoc-subseq-in-node
    [layer keyseq test key]
    [layer keyseq test key etest ekey]))

(defprotocol Nested
  "Layer that has indexed access into some subset of 'deeper' attributes, such as edges."
  (get-in-node     [layer keyseq not-found])
  (assoc-in-node!  [layer keyseq value])
  (dissoc-in-node! [layer keyseq]))

(defprotocol Update
  "Layer that can do (some) updates of sub-nodes more efficiently than fetching a whole node, changing it, and then writing it."
  (update-in-node!
    [layer keyseq f argseq]
    "Arguments are as to clojure.core/update-in, but without varargs."))

;; Meta-related protocols
(letfn [(layer-meta-key [id] (str "_" id))
        (node-meta-key [node key] (str "_" node "_" key))
        (incoming-key [id] (node-meta-key id "incoming"))]
  (extend-type Object
    LayerMeta
    ;; default behavior: create specially-named regular nodes to hold metadata
    (get-layer-meta [layer key]
      ((fallback get-node) layer (layer-meta-key key)))
    (assoc-layer-meta! [layer key value]
      ((fallback assoc-node!) layer (layer-meta-key key) value))

    NodeMeta
    ;; default behavior: use specially-named layer-wide meta to fake node metadata
    (get-meta [layer node key]
      ((fallback get-layer-meta) layer (node-meta-key node key)))
    (assoc-meta! [layer node key val]
      ((fallback assoc-layer-meta!) layer (node-meta-key node key) val))

    UpdateMeta
    ;; default behavior: get old meta, apply f, set new value
    (update-meta! [layer node key f args]
      ((fallback assoc-meta!) layer node key (apply f ((fallback get-meta) layer node key) args)))

    Incoming
    ;; default behavior: use node meta with special prefix to track incoming edges
    (get-incoming [layer id]
      ((fallback get-meta) layer id (incoming-key id)))
    (add-incoming! [layer id from-id]
      ((fallback update-meta!) layer id (incoming-key id) conj from-id))
    (drop-incoming! [layer id from-id]
      ((fallback update-meta!) layer id (incoming-key id) disj from-id))))

;; these guys want a default/permanent sentinel
(let [sentinel (Object.)]
  (extend-type Object
    Schema
    ;; default behavior: no fields, all nodes valid
    (fields
      ([layer] nil)
      ([layer subfields] nil))
    ;; TODO add a whats-wrong-with-this-node function
    (node-valid? [layer attrs] true)

    Enumerate
    ;; Fallback behavior is the empty list. Jiraph does *not* automatically
    ;; track assoc'd and dissoc'd nodes for you in order to provide node-ids,
    ;; because that would be a huge performance hit.
    (node-id-seq [layer]
      ())
    (node-seq [layer]
      ())

    Counted
    ;; default behavior: walk through all node ids, counting. not very fast
    (node-count [layer]
      (apply + (map (constantly 1) ((fallback node-id-seq) layer))))

    Compound
    ;; default behavior:
    (get-node [layer id not-found]
      ((fallback get-in-node) layer [id] not-found))
    (assoc-node! [layer id attrs]
      ((fallback assoc-in-node!) layer [id] attrs))
    (dissoc-node! [layer id]
      ((fallback dissoc-in-node!) layer [id]))
    (update-node! [layer k f args]
      ((fallback assoc-node!) layer id
       (apply update-in ((fallback get-node) layer id) [k] f args)))

    Nested
    (get-in-node [layer keyseq not-found]
      (let [[id & path] keyseq]
        (get-in ((fallback get-node) layer id) path not-found)))
    (assoc-in-node! [layer keyseq val]
      (let [[path end] ((juxt butlast last) keyseq)]
        ((fallback update-in-node!) layer path assoc end val)))
    (dissoc-in-node! [layer keyseq]
      (let [[path end] ((juxt butlast last) keyseq)]
        ((fallback update-in-node!) layer path dissoc end)))
    (update-in-node! [layer keyseq f args]
      (let [[id & path] keyseq]
        ((fallback update-node!) layer id update-in (list* path f args))))

    Revisioned
    (get-revision
      ([layer] 0)
      ([layer id] 0))

    Layer
    ;; default implementation is to not do anything, hoping you do it
    ;; automatically at reasonable times, or don't need it done at all
    (open [layer] nil)
    (close [layer] nil)
    (sync! [layer] nil)
    (optimize! [layer] nil)
    (get-revisions [layer id]
      ())

    ;; we can simulate these for you, pretty inefficiently
    (truncate! [layer]
      (let [dissoc! (fallback dissoc-node!)]
        (doseq [id ((fallback node-id-seq) layer)]
          (dissoc! layer id))))
    (node-exists? [layer id]
      (not= sentinel ((fallback get-node) layer id sentinel)))

    Seqable
    (seq-in-node [layer keyseq]
      (seq ((fallback get-in-node) layer keyseq)))))

;; Use raw extend format to allow recycling functions instead of using literals
(letfn [(subseq-impl [seqfn]
          (fn [layer keyseq & args]
            (apply seqfn ((fallback seq-in-node) layer keyseq) fnargs)))]
  (extend Object
    Sorted
    {:subseq-in-node (subseq-impl subseq)
     :rsubseq-in-node (subseq-impl rsubseq)
     :dissoc-subseq-in-node (fn [layer keyseq & args]
                              (let [keyseq (vec keyseq)
                                    dissoc-in! (fallback dissoc-in-node!)]
                                (doseq [item (apply (fallback subseq-in-node) layer keyseq args)]
                                  (dissoc-in! layer (conj keyseq (if (instance? Map$Entry item)
                                                                   (.getKey ^Map$Entry item)
                                                                   pitem))))))}))

(defn default-impl [protocol]
  (get-in protocol [:impls Object]))