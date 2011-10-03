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

(defprotocol Basic
  (get-node      [layer id not-found] "Fetch a node.")
  (assoc-node!   [layer id attrs]     "Add a node to the database.")
  (dissoc-node!  [layer id]           "Remove a node from the database."))

(defprotocol Optimized
  "Describe to jiraph how to perform a more-optimized version of some subset of operations on a
  layer. These functions will be called on a layer in preparation to actually performing an
  operation on some internal node. The layer is responsible for determining whether that operation
  can be optimized at that position in the tree; this will usually be implemented by inspecting
  the function passed in and testing it against some internal list of optimizable functions.

  If nil is returned, the operation will be done in some less optimized way, often by calling
  get-node, applying the update, and then assoc-node!.  If an optimization is possible, you should
  return a function which will take any additional args to f (but not f itself, or the keyseq;
  those should be closed over and captured in the returned function); jiraph will call this
  function with the appropriate arguments.

  For example, jiraph might ask for an optimization with (update-fn layer [from-id :edges to-id]
  assoc), and call the returned function as (the-fn :rel :child), thus achieving basically the
  same effect as (update-in layer [from-id :edges to-id] assoc :rel :child), or (assoc-in layer
  [from-id :edges to-id :rel] child)."

  (update-fn [layer keyseq f] "Get a function for performing an optimized update/mutation on the
  layer at a specified node. See documentation of Optimized for the general contract of Optimized
  functions. In addition, the function returned by update-fn should, when called, return a hash
  containing information about the changes it made, if possible.

  The hash should contain keys :old and :new, describing the contents of the sub-node (that is,
  the one in the layer under keyseq) before and after the update was applied. It is legal for
  these to both be nil (eg, returning an empty hash), but in that case jiraph will be unable to
  provide some services, such as managing incoming edges for you.

  jiraph's behavior in the case of return values of any type other than hash (including nil) is
  unspecified; these may receive special handling in some future version.")

  (query-fn [layer keyseq f] "Get a function for performing an optimized read on the layer at a
  specified node. See documentation of Optimized for the general contract of Optimized
  functions. The function returned by update-fn should, when called, call f on the data at
  keyseq (presumably in some optimized way), and return f's result.

  For example, a layer might store a node's edge-count in a separate field which can be read
  without reading the edges themselves; in that case, (query-fn layer [node-name :edges] count)
  should return a function like (fn [] (get-from-db (str node-name \":edge-count\")))."))

(defprotocol Layer
  "Jiraph layer protocol"
  (open             [layer]            "Open the layer file.")
  (close            [layer]            "Close the layer file.")
  (sync!            [layer]            "Flush all layer changes to the storage medium.")
  (optimize!        [layer]            "Optimize underlying layer storage.")
  (truncate!        [layer]            "Removes all node data from the layer.")
  (get-revisions    [layer id]         "Return all revision ids for a given node.")
  (node-exists?     [layer id]         "Check if a node exists on this layer."))

(defprotocol Preferences
  "Indicate to jiraph what things you want it to do for you. These preferences should not
  change while the system is running; jiraph may choose to cache any of them."
  (manage-incoming? [layer] "Should jiraph decide when to add/drop incoming edges?")
  (single-edge?     [layer] "Is it legal to have more than one edge from a node on this layer?"))

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
  "Layer that can do (some) updates of sub-nodes more efficiently than fetching
  a whole node, changing it, and then writing it. See docs for
  jiraph.layer/update-node! for 'optional' parts of the Update contract."
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
    (update-node! [layer id f args]
      ((fallback assoc-node!) layer id
       (apply f ((fallback get-node) layer id) args)))

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

    Layer
    ;; default implementation is to not do anything, hoping you do it
    ;; automatically at reasonable times, or don't need it done at all
    (open [layer] nil)
    (close [layer] nil)
    (sync! [layer] nil)
    (optimize! [layer] nil)

    Preferences
    (manage-incoming? [layer] true)
    (single-edge? [layer] (:single-edge layer))

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
            (apply seqfn ((fallback seq-in-node) layer keyseq) args)))]
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
                                                                   item))))))}))

(defn default-impl [protocol]
  (get-in protocol [:impls Object]))