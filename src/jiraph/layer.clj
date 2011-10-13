(ns jiraph.layer
  (:use [useful.utils :only [adjoin]]
        [clojure.stacktrace :only [print-trace-element]])
  (:require [retro.core :as retro])
  (:import (java.util Map$Entry)))

(def ^:dynamic *warn-on-fallback* false)

(defn fallback-warning []
  (when *warn-on-fallback*
    (let [stacktrace (seq (.getStackTrace (Exception.)))]
      (binding [*out* *err*]
        (print "Jiraph fallback warning: ")
        (print-trace-element (second stacktrace))
        (println)))))

(defprotocol Enumerate
  (node-id-seq [layer]
    "A seq of all node ids in this layer")
  (node-seq [layer]
    "A seq of all nodes in this layer"))

(defprotocol Counted
  (node-count [layer]
    "Return the total number of nodes in this layer."))

(defprotocol Schema
  (fields [layer] [layer subfields]
    "A list of canonical fields stored in this layer. Can be empty.")
  (node-valid? [layer attrs]
    "Check if the given node is valid according to the layer schema."))

(defprotocol LayerMeta
  (get-layer-meta [layer key]
    "Fetch layer-wide meta.")
  (assoc-layer-meta! [layer key val]
    "Store layer-wide meta."))

(defprotocol NodeMeta
  (get-meta [layer node key]
    "Fetch the metadata for a specific node.")
  (assoc-meta! [layer node key val]
    "Set the metadata for a node."))

(defprotocol UpdateMeta
  (update-meta! [layer node key f args]
    "Update the metadata for a node."))

(defprotocol Incoming
  (get-incoming [layer id]
    "Return the ids of all nodes that have an incoming edge to this one.")
  (add-incoming! [layer id from-id]
    "Add an incoming edge record on id for from-id.")
  (drop-incoming! [layer id from-id]
    "Remove the incoming edge record on id for from-id."))

(defprotocol Basic
  (get-node [layer id not-found]
    "Fetch a node from the graph.")
  (assoc-node! [layer id attrs]
    "Add a node to the graph.")
  (dissoc-node! [layer id]
    "Remove a node from the graph."))

(defprotocol Optimized
  "Describe to Jiraph how to perform a more-optimized version of some subset of operations on a
  layer. These functions will be called on a layer in preparation to actually performing an
  operation on some internal node. The layer is responsible for determining whether that operation
  can be optimized at that position in the tree; this will usually be implemented by inspecting
  the function passed in and testing it against some internal list of optimizable functions.

  If nil is returned, the operation will be done in some less optimized way, often by calling
  get-node, applying the update, and then assoc-node!. If an optimization is possible, you should
  return a function which will take any additional args to f (but not f itself, or the keyseq;
  those should be closed over and captured in the returned function); Jiraph will call this
  function with the appropriate arguments.

  For example, Jiraph might ask for an optimization with (update-fn layer [from-id :edges to-id]
  assoc), and call the returned function as (the-fn :rel :child), thus achieving basically the
  same effect as (update-in layer [from-id :edges to-id] assoc :rel :child), or (assoc-in layer
  [from-id :edges to-id :rel] child)."

  (update-fn [layer keyseq f]
    "Get a function for performing an optimized update/mutation on the layer at a specified
    node. See documentation of Optimized for the general contract of Optimized functions. In
    addition, the function returned by update-fn should, when called, return a hash containing
    information about the changes it made, if possible.

    The hash should contain keys :old and :new, describing the contents of the sub-node (that is,
    the one in the layer under keyseq) before and after the update was applied. It is legal for
    these to both be nil (eg, returning an empty hash), but in that case Jiraph will be unable to
    provide some services, such as managing incoming edges for you.

    Jiraph's behavior in the case of return values of any type other than hash (including nil) is
    unspecified; these may receive special handling in some future version.")

  (query-fn [layer keyseq f]
    "Get a function for performing an optimized read on the layer at a specified node. See
    documentation of Optimized for the general contract of Optimized functions. The function
    returned by query-fn should, when called, call f on the data at keyseq (presumably in some
    optimized way), and return f's result.

    For example, a layer might store a node's edge-count in a separate field which can be read
    without reading the edges themselves; in that case, (query-fn layer [node-name :edges] count)
    should return a function like (fn [] (get-from-db (str node-name \":edge-count\")))."))

(defprotocol Layer
  "Jiraph layer protocol"
  (open [layer]
    "Open the layer file.")
  (close [layer]
    "Close the layer file.")
  (sync! [layer]
    "Flush all layer changes to the storage medium.")
  (optimize! [layer]
    "Optimize underlying layer storage.")
  (truncate! [layer]
    "Removes all node data from the layer.")
  (node-exists? [layer id]
    "Check if a node exists on this layer."))

(defprotocol ChangeLog
  (get-revisions [layer id]
    "The revisions that changed the given node.")
  (get-changed-ids [layer rev]
    "The nodes changed by the given revision.")
  (max-revision [layer]
    "The most recent revision of this layer, disregarding `at-revision` restrictions."))

(defprotocol Preferences
  "Indicate to Jiraph what things you want it to do for you. These preferences should not
  change while the system is running; Jiraph may choose to cache any of them."
  (manage-incoming? [layer]
    "Should Jiraph decide when to add/drop incoming edges?")
  (manage-changelog? [layer]
    "Should Jiraph store changelog information for you?")
  (single-edge? [layer]
    "Is it illegal to have more than one outgoing edge per node on this layer?"))

;; Meta-related protocols
(letfn [(layer-meta-key [id] (str "_" id))
        (node-meta-key [node key] (str "_" node "_" key))
        (incoming-key [id] "incoming")]
  (extend-type Object
    ChangeLog
    (get-revisions [layer id]
      (get-meta layer id "affected-by"))
    (get-changed-ids [layer rev]
      (get-layer-meta layer (str "changed-ids-" rev)))
    (max-revision [layer]
      (-> layer
          (retro/at-revision nil)
          (get-layer-meta "revision-id")
          (or 0)))

    LayerMeta
    ;; default behavior: create specially-named regular nodes to hold metadata
    (get-layer-meta [layer key]
      (fallback-warning)
      (get-node layer (layer-meta-key key) nil))
    (assoc-layer-meta! [layer key value]
      (fallback-warning)
      (assoc-node! layer (layer-meta-key key) value))

    NodeMeta
    ;; default behavior: use specially-named layer-wide meta to fake node metadata
    (get-meta [layer node key]
      (fallback-warning)
      (get-layer-meta layer (node-meta-key node key)))
    (assoc-meta! [layer node key val]
      (fallback-warning)
      (assoc-layer-meta! layer (node-meta-key node key) val))

    UpdateMeta
    ;; default behavior: get old meta, apply f, set new value
    (update-meta! [layer node key f args]
      (assoc-meta! layer node key (apply f (get-meta layer node key) args)))

    Incoming
    ;; default behavior: use node meta with special prefix to track incoming edges
    (get-incoming [layer id]
      (fallback-warning)
      (get-meta layer id (incoming-key id)))
    (add-incoming! [layer id from-id]
      (fallback-warning)
      (update-meta! layer id (incoming-key id) (fnil conj #{}) [from-id]))
    (drop-incoming! [layer id from-id]
      (fallback-warning)
      (update-meta! layer id (incoming-key id) disj [from-id]))))

;; these guys want a default/permanent sentinel
(let [sentinel (Object.)]
  (extend-type Object
    Layer
    ;; default implementation is to not do anything, hoping you do it
    ;; automatically at reasonable times, or don't need it done at all
    (open      [layer] nil)
    (close     [layer] nil)
    (sync!     [layer] nil)
    (optimize! [layer] nil)

    ;; we can simulate these for you, pretty inefficiently
    (truncate! [layer]
      (fallback-warning)
      (doseq [id (node-id-seq layer)]
        (dissoc-node! layer id)))

    (node-exists? [layer id]
      (fallback-warning)
      (not= sentinel (get-node layer id sentinel)))

    Preferences
    ;; opt in to managed-incoming, and let the layer set a :single-edge key to
    ;; indicate it should be in single-edge mode
    (manage-incoming? [layer] true)
    (manage-changelog? [layer] true)
    (single-edge? [layer]
      (:single-edge layer))))

;; Don't need any special closures here
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
  (node-seq    [layer] ())
  (node-id-seq [layer] ())

  Counted
  ;; default behavior: walk through all node ids, counting. not very fast
  (node-count [layer]
    (fallback-warning)
    (apply + (map (constantly 1) (node-id-seq layer))))

  Optimized
  ;; can't optimize anything
  (update-fn [layer keyseq f] nil)
  (query-fn  [layer keyseq f] nil))

(defn default-impl [protocol]
  (get-in protocol [:impls Object]))

(defn skip-applied-revs
  "Useful building block for skipping applied revisions. Calling this on a layer
  will empty the layer's queue if the revision has already been applied; it
  will otherwise increment the layer's revision to point to the to-be-written
  revision number."
  [layer]
  (let [[rev max] ((juxt retro/current-revision max-revision) layer)]
    (if rev
      (if (and max (< rev max))
        (retro/empty-queue layer)
        (retro/at-revision layer (inc rev)))
      (if max
        (throw (IllegalStateException.
                "Trying to write to revisioned layer without a revision set."))
        layer))))
