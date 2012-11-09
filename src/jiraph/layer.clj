(ns jiraph.layer
  (:use [useful.utils :only [adjoin map-entry]]
        [clojure.stacktrace :only [print-trace-element]]
        [useful.seq :only [assert-length]])
  (:require [retro.core :as retro])
  (:import (java.util Map$Entry)))

(def ^:dynamic *warn-on-fallback* false)

(defprotocol Enumerate
  (node-id-seq [layer]
    "A seq of all node ids in this layer.")
  (node-seq [layer]
    "A seq of all [id, node] entries in this layer."))

(defprotocol SortedEnumerate
  (node-id-subseq [layer opts]
    "An ordered, bounded seq of all node ids in this layer.")
  (node-subseq [layer opts]
    "An ordered, bounded seq of all nodes in this layer."))

(defprotocol Schema
  (schema [layer id]
    "A map describing the structure of a node with the given id on this layer. Return value will
     be a nested hash. Each hash will contain at least a :type key, describing the data at that
     level (with :any meaning \"it could be anything\"). Additional keys may be present:
     - For maps, a :fields key descibes the schema for all fields the map may contain - each key
       in the :fields map is a fieldname and its value is the schema for that value. A field with
       the special name :* means any key may be present, in addition to those specifically listed.
     - For other composite types such as sets and lists, an :item-type key describes the schema
       for the items contained within the composite (which should all be the same type).")
  (verify-node [layer id attrs]
    "Verify that the given node is valid according to the layer schema."))

(defprotocol Basic
  (get-node [layer id not-found]
    "Fetch a node from the graph.")

  (update-in-node [layer keyseq f args]
    "Return a jiraph IOValue that will update the layer under the given keyseq by calling
     (apply f current-value args). If keyseq is empty, f must be either assoc or dissoc, in which
     case an entire node will be either destroyed (in the case of dissoc) or added/overwritten (in
     the case of assoc)."))

(defprotocol Optimized
  "Describe to Jiraph how to perform a more-optimized version of some subset of operations on a
  layer. These functions will be called on a layer in preparation to actually performing an
  operation on some internal node. The layer is responsible for determining whether that operation
  can be optimized at that position in the tree; this will usually be implemented by inspecting
  the function passed in and testing it against some internal list of optimizable functions, or by
  checking the keyseq to see if optimized access is possible at this node.

  If nil is returned, the operation will be done in some less optimized way, often by calling
  get-node, applying the update, and then assoc-node. If an optimization is possible, you should
  return a function which will take any additional args to f (but not f itself, or the keyseq:
  those should be closed over and captured in the returned function); Jiraph will call this
  function with the appropriate arguments.

  For example, Jiraph might ask for an optimization with (update-fn layer [from-id :edges to-id]
  assoc), and call the returned function as (the-fn :rel :child), thus achieving basically the
  same effect as (update-in layer [from-id :edges to-id] assoc :rel :child), or (assoc-in layer
  [from-id :edges to-id :rel] child)."

  (query-fn [layer keyseq not-found f]
    "Get a function for performing an optimized read on the layer at a specified node. See
    documentation of Optimized for the general contract of Optimized functions. The function
    returned by query-fn should, when called, call f (presumably in some optimized way) on the data
    at keyseq (or not-found if no data is present), and return f's result.

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
  (same? [layer other]
    "Given two layer objects (which must be of the same class), tell whether they refer to the same
    storage backend. Useful because = will compare incidentals such as current revision, but you may
    wish to ignore those."))

(defprotocol ChangeLog
  (get-revisions [layer id]
    "The revisions that changed the given node.")
  (get-changed-ids [layer rev]
    "The nodes changed by the given revision."))

(defprotocol Historical
  (node-history [layer id]
    "Return a map from revision number to node data, for each revision that
     affected this node. The map must be sorted, with earliest revisions first."))

(defprotocol Parent
  (children [layer]
    "Return the names of all the children that this layer has, ie those for
    which (child l name) would return non-nil.")
  (child [layer name]
    "Find a layer which is related in some way to this one. For example, pass :incoming to get the
    layer (if any) on which incoming edges from this layer are stored."))

(def ^{:doc "A default schema describing a map which contains edges and possibly other keys."}
  edges-schema {:schema {:type :map
                         :fields {:edges {:type :map
                                          :fields {:* :any}}
                                  :* :any}}})

;; Don't need any special closures here
(extend-type Object
  Schema
  ;; default behavior: no schema, all nodes valid
  (schema [layer id]
    edges-schema)
  (verify-node [layer id attrs]
    true)

  ;; TODO add a whats-wrong-with-this-node function
  (node-valid? [layer attrs] true)

  Enumerate
  (node-id-seq [layer]
    (node-id-subseq layer {}))
  (node-seq [layer]
    (node-subseq layer {}))

  SortedEnumerate
  (node-subseq [layer opts]
    (for [id (node-id-subseq layer {})]
      (map-entry id (get-node layer id nil))))
  ;; intentionally unimplemented - this blows up if you don't support it
  ;; (node-id-subseq [layer {}])

  Optimized
  ;; can't optimize anything
  (query-fn [layer keyseq not-found f] nil)

  Historical
  (node-history [layer id]
    (into (sorted-map)
          (for [r (get-revisions layer id)]
            [r (get-node (retro/at-revision layer r) id nil)])))

  Parent
  (children [this] nil)
  (child [this name] nil))

(extend-type nil
  Schema
  (schema [layer id] nil)
  (verify-node [layer id attrs] true))

(defn default-impl [protocol]
  (get-in protocol [:impls Object]))

(defn node-valid? [layer id attrs]
  (try (verify-node layer id attrs)
       true
       (catch AssertionError e nil)))

(defn dispatch-update
  "Takes the args to update-in-node (keyseq, f, args) and functions to handle top-level assoc,
  top-level dissoc, or anything else. Calls the appropriate function, passing the following
  arguments:

    (assoc-fn id value)
    (dissoc-fn id)
    (update-fn id keys)"
  [keyseq f args assoc-fn dissoc-fn update-fn]
  (if (empty? keyseq)
    (condp = f
      assoc (let [[id value] (assert-length 2 args)]
              (assoc-fn id value))
      dissoc (let [[id] (assert-length 1 args)]
               (dissoc-fn id))
      (throw (IllegalArgumentException.
              (format "Can't perform function %s at top level"
                      f))))
    (update-fn (first keyseq) (rest keyseq))))
