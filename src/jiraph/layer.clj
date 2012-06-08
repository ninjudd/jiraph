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

(defprotocol Incoming
  (get-incoming [layer id]
    "Return the ids of all nodes that have an incoming edge to this one.
     The return type may be either a set, or an \"existence hash\", where the value for each key
     is a boolean, indicating whether an incoming edge comes from the key to this node. This can
     be useful for allowing a client to detect that an edge once existed but has since been
     removed.")
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
  the function passed in and testing it against some internal list of optimizable functions, or by
  checking the keyseq to see if optimized access is possible at this node.

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
    "Removes all node data from the layer."))

(defprotocol ChangeLog
  (get-revisions [layer id]
    "The revisions that changed the given node.")
  (get-changed-ids [layer rev]
    "The nodes changed by the given revision."))

(defprotocol Historical
  (node-history [layer id]
    "Return a map from revision number to node data, for each revision that
     affected this node. The map must be sorted, with earliest revisions first."))

(defprotocol Preferences
  "Indicate to Jiraph what things you want it to do for you. These preferences should not
  change while the system is running; Jiraph may choose to cache any of them."
  (manage-incoming? [layer]
    "Should Jiraph decide when to add/drop incoming edges?")
  (manage-changelog? [layer]
    "Should Jiraph store changelog information for you?")
  (single-edge? [layer]
    "Is it illegal to have more than one outgoing edge per node on this layer?"))

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
        (dissoc-node! layer id)))))

(def ^{:doc "A default schema describing a map which contains edges and possibly other keys."}
  edges-schema {:schema {:type :map
                         :fields {:edges {:type :map
                                          :fields {:* :any}}
                                  :* :any}}})

;; Don't need any special closures here
(extend-type Object
  Preferences
  ;; opt in to managed-incoming, and let the layer set a :single-edge key to
  ;; indicate it should be in single-edge mode
  (manage-incoming?  [layer] true)
  (manage-changelog? [layer] true)
  (single-edge? [layer]
    (:single-edge layer))

  Schema
  ;; default behavior: no schema, all nodes valid
  (schema [layer id]
    edges-schema)
  (verify-node [layer id attrs]
    true)

  ;; TODO add a whats-wrong-with-this-node function
  (node-valid? [layer attrs] true)

  Enumerate
  ;; Fallback behavior is the empty list. Jiraph does *not* automatically
  ;; track assoc'd and dissoc'd nodes for you in order to provide node-ids,
  ;; because that would be a huge performance hit.
  (node-id-seq [layer] ())
  (node-seq [layer]
    (map #(get-node layer % nil) (node-id-seq layer)))

  Optimized
  ;; can't optimize anything
  (query-fn  [layer keyseq not-found f] nil)
  (update-fn [layer keyseq f] nil)

  Historical
  (node-history [layer id]
    (into (sorted-map)
          (for [r (get-revisions layer id)]
            [r (get-node (retro/at-revision layer r) id nil)]))))

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
