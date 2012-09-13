(ns jiraph.layer
  (:use [useful.utils :only [adjoin map-entry]]
        [clojure.stacktrace :only [print-trace-element]])
  (:require [retro.core :as retro])
  (:import (java.util Map$Entry)))

(def ^:dynamic *warn-on-fallback* false)

(defprotocol Enumerate
  (node-id-seq [layer]
    "A seq of all node ids in this layer")
  (node-seq [layer]
    "A seq of all [id, node] entries in this layer"))

(defprotocol SortedEnumerate
  "For layers which can provide indexed access into their Enumerate functions.
   Operations are versions fo Enumerate functions that take additional arguments
   in the form accepted by clojure.core/subseq."
  (node-id-subseq [layer cmp start])
  (node-subseq [layer cmp start]))

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
     removed."))

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

    ;; we can simulate this for you, pretty inefficiently
    (truncate! [layer]
      (retro/unsafe-txn [layer]
        (apply retro/compose
               (for [id (node-id-seq layer)]
                 (update-in-node layer [] dissoc id)))))))

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
    (node-id-subseq layer >= ""))
  (node-seq [layer]
    (node-subseq layer >= ""))

  SortedEnumerate
  (node-subseq [layer cmp start]
    (for [id (node-id-subseq layer cmp start)]
      (map-entry id (get-node layer id nil))))
  ;; intentionally unimplemented - this blows up if you don't support it
  ;; (node-id-subseq [layer cmp start])

  Optimized
  ;; can't optimize anything
  (query-fn  [layer keyseq not-found f] nil)

  Incoming
  (get-incoming [layer id]
    #{})

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

(defn advance-reader
  "Given a vector of actualized jiraph IOValues and a read function, return a new read function by
  calling the :wrap-read function of each action."
  [read actions]
  (reduce (fn [read wrapper]
            (wrapper read))
          read, (map :wrap-read actions)))

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
        (-> (read layer' read-path)
            (apply update-in* update-path f args)
            (get-in get-path))
        (read layer' read-keyseq)))))
