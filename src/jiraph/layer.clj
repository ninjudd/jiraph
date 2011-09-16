(ns jiraph.layer
  (use [retro.core :only [*revision*]]))

(defprotocol Counted
  (node-count       [layer]            "Return the total number of nodes in this layer."))

(defprotocol Schema
  (fields
    [layer]
    [layer subfields]
    "A list of canonical fields stored in this layer. Can be empty.")
  (node-valid?  [layer attrs]      "Check if the given node is valid according to the layer schema."))

(defprotocol LayerMeta
  (get-meta         [layer key]        "Fetch a layer-wide property.")
  (assoc-meta!      [layer key val]    "Store a layer-wide property."))

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
  (get-in-node [layer id keyseq])
  (assoc-in-node! [layer id keyseq value])
  (dissoc-in-node! [layer id keyseq]))

(defprotocol Update
  "Layer that can do (some) updates of sub-nodes more efficiently than fetching a whole node, changing it, and then writing it."
  (update-in-node!
    [layer keyseq f argseq]
    "Arguments are as to clojure.core/update-in, but without varargs."))
