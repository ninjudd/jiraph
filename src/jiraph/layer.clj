(ns jiraph.layer
  (use [retro.core :only [*revision*]]))

(defn make-node [attrs]
  (let [attrs (dissoc attrs :id)]
    (if *revision*
      (assoc attrs :rev *revision*)
      (dissoc attrs :rev))))

(defprotocol Layer
  "Jiraph layer protocol"
  (open             [layer]            "Open the layer file.")
  (close            [layer]            "Close the layer file.")
  (sync!            [layer]            "Flush all layer changes to the storage medium.")
  (optimize!        [layer]            "Optimize underlying layer storage.")
  (truncate!        [layer]            "Removes all node data from the layer.")
  (node-count       [layer]            "Return the total number of nodes in this layer.")
  (node-ids         [layer]            "Return a lazy sequence of all node ids in this layer.")
  (fields           [layer]
                    [layer subfields]  "A list of canonical fields stored in this layer. Can be empty.")
  (verify-node      [layer attrs]      "Verify that the given node is valid according to the layer schema.")
  (get-property     [layer key]        "Fetch a layer-wide property.")
  (set-property!    [layer key val]    "Store a layer-wide property.")
  (get-node         [layer id]         "Fetch a node.")
  (set-node!        [layer id attrs]   "Set a node.")
  (node-exists?     [layer id]         "Check if a node exists on this layer.")
  (add-node!        [layer id attrs]   "Add a node with the given id and attrs if it doesn't already exist.")
  (update-node!     [layer id f args]  "Update a node with (apply f node args), and return [old new].")
  (delete-node!     [layer id]         "Remove a node from the layer (incoming links remain).")
  (get-revisions    [layer id]         "Return all revision ids for a given node.")
  (get-incoming     [layer id]         "Return the ids of all nodes that have an incoming edge to this one.")
  (add-incoming!    [layer id from-id] "Add an incoming edge record on id for from-id.")
  (drop-incoming!   [layer id from-id] "Remove the incoming edge record on id for from-id."))

(defprotocol Append
  "Jiraph appending format"
  (append-node! [layer id attrs] "Append attrs to a node or create it if it doesn't exist."))

(defprotocol Assoc
  "Jiraph assoc protocol"
  (assoc-node! [layer id attrs] "Associate attrs with a node."))

(defn node-valid? [layer attrs]
  (try (verify-node layer attrs)
       true
       (catch AssertionError e)))