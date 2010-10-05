(ns jiraph.layer)

(def *rev* nil)

(defprotocol Layer "Jiraph layer protocol"
  (open          [layer]           "Open the layer file.")
  (close         [layer]           "Close the layer file.")
  (sync!         [layer]           "Flush all layer changes to the storage medium.")
  (truncate!     [layer]           "Removes all node data from the layer.")
  (node-count    [layer]           "Return the total number of nodes in this layer.")
  (node-ids      [layer]           "Return a lazy sequence of all node ids in this layer.")
  (get-property  [layer key]       "Fetch a layer-wide property.")
  (set-property! [layer key val]   "Store a layer-wide property.")
  (txn           [layer f]         "Wrap the function f in a transaction.")
  (get-node      [layer id]        "Fetch a node.")
  (get-meta      [layer id]        "Fetch the meta-node (contains incoming edges and revision data).")
  (node-exists?  [layer id]        "Check if a node exists in the layer.")
  (add-node!     [layer id attrs]  "Add a node, throwing an exception if it already exists.")
  (append-node!  [layer id attrs]  "Append attrs to a node or create it if it doesn't exist.")
  (update-node!  [layer id f args] "Update a node with (apply f node args).")
  (assoc-node!   [layer id attrs]  "Associate attrs with a node.")
  (compact-node! [layer id]        "Compact a node by collapsing appended revisions.")
  (delete-node!  [layer id]        "Remove a node from the layer (incoming links remain)."))

(defmacro transaction [layer & forms]
  `(txn ~layer (fn [] (do ~@forms))))