(ns jiraph.layer)

(def *rev* nil)

(defprotocol Layer "Jiraph layer protocol"
  (open           [layer]            "Open the layer file.")
  (close          [layer]            "Close the layer file.")
  (sync!          [layer]            "Flush all layer changes to the storage medium.")
  (truncate!      [layer]            "Removes all node data from the layer.")
  (node-count     [layer]            "Return the total number of nodes in this layer.")
  (node-ids       [layer]            "Return a lazy sequence of all node ids in this layer.")
  (fields         [layer]            "A list of canonical fields stored in this layer. Can be empty.")
  (get-property   [layer key]        "Fetch a layer-wide property.")
  (set-property!  [layer key val]    "Store a layer-wide property.")
  (txn            [layer f]          "Wrap the function f in a transaction.")
  (get-node       [layer id]         "Fetch a node.")
  (node-exists?   [layer id]         "Check if a node exists in the layer.")
  (add-node!      [layer id attrs]   "Add a node, throwing an exception if it already exists.")
  (append-node!   [layer id attrs]   "Append attrs to a node or create it if it doesn't exist.")
  (update-node!   [layer id f args]  "Update a node with (apply f node args).")
  (delete-node!   [layer id]         "Remove a node from the layer (incoming links remain).")
  (get-revisions  [layer id]         "Return all revision ids for a given node.")
  (get-incoming   [layer id]         "Return the ids of all nodes that have an incoming edge to this one.")
  (add-incoming!  [layer id from-id] "Add an incoming edge record on id for from-id.")
  (drop-incoming! [layer id from-id] "Remove the incoming edge record on id for from-id."))

(defmacro with-transaction [layer & forms]
  `(txn ~layer
     (fn []
       (if *rev*
         (if (> *rev* (or (get-property ~layer :rev) 0)) ; skip revisions that have already been committed
           (let [result# (do ~@forms)]
             (set-property! ~layer :rev *rev*)
             result#)) 
         (do ~@forms)))))
