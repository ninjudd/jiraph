(ns jiraph.graph
  (:use [useful :only [into-map update remove-keys-by-val remove-vals any]])
  (:require [jiraph.layer :as layer]
            [jiraph.tokyo-database :as tokyo]
            [jiraph.byte-append-layer :as byte-append-layer]))

(def ^{:dynamic true} *graph* nil)
(def ^{:dynamic true} *transactions* #{})

(defn edge-ids [node & [pred]]
  (remove-keys-by-val
   (if pred
     (any :deleted (complement pred))
     :deleted)
   (:edges node)))

(defn edges [node & [pred]]
  (select-keys (:edges node) (edge-ids node pred)))

(defn open! []
  (dorun (map layer/open (vals *graph*))))

(defn close! []
  (dorun (map layer/close (vals *graph*))))

(defmacro with-graph [graph & forms]
  `(binding [*graph* ~graph]
     (try (open!)
          ~@forms
          (finally (close!)))))

(defmacro with-graph! [graph & forms]
  `(let [graph# *graph*]
     (alter-var-root #'*graph* (fn [_#] ~graph))
     (try (open!)
          ~@forms
          (finally (close!)))
     (alter-var-root #'*graph* (fn [_#] graph#))))

(defmacro at-revision
  "Execute the given forms with the graph at revision rev. Can be used in to mark changes with a given
   revision, or rewind the state of the graph to a given revision. See the README for more information."
  [rev & forms]
  `(binding [layer/*rev* ~rev]
     ~@forms))

(defmacro with-each-layer
  "Execute forms with layer bound to each layer specified or all layers if an layers is empty."
  [layers & forms]
  `(doseq [~'layer (if (empty? ~layers) (vals *graph*) (map *graph* ~layers))]
     ~@forms))

(defn sync!
  "Flush changes for the specified layers to the storage medium, or all layers if none are specified."
  [& layers]
  (with-each-layer layers
    (layer/sync! layer)))

(defn truncate!
  "Remove all nodes from the specified layers, or all layers if none are specified."
  [& layers]
  (with-each-layer layers
    (layer/truncate! layer)))

(defn node-ids
  "Return a lazy sequence of all node ids in this layer."
  [layer]
  (layer/node-ids   (*graph* layer)))

(defn node-count
  "Return the total number of nodes in this layer."
  [layer]
  (layer/node-count (*graph* layer)))

(defn get-property
  "Fetch a layer-wide property."
  [layer key]
  (layer/get-property (*graph* layer) key))

(defn set-property!
  "Store a layer-wide property."
  [layer key val]
  (layer/set-property! (*graph* layer) key val))

(defn- skip-past-revisions
  "Takes a layer name and a function and wraps it in a new function that skips it if the current
   revision has already been applied on this layer, also setting the revision property on the layer
   upon executing the function."
  [layer f]
  (fn []
    (if-not layer/*rev*
      (f)
      (let [rev (or (get-property layer :rev) 0)]
        (if (<= layer/*rev* rev)
          (printf "skipping revision: revision [%s] <= current revision [%s]\n" layer/*rev* rev)
          (let [result (f)]
            (if layer/*rev*
              (set-property! layer :rev layer/*rev*))
            result))))))

(defn- ignore-nested-transactions
  "Takes a layer name and two functions, one that's wrapped in a transaction and one that's not,
   returning a new fn that calls the transactional fn if not currently in a transaction or otherwise
   calls the plain fn."
  [layer f f-txn]
  (fn []
    (if (contains? *transactions* layer)
      (f)
      (binding [*transactions* (conj *transactions* layer)]
        (f-txn)))))

(defn- catch-rollbacks
  "Takes a function and wraps it in a new function that catches the exception thrown by abort-transaction."
  [f]
  (fn []
    (try (f)
         (catch javax.transaction.TransactionRolledbackException e))))

(defn abort-transaction
  "Throws an exception that will be caught by catch-rollbacks to abort the transaction."
  []
  (throw (javax.transaction.TransactionRolledbackException.)))

(defn wrap-transaction
  "Takes a function and returns a new function wrapped in a transaction on the named layer."
  [layer f]
  (->> (layer/wrap-transaction (*graph* layer) f)
       (catch-rollbacks)
       (skip-past-revisions layer)
       (ignore-nested-transactions layer f)))

(defmacro with-transaction
  "Execute forms withing a transaction on the named layer."
  [layer & forms]
  `((wrap-transaction ~layer (fn [] ~@forms))))

(defn update-property!
  "Update the given layer property by calling function f with the old value and any supplied args."
  [layer key f & args]
  (let [val (get-property layer key)]
    (set-property! layer key (apply f val args))))

(defn current-revision
  "The maximum revision on all specified layers, or all layers if none are specified."
  [& layers]
  (apply max 0 (for [layer (if (empty? layers) (keys *graph*) layers)]
                 (or (get-property layer :rev) 0))))

(defn get-node
  "Fetch a node's data from this layer."
  [layer id]
  (layer/get-node (*graph* layer) id))

(defn node-exists?
  "Check if a node exists on this layer."
  [layer id]
  (layer/node-exists? (*graph* layer) id))

(defn append-only?
  "Is the named layer marked append-only?"
  [layer]
  (let [append-only (:append-only (meta *graph*))]
    (or (true? append-only)
        (contains? append-only layer))))

(defn add-node!
  "Add a node with the given id and attrs if it doesn't already exist."
  [layer id & attrs]
  {:pre [(if (append-only? layer) layer/*rev* true)]}
  (with-transaction layer
    (let [layer (*graph* layer)
          node  (layer/add-node! layer id (into-map attrs))]
      (doseq [[to-id edge] (:edges node)]
        (when-not (:deleted edge)
          (layer/add-incoming! layer to-id id)))
      node)))

(defn append-node!
  "Append attrs to a node or create it if it doesn't exist. Note: some layers may not implement this."
  [layer id & attrs]
  {:pre [(if (append-only? layer) layer/*rev* true)]}
  (with-transaction layer
    (let [layer (*graph* layer)
          node  (layer/append-node! layer id (into-map attrs))]
      (doseq [[to-id edge] (:edges node)]
        (if (:deleted edge)
          (layer/drop-incoming! layer to-id id)
          (layer/add-incoming!  layer to-id id)))
      node)))

(def ^{:dynamic true :private true} *compacting* false)

(defn update-node!
  "Update a node by calling function f with the old value and any supplied args."
  [layer id f & args]
  {:pre [(or (not (append-only? layer)) *compacting*)]}
  (with-transaction layer
    (let [layer     (*graph* layer)
          [old new] (layer/update-node! layer id f args)]
      (let [new-edges (set (edge-ids new))
            old-edges (set (edge-ids old))]
        (doseq [to-id (remove old-edges new-edges)]
          (layer/add-incoming! layer to-id id))
        (doseq [to-id (remove new-edges old-edges)]
          (layer/drop-incoming! layer to-id id)))
      [old new])))

(defn delete-node!
  "Remove a node from a layer (incoming links remain)."
  [layer id]
  (with-transaction layer
    (let [layer (*graph* layer)
          node  (layer/delete-node! layer id)]
      (doseq [[to-id edge] (:edges node)]
        (if-not (:deleted edge)
          (layer/drop-incoming! layer to-id id))))))

(defn assoc-node!
  "Associate attrs with a node."
  [layer id & attrs]
  (apply update-node! layer id merge attrs))

(defn compact-node!
  "Compact a node by removing deleted edges. This will also collapse appended revisions."
  [layer id]
  (binding [*compacting* true]
    (update-node! layer id update :edges (partial remove-vals :deleted))))

(defn layers
  "Return the names of all layers in the current graph."
  []
  (keys *graph*))

(defn get-all-revisions
  "Return a seq of all revisions that have ever modified this node on this layer, even if the data has been
   subsequently compacted."
  [layer id]
  (filter pos? (layer/get-revisions (*graph* layer) id)))

(defn get-revisions
  "Return a seq of all revisions with data for this node."
  [layer id]
  (reverse
   (take-while pos? (reverse (layer/get-revisions (*graph* layer) id)))))

(defn get-incoming
  "Return the ids of all nodes that have incoming edges on this layer to this node (excludes edges marked :deleted)."
  [layer id]
  (layer/get-incoming (*graph* layer) id))

(defn field-to-layer
  "Return a mapping from field to layer for all the layers provided. If a field appears in more
   than one layer, the first matching layer will be used. Fields are provided as keywords with
   internal dashes, but a field-transform function that can be provided to change this."
  [graph layers & [field-transform]]
  (let [field-transform (or field-transform identity)]
    (reduce (fn [m layer]
              (reduce #(assoc %1 (field-transform %2) layer)
                      m (layer/fields (graph layer))))
            {} (reverse layers))))

(defn layer [path]
  (byte-append-layer/make (tokyo/make {:path path :create true})))