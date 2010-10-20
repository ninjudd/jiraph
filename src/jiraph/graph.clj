(ns jiraph.graph
  (:use [useful :only [into-map update remove-keys-by-val remove-vals]])
  (:require [jiraph.layer :as layer]))

(def *graph* nil)

(defmacro with-graph [graph & forms]
  `(binding [*graph* ~graph]
     (try (dorun (map layer/open (vals *graph*)))
          ~@forms
          (finally (dorun (map layer/close (vals *graph*)))))))

(defmacro at-revision [rev & forms]
  `(binding [layer/*rev* ~rev]
     ~@forms))

(defmacro with-transaction [layer & forms]
  `(try
     (let [result# (layer/with-transaction (*graph* ~layer) ~@forms)]
       (sync! ~layer)
       result#)
     (catch javax.transaction.TransactionRolledbackException e#)))

(defn abort-transaction []
  (throw (javax.transaction.TransactionRolledbackException.)))

(defmacro with-each-layer [layers & forms]
  `(doseq [~'layer (if (empty? ~layers) (vals *graph*) (map *graph* ~layers))]
     ~@forms))

(defn sync! [& layers]
  (with-each-layer layers
    (layer/sync! layer)))

(defn truncate! [& layers]
  (with-each-layer layers
    (layer/truncate! layer)))

(defn node-ids   [layer] (layer/node-ids   (*graph* layer)))
(defn node-count [layer] (layer/node-count (*graph* layer)))

(defn get-property  [layer key]     (layer/get-property  (*graph* layer) key))
(defn set-property! [layer key val] (layer/set-property! (*graph* layer) key val))

(defn update-property! [layer key f & args]
  (with-transaction layer
    (let [val (get-property layer key)]
      (set-property! layer key (apply f val args)))))

(defn current-revision [& layers]
  (apply max (for [layer (if (empty? layers) (keys *graph*) layers)]
               (or (get-property layer :rev) 0))))

(defn get-node     [layer id] (layer/get-node     (*graph* layer) id))
(defn node-exists? [layer id] (layer/node-exists? (*graph* layer) id))

(defn add-node! [layer id & attrs]
  (let [layer (*graph* layer)
        node  (layer/add-node! layer id (into-map attrs))]
    (doseq [[to-id edge] (:edges node)]
      (if-not (:deleted edge)
        (layer/add-incoming! layer to-id id)))
    node))

(defn append-node! [layer id & attrs]
  (let [layer (*graph* layer)
        node  (layer/append-node! layer id (into-map attrs))]
    (doseq [[to-id edge] (:edges node)]
      (if (:deleted edge)
        (layer/drop-incoming! layer to-id id)
        (layer/add-incoming!  layer to-id id)))
    node))

(defn update-node! [layer id f & args]
  (let [layer     (*graph* layer)
        [old new] (layer/update-node! layer id f args)]
    (let [new-edges (set (remove-keys-by-val :deleted (:edges new)))
          old-edges (set (remove-keys-by-val :deleted (:edges old)))]
      (doseq [to-id (remove old-edges new-edges)]
        (layer/add-incoming! layer to-id id))
      (doseq [to-id (remove new-edges old-edges)]
        (layer/drop-incoming! layer to-id id)))
    [old new]))

(defn delete-node! [layer id]
  (let [layer (*graph* layer)
        node  (layer/delete-node! layer id)]
    (doseq [[to-id edge] (:edges node)]
      (if-not (:deleted edge)
        (layer/drop-incoming! layer to-id id)))))

(defn assoc-node!
  "Associate attrs with a node."
  [layer id & attrs]
  (apply update-node! layer id merge attrs))

(defn compact-node!
  "Compact a node by removing deleted edges. This will also collapse appended revisions."
  [layer id]
  (update-node! layer id update :edges (partial remove-vals :deleted)))

(defn layers []
  (keys *graph*))

(defn get-all-revisions [layer id]
  (filter pos? (layer/get-revisions (*graph* layer) id)))

(defn get-revisions [layer id]
  (reverse
   (take-while pos? (reverse (layer/get-revisions (*graph* layer) id)))))

(defn get-incoming [layer id] (layer/get-incoming (*graph* layer) id))

(defn field-to-layer [graph layers]
  (reduce (fn [m layer]
            (reduce #(assoc %1 %2 layer)
                    m (layer/fields (graph layer))))
          {} (reverse layers)))